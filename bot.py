import asyncio
import html
import json
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
import asyncpg
import phonenumbers
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.types import InlineKeyboardButton as _AIButton
from aiogram.types import InlineKeyboardMarkup as _AIMarkup

from telethon import TelegramClient, errors, events
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
MACRODROID_WEBHOOK_URL = os.getenv(
    "MACRODROID_WEBHOOK_URL",
    "http://trigger.macrodroid.com/YOUR_UNIQUE_DEVICE_UUID/telegram_bot",
)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/dbname")
PORT = int(os.getenv("PORT", "10000"))

# fordet tunables (same names and defaults as fordet.py)
MAX_CONCURRENT_USERS = max(80, int(os.getenv("MAX_CONCURRENT_USERS", "80")))
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "1"))
TARGET_ENTITY_CACHE_SIZE = int(os.getenv("TARGET_ENTITY_CACHE_SIZE", "2000"))
OTP_BACKFILL_WINDOW_SECONDS = int(os.getenv("OTP_BACKFILL_WINDOW_SECONDS", "10"))
OTP_BACKFILL_MAX_MESSAGES = int(os.getenv("OTP_BACKFILL_MAX_MESSAGES", "10"))
OTP_SEARCH_DURATION = int(os.getenv("OTP_SEARCH_DURATION", "300"))

# number files (/numberfiles)
NUMBER_FILE_TTL_HOURS = int(os.getenv("NUMBER_FILE_TTL_HOURS", "48"))
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(5 * 1024 * 1024)))
FILES_PER_PAGE = 10
OTP_PICK_PER_PAGE = 10
PENDING_BATCH_TTL = 1800  # seconds a picked-but-unstarted batch stays valid

router = Router()
bot_instance: Optional[Bot] = None

# ------------------------------------------------------------
# DB / state
# ------------------------------------------------------------
db_pool: Optional[asyncpg.Pool] = None

target_chat_id: Optional[int] = None
user_target_chat_ids: dict[int, int] = {}

is_processing = False
is_paused = False
current_task: Optional[asyncio.Task] = None
active_file_id: Optional[int] = None
_background_tasks: Set[asyncio.Task] = set()
pending_batches: Dict[int, Dict] = {}  # batch waiting for the user to pick an OTP group

current_index = 0
total_numbers_count = 0
active_number: Optional[str] = None

device_event = asyncio.Event()
device_status: Optional[str] = None

skip_current_number = False
pause_event = asyncio.Event()
pause_event.set()

# SUCCESS / FAILED waiters, keyed by (user_id, number); resolved by the live chat handler
live_success_failure_events: dict[tuple, asyncio.Event] = {}
live_success_failure_results: dict[tuple, str] = {}

# fordet client / live-handler state
user_clients: Dict[int, "ClientWrapper"] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
otp_sets_cache: Dict[int, List[Dict]] = {}
otp_live_handler_registered: Dict[int, List[Callable]] = {}
result_live_handler_registered: Dict[int, List[Callable]] = {}
result_live_handler_chat: Dict[int, int] = {}
target_entity_cache: Dict[int, OrderedDict] = {}

OTP_PATTERN = re.compile(r'(\d{3})-?(\d{3})')
FORMAT_PATTERN = re.compile(r'\d{1,2}[|-]\d{1,2}')


# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
async def init_db():
    global db_pool
    dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL

    try:
        db_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=20)
        logger.info("Connected to PostgreSQL")

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS number_logs (
                    id SERIAL PRIMARY KEY,
                    phone_number VARCHAR(32) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    user_id BIGINT PRIMARY KEY,
                    chat_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    phone VARCHAR(255),
                    name TEXT,
                    session_data TEXT,
                    is_logged_in BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS otp_groups (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    set_index INTEGER NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    formats JSONB NOT NULL,
                    group_ids JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                    UNIQUE(user_id, set_index)
                );

                CREATE TABLE IF NOT EXISTS number_files (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    numbers JSONB NOT NULL,
                    total INTEGER NOT NULL,
                    next_index INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_number_files_user ON number_files (user_id, created_at);
                """
            )

            # Prevent duplicate set names per user (add_otp_set relies on this)
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_otp_groups_user_name
                ON otp_groups (user_id, name)
                """
            )
    except Exception:
        logger.exception("Failed to initialize PostgreSQL")


async def log_number_status(phone_number: str, status: str, details: str = None):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO number_logs (phone_number, status, details) VALUES ($1, $2, $3)",
                phone_number,
                status,
                details,
            )
    except Exception:
        logger.exception("Failed to write log for %s", phone_number)


def parse_phone_number(raw_number: str):
    cleaned = raw_number.strip()
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned

    try:
        parsed = phonenumbers.parse(cleaned, None)
        if phonenumbers.is_valid_number(parsed):
            return str(parsed.country_code), str(parsed.national_number)
    except Exception:
        pass

    if cleaned.startswith("+234") or cleaned.startswith("+233"):
        return cleaned[1:4], cleaned[4:]
    elif cleaned.startswith("+44"):
        return cleaned[1:3], cleaned[3:]
    elif len(cleaned) > 3:
        return cleaned[1:4], cleaned[4:]
    return "", cleaned[1:]


async def call_device_api(raw_number: str, payload_type: str = "check", code: str | None = None):
    country_code, national_number = parse_phone_number(raw_number)

    url = (
        f"{MACRODROID_WEBHOOK_URL}"
        f"?country_code={country_code}&national_number={national_number}&type={payload_type}"
    )
    if code:
        url += f"&code={code}"

    logger.info("Triggering MacroDroid -> +%s %s [%s]", country_code, national_number, payload_type)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                logger.info("Webhook response status: %s", response.status)
                return True
    except Exception:
        logger.exception("Failed to reach MacroDroid webhook")
        return False


# ------------------------------------------------------------
# fordet helpers (copied verbatim from fordet.py)
# ------------------------------------------------------------
def parse_chat_id(id_val) -> Tuple[int, Optional[int]]:
    """
    Parse a stored ID value into (chat_id, topic_id).
    Accepts int, or strings like "-100123456789" or "-100123456789:36615".
    Returns (chat_id, None) when no topic is specified.
    """
    s = str(id_val)
    if ':' in s:
        parts = s.split(':', 1)
        return int(parts[0]), int(parts[1])
    return int(s), None


def _is_valid_id_token(token: str) -> bool:
    """Return True if *token* is a valid plain ID or 'chat_id:topic_id' token."""
    token = token.strip()
    if ':' in token:
        parts = token.split(':', 1)
        return (parts[0].strip().lstrip('-').isdigit() and
                parts[1].strip().isdigit())
    return token.lstrip('-').isdigit()


def _parse_id_token(token: str) -> Optional[str]:
    """
    Convert a user-typed ID token to its canonical string form.
    Examples: "-100123" → "-100123",  "-100123:36615" → "-100123:36615".
    Returns None for invalid tokens.
    """
    token = token.strip()
    if ':' in token:
        parts = token.split(':', 1)
        chat_part = parts[0].strip()
        topic_part = parts[1].strip()
        if chat_part.lstrip('-').isdigit() and topic_part.isdigit():
            return f"{int(chat_part)}:{int(topic_part)}"
        return None
    elif token.lstrip('-').isdigit():
        return token
    return None


def _get_message_topic_id(message) -> Optional[int]:
    """
    Extract the forum-topic ID from a Telethon message object.
    Returns None for regular (non-topic) messages.
    """
    reply_to = getattr(message, 'reply_to', None)
    if reply_to is None:
        return None
    return (getattr(reply_to, 'reply_to_top_id', None) or
            getattr(reply_to, 'reply_to_msg_id', None))


def extract_strict_numbers_only(text: str) -> str:
    if not text:
        return ""
    return ''.join(c for c in text if c.isdigit())


def extract_numbers_only(text: str) -> str:
    return ''.join(c for c in text if c.isdigit())


def extract_message_text_only(message) -> str:
    if not message:
        return ""
    text = (
        getattr(message, "raw_text", None)
        or getattr(message, "message", None)
        or getattr(message, "text", None)
        or ""
    )
    return text if isinstance(text, str) else ""


def extract_full_message(message) -> str:
    if not message:
        return ""

    text = (
        getattr(message, "raw_text", None)
        or getattr(message, "message", None)
        or getattr(message, "text", None)
        or ""
    )
    if not isinstance(text, str):
        text = ""

    button_texts: List[str] = []
    callback_payloads: List[str] = []

    def _decode_data(data) -> Optional[str]:
        """Safely decode bytes or str callback data."""
        if not data:
            return None
        try:
            return data.decode("utf-8") if isinstance(data, bytes) else str(data)
        except Exception:
            return None

    if getattr(message, "buttons", None):
        for row in message.buttons:
            if not row:
                continue
            for btn in row:
                label = getattr(btn, "text", None)
                if label:
                    button_texts.append(label)
                raw_btn = getattr(btn, "button", None)
                cb_data = (
                    getattr(raw_btn, "data", None)
                    if raw_btn is not None else None
                ) or getattr(btn, "data", None)
                payload = _decode_data(cb_data)
                if payload:
                    callback_payloads.append(payload)
                if raw_btn is not None:
                    copy_text = getattr(raw_btn, "copy_text", None)
                    if copy_text and isinstance(copy_text, str):
                        callback_payloads.append(copy_text)
    elif getattr(message, "reply_markup", None):
        markup = message.reply_markup
        if hasattr(markup, "rows") and markup.rows:
            for row in markup.rows:
                for btn in getattr(row, "buttons", []):
                    label = getattr(btn, "text", None)
                    if label:
                        button_texts.append(label)
                    payload = _decode_data(getattr(btn, "data", None))
                    if payload:
                        callback_payloads.append(payload)
                    copy_text = getattr(btn, "copy_text", None)
                    if copy_text and isinstance(copy_text, str):
                        callback_payloads.append(copy_text)
        elif hasattr(markup, "inline_keyboard") and markup.inline_keyboard:
            for row in markup.inline_keyboard:
                for btn in row:
                    label = getattr(btn, "text", None)
                    if label:
                        button_texts.append(label)
                    payload = _decode_data(getattr(btn, "callback_data", None))
                    if payload:
                        callback_payloads.append(payload)
        elif hasattr(markup, "keyboard") and markup.keyboard:
            for row in markup.keyboard:
                for btn in row:
                    label = getattr(btn, "text", None)
                    if label:
                        button_texts.append(label)

    parts: List[str] = []
    if button_texts:
        parts.append(" - ".join(button_texts))
    if callback_payloads:
        parts.append(" - ".join(callback_payloads))
    if text:
        parts.append(text)

    return " | ".join(parts)


class OTPFormatParser:
    
    @staticmethod
    def extract_number(text: str) -> str:
        return extract_numbers_only(text)
    
    @staticmethod
    def parse_format(format_str: str) -> Dict:
        parts = format_str.split('|')
        if len(parts) != 3:
            raise ValueError(f"Invalid format: {format_str}")
        
        prefix_rule, insert_text, suffix_rule = parts
        
        if prefix_rule.startswith('[') and prefix_rule.endswith(']'):
            prefix_dict = {'type': 'letters', 'letters': prefix_rule[1:-1]}
        else:
            prefix_parts = prefix_rule.split('-')
            if len(prefix_parts) == 1:
                prefix_keep = int(prefix_parts[0]) if prefix_parts[0] else 0
                prefix_dict = {
                    'type': 'count',
                    'count': prefix_keep
                }
            else:
                prefix_dict = {
                    'type': 'range',
                    'start': int(prefix_parts[0]) - 1,
                    'end': int(prefix_parts[1])
                }
        
        if suffix_rule.startswith('[') and suffix_rule.endswith(']'):
            suffix_dict = {'type': 'letters', 'letters': suffix_rule[1:-1]}
        else:
            suffix_parts = suffix_rule.split('-')
            if len(suffix_parts) == 1:
                suffix_keep = int(suffix_parts[0]) if suffix_parts[0] else 0
                suffix_dict = {
                    'type': 'count',
                    'count': suffix_keep,
                    'remove': 0
                }
            else:
                suffix_dict = {
                    'type': 'range',
                    'count': int(suffix_parts[0]),
                    'remove': int(suffix_parts[1]) if len(suffix_parts) > 1 else 0
                }
        
        return {
            'prefix_rule': prefix_dict,
            'insert_text': insert_text,
            'suffix_rule': suffix_dict
        }
    
    @staticmethod
    def apply_format(number: str, format_dict: Dict) -> str:
        if not number:
            return ""
            
        def extract_by_letters(letters: str) -> str:
            res = []
            for char in letters:
                idx = ord(char.upper()) - ord('A')
                if 0 <= idx < len(number):
                    res.append(number[idx])
            return "".join(res)
            
        prefix = format_dict['prefix_rule']
        insert = format_dict['insert_text']
        suffix = format_dict['suffix_rule']
        
        import re
        def replace_letters(match):
            return extract_by_letters(match.group(1))
            
        insert = re.sub(r'\[([A-Za-z]+)\]', replace_letters, insert)
        
        if prefix['type'] == 'letters':
            prefix_part = extract_by_letters(prefix['letters'])
        elif prefix['type'] == 'count':
            prefix_part = number[:prefix['count']] if prefix.get('count') and prefix['count'] > 0 else ''
        else:
            prefix_part = number[prefix.get('start', 0):prefix.get('end', 0)]
        
        if suffix['type'] == 'letters':
            suffix_part = extract_by_letters(suffix['letters'])
        elif suffix['type'] == 'count':
            suffix_part = number[-suffix['count']:] if suffix.get('count') and suffix['count'] > 0 else ''
        else:
            last_part = number[-suffix['count']:] if suffix.get('count') and suffix['count'] > 0 else ''
            if suffix.get('remove', 0) > 0 and len(last_part) > suffix['remove']:
                suffix_part = last_part[:-suffix['remove']]
            else:
                suffix_part = last_part
        
        return f"{prefix_part}{insert}{suffix_part}"
    
    @staticmethod
    def format_display_examples() -> str:
        examples = [
            ("`2|TGT|3`", "First 2 digits + TGT + Last 3 digits"),
            ("`[CDE]|xxx|4`", "Digits at C,D,E (3rd,4th,5th) + xxx + Last 4 digits"),
            ("`3|TNE[DE]ER|2`", "First 3 digits + TNE + Digits at D,E + ER + Last 2 digits"),
            ("`3|XXXX|[JK]`", "First 3 digits + XXXX + Digits at J,K"),
            ("`3|P|5`", "First 3 digits + P + Last 5 digits")
        ]
        
        result = "**Format Examples:**\n"
        for fmt, desc in examples:
            result += f"• {fmt} → {desc}\n"
        return result
    
    @staticmethod
    def extract_otp(text: str) -> Optional[str]:
        if not text:
            return None
        
        dash_pattern = r'(\d{3})-(\d{3})'
        for dash_match in re.finditer(dash_pattern, text):
            full_match = dash_match.group(0)
            before = text[:dash_match.start()]
            after = text[dash_match.end():]
            
            if (before and before[-1].isdigit()) or (after and after[0].isdigit()):
                logger.debug(f"Found 3-3 pattern but it's part of longer number: {full_match}")
            else:
                logger.debug(f"Found valid 3-3 OTP: {full_match}")
                return dash_match.group(1) + dash_match.group(2)
        
        digits_pattern = r'(\d{6})'
        for digits_match in re.finditer(digits_pattern, text):
            full_match = digits_match.group(1)
            before = text[:digits_match.start()]
            after = text[digits_match.end():]
            
            if (before and before[-1].isdigit()) or (after and after[0].isdigit()):
                logger.debug(f"Found 6 digits but they're part of longer number: {full_match}")
            else:
                logger.debug(f"Found valid 6-digit OTP: {full_match}")
                return full_match
        
        all_digits = re.sub(r'\D', '', text)
        if len(all_digits) == 6:
            if re.search(r'\d{6}', text):
                logger.debug(f"Found 6 digits after removing non-digits: {all_digits}")
                return all_digits
            else:
                logger.debug(f"Found 6 digits but they're not contiguous: {all_digits}")
        
        return None
    
    @staticmethod
    def message_contains_format(message_text: str, format_str: str) -> bool:
        if not message_text:
            return False
            
        parts = format_str.split('|')
        if len(parts) == 3:
            pattern = parts[1] 
            if pattern:
                return pattern in message_text
            else:
                return True
        
        return False


def _build_search_patterns(pure_number: str, otp_sets: List[Dict]) -> List[Dict]:
    all_search_patterns = []
    for otp_set in otp_sets:
        set_patterns = []
        for format_str in otp_set['formats']:
            try:
                parsed = OTPFormatParser.parse_format(format_str)
                pattern = OTPFormatParser.apply_format(pure_number, parsed)
                if pattern:
                    set_patterns.append({
                        'format': format_str,
                        'pattern': pattern,
                        'set_name': otp_set['name'],
                        'set_index': otp_set['set_index'],
                        'group_ids': otp_set['group_ids']
                    })
            except Exception as e:
                logger.debug(f"Error creating pattern for format {format_str}: {e}")
        if set_patterns:
            all_search_patterns.append({
                'set_index': otp_set['set_index'],
                'set_name': otp_set['name'],
                'patterns': set_patterns
            })
    return all_search_patterns


class ClientWrapper:
    """
    Transparent proxy around TelegramClient.
    All attribute/method access is forwarded to the inner client via __getattr__.
    When a new client is needed after a storm, call wrapper.update(new_client) —
    every reference to the wrapper (NW runner, OTP workers, forward handler etc.)
    automatically uses the new inner client with zero restarts.
    """
    def __init__(self, client: TelegramClient):
        object.__setattr__(self, '_client', client)

    def update(self, new_client: TelegramClient):
        object.__setattr__(self, '_client', new_client)

    def get_inner(self) -> TelegramClient:
        return object.__getattribute__(self, '_client')

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, '_client'), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, '_client'), name, value)

    def __repr__(self):
        return f"ClientWrapper({object.__getattribute__(self, '_client')!r})"


class FloodWaitManager:
    
    def __init__(self):
        self.user_flood_wait_until = {}
        self.start_notifications_sent = set()
        self.end_notifications_pending = set()
        self.lock = threading.Lock()
    
    def set_flood_wait(self, user_id: int, wait_seconds: int):
        with self.lock:
            wait_until = time.time() + wait_seconds + 2
            self.user_flood_wait_until[user_id] = wait_until
            
            should_notify_start = False
            
            if wait_seconds > 30:
                flood_wait_key = f"{user_id}_start_{wait_seconds}"
                if flood_wait_key not in self.start_notifications_sent:
                    self.start_notifications_sent.add(flood_wait_key)
                    self.end_notifications_pending.add(user_id)
                    should_notify_start = True
            
            return should_notify_start, wait_seconds
    
    def is_in_flood_wait(self, user_id: int):
        with self.lock:
            if user_id not in self.user_flood_wait_until:
                should_notify_end = user_id in self.end_notifications_pending
                if should_notify_end:
                    self.end_notifications_pending.discard(user_id)
                    self._cleanup_old_notifications(user_id)
                return False, 0, should_notify_end
            
            wait_until = self.user_flood_wait_until[user_id]
            current_time = time.time()
            
            if current_time >= wait_until:
                del self.user_flood_wait_until[user_id]
                should_notify_end = user_id in self.end_notifications_pending
                if should_notify_end:
                    self.end_notifications_pending.discard(user_id)
                    self._cleanup_old_notifications(user_id)
                return False, 0, should_notify_end
            
            return True, wait_until - current_time, False
    
    def _cleanup_old_notifications(self, user_id: int):
        current_time = time.time()
        keys_to_remove = []
        
        for key in self.start_notifications_sent:
            if key.startswith(f"{user_id}_"):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            self.start_notifications_sent.discard(key)
    
    def clear_flood_wait(self, user_id: int):
        with self.lock:
            self.user_flood_wait_until.pop(user_id, None)
            self._cleanup_old_notifications(user_id)
            self.end_notifications_pending.discard(user_id)

    def restore_flood_waits(self, waits: dict):
        """Repopulate flood wait timestamps from DB on startup.
        Only entries still in the future are kept."""
        with self.lock:
            current_time = time.time()
            for uid, wait_until in waits.items():
                if wait_until > current_time:
                    self.user_flood_wait_until[int(uid)] = float(wait_until)


def _ensure_user_target_cache(user_id: int):
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = OrderedDict()


def _get_cached_target(user_id: int, target_id: int):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    if target_id in od:
        od.move_to_end(target_id)
        return od[target_id]
    return None


def _set_cached_target(user_id: int, target_id: int, entity: object):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    od[target_id] = entity
    od.move_to_end(target_id)
    while len(od) > TARGET_ENTITY_CACHE_SIZE:
        od.popitem(last=False)


async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id):
    cache_key = str(target_id)
    ent = _get_cached_target(user_id, cache_key)
    if ent:
        return ent

    chat_id_only, _ = parse_chat_id(target_id)
    try:
        entity = await client.get_input_entity(chat_id_only)
        _set_cached_target(user_id, cache_key, entity)
        return entity
    except Exception as e:
        logger.debug(f"Failed to resolve entity {target_id} for user {user_id}: {e}")
        return None


async def _preload_user_dialogs(user_id: int, client: TelegramClient, user_name: str):
    try:
        logger.info(f"🔄 Pre-caching dialogs for user {user_id} ({user_name})...")
        count = 0
        async for _ in client.iter_dialogs():
            count += 1
        logger.info(f"✅ Pre-cached {count} dialogs for user {user_id}")
    except Exception as e:
        logger.warning(f"Failed to pre-cache dialogs for user {user_id}: {e}")


def _clear_otp_handlers(user_id: int, client):
    """Remove all live OTP group handlers for a user."""
    handlers = otp_live_handler_registered.pop(user_id, [])
    for handler in handlers:
        try:
            client.remove_event_handler(handler)
        except Exception:
            pass


async def notify_user_flood_wait(user_id: int, wait_seconds: int):
    try:
        bot = get_bot()
        
        wait_minutes = wait_seconds // 60
        if wait_seconds % 60 > 0:
            wait_minutes += 1
        
        resume_time = format_datetime_utc1(datetime.fromtimestamp(time.time() + wait_seconds, tz=timezone.utc))
        
        message = f"""⏰ **Flood Wait Alert**

Your account is temporarily limited by Telegram.

📋 **Details:**
• Wait time: {wait_minutes} minutes
• Resumes at: {resume_time}

⚠️ **Please note:**
• This is a Telegram restriction, not the bot
• Bot will automatically resume when the wait ends
• No action is needed on your part

🔄 **Status:** ⏳ Waiting for Telegram to lift restriction..."""
        
        await bot.send_message(user_id, message, parse_mode="Markdown")
        
    except Exception as e:
        logger.debug(f"Failed to send flood wait notification to user {user_id}: {e}")


async def notify_user_flood_wait_ended(user_id: int):
    try:
        bot = get_bot()
        
        timestamp = format_datetime_utc1()
        
        message = f"""✅ **Flood Wait Ended**

Your account restriction has been lifted!

🕐 **Time:** {timestamp}

📋 **Status:**
• Forwarding has resumed automatically
• All queued messages are being sent
• You can now send messages normally

🔄 **Status:** ✅ Active and forwarding..."""
        
        await bot.send_message(user_id, message, parse_mode="Markdown")
        
    except Exception as e:
        logger.debug(f"Failed to send flood wait ended notification to user {user_id}: {e}")


def format_datetime_utc1(dt: datetime = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    utc_plus_one = timezone(timedelta(hours=1))
    dt_utc1 = dt.astimezone(utc_plus_one)
    
    month = dt_utc1.strftime('%b')
    day = str(dt_utc1.day)
    year = dt_utc1.strftime('%Y')
    hour = dt_utc1.strftime('%I').lstrip('0') or '12'
    minute = dt_utc1.strftime('%M')
    second = dt_utc1.strftime('%S')
    am_pm = dt_utc1.strftime('%p')
    
    return f"{month} {day}, {year} {hour}:{minute}:{second} {am_pm}"


flood_wait_manager = FloodWaitManager()



# ------------------------------------------------------------
# DB layer (asyncpg port of fordet's users + otp_groups methods)
# ------------------------------------------------------------
async def db_get_user(user_id: int) -> Optional[Dict]:
    if not db_pool:
        return None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at
                FROM users WHERE user_id = $1
                """,
                user_id,
            )
        if not row:
            return None
        return {
            'user_id': row["user_id"],
            'phone': row["phone"],
            'name': row["name"],
            'session_data': row["session_data"],
            'is_logged_in': row["is_logged_in"],
            'created_at': row["created_at"].isoformat() if row["created_at"] else None,
            'updated_at': row["updated_at"].isoformat() if row["updated_at"] else None,
        }
    except Exception as e:
        logger.exception("Error in get_user for %s: %s", user_id, e)
        return None


async def db_save_user(user_id: int, phone: Optional[str] = None, name: Optional[str] = None,
                       session_data: Optional[str] = None, is_logged_in: bool = False):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    phone = EXCLUDED.phone,
                    name = EXCLUDED.name,
                    session_data = EXCLUDED.session_data,
                    is_logged_in = EXCLUDED.is_logged_in,
                    updated_at = CURRENT_TIMESTAMP
                """,
                user_id, phone, name, session_data, is_logged_in,
            )
    except Exception as e:
        logger.exception("Error in save_user for %s: %s", user_id, e)
        raise


async def db_purge_user_data(user_id: int) -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM otp_groups WHERE user_id = $1", user_id)
                await conn.execute("DELETE FROM number_files WHERE user_id = $1", user_id)
                await conn.execute(
                    """
                    UPDATE users
                    SET phone = NULL,
                        name = NULL,
                        session_data = NULL,
                        is_logged_in = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = $1
                    """,
                    user_id,
                )
        otp_sets_cache.pop(user_id, None)
        logger.info(f"Purged all data for user {user_id} on logout")
        return True
    except Exception as e:
        logger.exception("Error purging user data for %s: %s", user_id, e)
        return False


def _row_to_otp_set(row) -> Dict:
    return {
        'set_index': row['set_index'],
        'name': row['name'],
        'formats': json.loads(row['formats']) if row['formats'] else [],
        'group_ids': json.loads(row['group_ids']) if row['group_ids'] else [],
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
    }


async def db_get_otp_sets_fresh(user_id: int) -> List[Dict]:
    """Always reads from the DB and refreshes the cache."""
    if not db_pool:
        return [s.copy() for s in otp_sets_cache.get(user_id, [])]
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT set_index, name, formats::text AS formats, group_ids::text AS group_ids, created_at
                FROM otp_groups
                WHERE user_id = $1
                ORDER BY set_index ASC
                """,
                user_id,
            )
        result = [_row_to_otp_set(r) for r in rows]
        otp_sets_cache[user_id] = result
        return [s.copy() for s in result]
    except Exception as e:
        logger.exception(f"Error getting OTP sets for user {user_id}: {e}")
        return []


async def db_get_otp_sets(user_id: int) -> List[Dict]:
    if user_id in otp_sets_cache:
        return [s.copy() for s in otp_sets_cache[user_id]]
    return await db_get_otp_sets_fresh(user_id)


async def db_add_otp_set(user_id: int, name: str, formats: List[str], group_ids: List) -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # otp_groups has a FK to users; make sure the row exists even before /login
                await conn.execute(
                    "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id
                )
                next_index = await conn.fetchval(
                    "SELECT COALESCE(MAX(set_index), -1) + 1 FROM otp_groups WHERE user_id = $1", user_id
                )
                result_id = await conn.fetchval(
                    """
                    INSERT INTO otp_groups (user_id, set_index, name, formats, group_ids)
                    VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
                    ON CONFLICT (user_id, name) DO NOTHING
                    RETURNING id
                    """,
                    user_id, next_index or 0, name, json.dumps(formats), json.dumps(group_ids),
                )
        if result_id is not None:
            await db_get_otp_sets_fresh(user_id)
        return result_id is not None
    except Exception as e:
        logger.exception(f"Error adding OTP set for user {user_id}: {e}")
        return False


async def db_update_otp_set(user_id: int, set_index: int, name: Optional[str] = None,
                            formats: Optional[List[str]] = None,
                            group_ids: Optional[List] = None) -> bool:
    if not db_pool:
        return False
    updates, params = [], []

    def _add(col, val, cast=""):
        params.append(val)
        updates.append(f"{col} = ${len(params)}{cast}")

    if name is not None:
        _add("name", name)
    if formats is not None:
        _add("formats", json.dumps(formats), "::jsonb")
    if group_ids is not None:
        _add("group_ids", json.dumps(group_ids), "::jsonb")
    if not updates:
        return False
    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.extend([user_id, set_index])
    query = (f"UPDATE otp_groups SET {', '.join(updates)} "
             f"WHERE user_id = ${len(params) - 1} AND set_index = ${len(params)}")
    try:
        async with db_pool.acquire() as conn:
            status = await conn.execute(query, *params)
        updated = int(status.split()[-1]) > 0
        if updated:
            await db_get_otp_sets_fresh(user_id)
        return updated
    except Exception as e:
        logger.exception(f"Error updating OTP set for user {user_id}: {e}")
        return False


async def db_delete_all_otp_sets(user_id: int) -> int:
    if not db_pool:
        return 0
    try:
        async with db_pool.acquire() as conn:
            status = await conn.execute("DELETE FROM otp_groups WHERE user_id = $1", user_id)
        deleted_count = int(status.split()[-1])
        otp_sets_cache[user_id] = []
        logger.info(f"Deleted all ({deleted_count}) OTP sets for user {user_id}")
        return deleted_count
    except Exception as e:
        logger.exception(f"Error in delete_all_otp_sets for user {user_id}: {e}")
        return 0


async def db_delete_otp_set(user_id: int, set_index: int) -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                found = await conn.fetchval(
                    "SELECT id FROM otp_groups WHERE user_id = $1 AND set_index = $2", user_id, set_index
                )
                if found is None:
                    logger.warning(f"Set {set_index} not found for user {user_id}")
                    return False
                await conn.execute(
                    "DELETE FROM otp_groups WHERE user_id = $1 AND set_index = $2", user_id, set_index
                )
                remaining = await conn.fetch(
                    "SELECT id FROM otp_groups WHERE user_id = $1 ORDER BY set_index ASC", user_id
                )
                # two-step reindex so the (user_id, set_index) unique constraint never collides
                await conn.execute(
                    "UPDATE otp_groups SET set_index = -set_index - 1 WHERE user_id = $1", user_id
                )
                for new_index, row in enumerate(remaining):
                    await conn.execute(
                        "UPDATE otp_groups SET set_index = $1 WHERE id = $2", new_index, row['id']
                    )
        logger.info(f"Deleted set {set_index} for user {user_id}; reindexed {len(remaining)} remaining")
        await db_get_otp_sets_fresh(user_id)
        return True
    except Exception as e:
        logger.exception(f"Error in delete_otp_set for user {user_id}, set {set_index}: {e}")
        return False


class _DB:
    """Name-compatible facade so fordet's `await db_call(db.method, ...)` calls run unchanged."""
    get_user = staticmethod(db_get_user)
    save_user = staticmethod(db_save_user)
    purge_user_data = staticmethod(db_purge_user_data)
    get_otp_sets = staticmethod(db_get_otp_sets)
    get_otp_sets_fresh = staticmethod(db_get_otp_sets_fresh)
    add_otp_set = staticmethod(db_add_otp_set)
    update_otp_set = staticmethod(db_update_otp_set)
    delete_otp_set = staticmethod(db_delete_otp_set)
    delete_all_otp_sets = staticmethod(db_delete_all_otp_sets)


db = _DB()


async def db_call(fn, *args, **kwargs):
    return await fn(*args, **kwargs)


# ------------------------------------------------------------
# python-telegram-bot -> aiogram compatibility layer
#
# fordet's UI code (login, logout, getallid, otpgroups and every button/screen
# behind them) is copied verbatim. These small adapters expose the handful of
# PTB calls that code makes (reply_text, edit_message_text, answer, user_data,
# context.bot, ...) on top of aiogram objects, so the message text, inline
# buttons and flows stay byte-for-byte identical to fordet.
# ------------------------------------------------------------
def InlineKeyboardButton(text, callback_data=None, **kwargs):
    return _AIButton(text=text, callback_data=callback_data, **kwargs)


def InlineKeyboardMarkup(inline_keyboard):
    return _AIMarkup(inline_keyboard=inline_keyboard)


class ContextTypes:
    DEFAULT_TYPE = Any


class Update:
    pass


def escape_markdown(text: str, version: int = 1, entity_type: Optional[str] = None) -> str:
    """Same escaping rules as telegram.helpers.escape_markdown."""
    if version == 1:
        escape_chars = r"_*`["
    else:
        escape_chars = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)


def _clean_phone_number(text: str) -> str:
    return '+' + ''.join(c for c in text if c.isdigit())


user_data_store: Dict[int, Dict[str, Any]] = {}


def _is_not_modified(exc: Exception) -> bool:
    return "message is not modified" in str(exc).lower()


class PTBMessage:
    def __init__(self, message: Message):
        self._m = message

    @property
    def chat(self):
        return self._m.chat

    @property
    def message_id(self):
        return self._m.message_id

    @property
    def text(self):
        return self._m.text

    @property
    def from_user(self):
        return self._m.from_user

    async def reply_text(self, text, reply_markup=None, parse_mode=None, **kwargs):
        sent = await self._m.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return PTBMessage(sent)

    async def edit_text(self, text, reply_markup=None, parse_mode=None, **kwargs):
        try:
            await self._m.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except TelegramBadRequest as e:
            if not _is_not_modified(e):
                raise

    async def delete(self):
        try:
            await self._m.delete()
        except Exception:
            pass


class PTBCallbackQuery:
    def __init__(self, callback: CallbackQuery):
        self._cb = callback
        self.data = callback.data
        self.from_user = callback.from_user
        self.message = PTBMessage(callback.message) if callback.message else None

    async def answer(self, text=None, show_alert=False, **kwargs):
        try:
            await self._cb.answer(text=text, show_alert=show_alert)
        except Exception:
            pass

    async def edit_message_text(self, text, reply_markup=None, parse_mode=None, **kwargs):
        await self.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)


class PTBUpdate:
    def __init__(self, message: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
        self.message = PTBMessage(message) if message is not None else None
        self.callback_query = PTBCallbackQuery(callback) if callback is not None else None
        self.effective_user = (message or callback).from_user


class PTBBot:
    def __init__(self, bot: Bot):
        self._bot = bot

    async def send_message(self, chat_id, text, reply_markup=None, parse_mode=None, **kwargs):
        sent = await self._bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return PTBMessage(sent)

    async def edit_message_text(self, text=None, chat_id=None, message_id=None,
                                reply_markup=None, parse_mode=None, **kwargs):
        try:
            await self._bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id,
                                              reply_markup=reply_markup, parse_mode=parse_mode)
        except TelegramBadRequest as e:
            if not _is_not_modified(e):
                raise


class PTBContext:
    def __init__(self, user_id: int):
        self.user_data = user_data_store.setdefault(user_id, {})
        self.bot = PTBBot(get_bot())


def ptb_from_message(message: Message):
    return PTBUpdate(message=message), PTBContext(message.from_user.id)


def ptb_from_callback(callback: CallbackQuery):
    return PTBUpdate(callback=callback), PTBContext(callback.from_user.id)


# ------------------------------------------------------------
# Client health / reconnect (fordet)
# ------------------------------------------------------------
_health_reconnect_in_progress: set = set()
_health_reconnect_cooldown: Dict[int, float] = {}
_HEALTH_RECONNECT_COOLDOWN = 60.0
_CONNECT_TIMEOUT = 15.0


async def _post_reconnect_recovery(user_id: int):
    """Re-register every live handler after a reconnect (OTP groups + SUCCESS/FAILED chat)."""
    try:
        try:
            await update_otp_handlers_for_user(user_id)
            active = [k for k, s in otp_search_manager.searches.items()
                      if s.get('user_id') == user_id and not s.get('cancelled') and not s.get('found_otp')]
            if active:
                logger.info(f"[Recovery] OTP live handlers re-registered for user {user_id} "
                            f"— {len(active)} active search(es) still watching")
        except Exception as e:
            logger.warning(f"[Recovery] OTP live handler re-registration failed for {user_id}: {e}")

        try:
            await update_result_handler_for_user(user_id)
        except Exception as e:
            logger.warning(f"[Recovery] Result live handler re-registration failed for {user_id}: {e}")

    except Exception as e:
        logger.warning(f"[Recovery] _post_reconnect_recovery error for {user_id}: {e}")


async def _do_connect(user_id: int, wrapper: ClientWrapper) -> bool:
    """
    Part 1 — fast inline connect: creates a fresh TelegramClient from the stored
    session and updates the wrapper so all existing references (OTP workers,
    live handlers, send path etc.) transparently use the new clean client.
    Wrapped in a timeout so it never hangs the event loop.
    """
    try:
        async def _connect():
            try:
                await wrapper.get_inner().disconnect()
            except Exception:
                pass
            user_record = await db_get_user(user_id)
            session_data = user_record.get("session_data") if user_record else None
            if not session_data:
                logger.warning(f"[HealthCheck] No session data found for user {user_id}")
                return False
            new_client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
            await new_client.connect()
            if new_client.is_connected():
                wrapper.update(new_client)
                return True
            return False
        return await asyncio.wait_for(_connect(), timeout=_CONNECT_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(f"[HealthCheck] Connect timed out for user {user_id}")
        return False
    except Exception as e:
        logger.warning(f"[HealthCheck] Connect failed for user {user_id}: {e}")
        return False


async def _do_recover(user_id: int, wrapper: ClientWrapper):
    """
    Part 2 — background recovery: all heavy work after connect succeeds.
    Fired as asyncio.create_task so it never blocks the caller.
    """
    client = wrapper.get_inner()
    try:
        asyncio.create_task(_preload_user_dialogs(user_id, client, "reconnect"))
        await _post_reconnect_recovery(user_id)
    except Exception as e:
        logger.warning(f"[HealthCheck] Recovery exception for user {user_id}: {e}")
    finally:
        _health_reconnect_in_progress.discard(user_id)


async def _ensure_client_healthy(user_id: int, client) -> bool:
    """
    Health check that guarantees the client is connected before returning.
    Accepts either a ClientWrapper or raw TelegramClient.
    - If disconnected: awaits _do_connect (creates fresh client, updates wrapper)
      then fires _do_recover as background task for heavy work.
    - If connected: returns True immediately — no network calls.
    """
    if not client.is_connected():
        if time.time() - _health_reconnect_cooldown.get(user_id, 0) < _HEALTH_RECONNECT_COOLDOWN:
            return True
        if user_id not in _health_reconnect_in_progress:
            _health_reconnect_in_progress.add(user_id)
            wrapper = user_clients.get(user_id)
            if not isinstance(wrapper, ClientWrapper):
                _health_reconnect_in_progress.discard(user_id)
                return False
            connected = await _do_connect(user_id, wrapper)
            if connected:
                logger.info(f"[HealthCheck] Reconnect successful for user {user_id} — fresh client created")
                _health_reconnect_cooldown[user_id] = time.time()
                asyncio.create_task(_do_recover(user_id, wrapper))
            else:
                _health_reconnect_in_progress.discard(user_id)
                return False
    return True


# ------------------------------------------------------------
# Auto-OTP search (fordet live-handler engine)
#
# Same engine as fordet: one backfill scan + permanent live handlers on every
# OTP group, pure in-memory pattern matching, no polling. The only change is
# the terminal action: instead of replying to a duplicate-notification message,
# the found OTP resolves a future that the number flow is waiting on.
# ------------------------------------------------------------
class AutoOTPSearchManager:

    def __init__(self):
        self.searches: Dict[str, Dict] = {}
        self.active_searches: Set[str] = set()
        self._meta_lock = asyncio.Lock()
        self._key_locks: Dict[str, asyncio.Lock] = {}

    def _get_key_lock(self, key: str) -> asyncio.Lock:
        if key not in self._key_locks:
            self._key_locks[key] = asyncio.Lock()
        return self._key_locks[key]

    async def start_search(self, user_id: int, search_key: str, extracted_number: str,
                           task_label: str = "", set_name: Optional[str] = None) -> Optional[asyncio.Future]:
        """
        Registers a search and returns a future that resolves to
        (otp, set_name) when found, or None when the search times out / is cancelled.
        Returns None (not a future) if there is nothing to search with.
        """
        otp_sets = otp_sets_cache.get(user_id, [])
        if set_name is not None:
            otp_sets = [s for s in otp_sets if s.get('name') == set_name]
        if not otp_sets:
            logger.debug(f"No OTP sets for user {user_id}, skipping search")
            return None

        pure_number = extract_strict_numbers_only(extracted_number)
        if not pure_number:
            logger.debug(f"No valid numbers extracted from: {extracted_number}")
            return None

        all_search_patterns = _build_search_patterns(pure_number, otp_sets)
        if not all_search_patterns:
            logger.debug(f"No valid search patterns created for user {user_id}")
            return None

        future: asyncio.Future = asyncio.get_running_loop().create_future()

        async with self._meta_lock:
            if search_key in self.active_searches:
                old = self.searches.get(search_key)
                if old and old.get('future') and not old['future'].done():
                    old['future'].set_result(None)
            self.active_searches.add(search_key)
            if search_key not in self._key_locks:
                self._key_locks[search_key] = asyncio.Lock()

            self.searches[search_key] = {
                'user_id':          user_id,
                'search_key':       search_key,
                'extracted_number': pure_number,
                'task_label':       task_label,
                'all_sets':         all_search_patterns,
                'start_time':       time.time(),
                'end_time':         time.time() + OTP_SEARCH_DURATION,
                'cancelled':        False,
                'found_otp':        False,
                'future':           future,
                'backfill_done':    asyncio.Event(),   # set when the history scan has finished (found or not)
            }

        logger.info(
            f"🔍 OTP search started for {search_key} "
            f"— {len(all_search_patterns)} set(s), live handler active, "
            f"backfill covers last {OTP_BACKFILL_WINDOW_SECONDS}s"
        )

        asyncio.create_task(self._run_backfill(search_key))
        asyncio.create_task(self._timeout_search(search_key))
        return future

    async def _run_backfill(self, search_key: str):
        info = self.searches.get(search_key)
        try:
            await self._run_backfill_inner(search_key)
        finally:
            if info and info.get('backfill_done') is not None:
                info['backfill_done'].set()

    async def _run_backfill_inner(self, search_key: str):
        """
        One-shot iter_messages scan covering the last OTP_BACKFILL_WINDOW_SECONDS.
        Catches OTPs that were already posted before the search started.
        The search is already registered in active_searches before this task
        starts, so the live handler has zero-gap coverage from this point forward.
        """
        search_info = self.searches.get(search_key)
        if not search_info or search_info['cancelled'] or search_info['found_otp']:
            return

        user_id           = search_info['user_id']
        time_window_start = time.time() - OTP_BACKFILL_WINDOW_SECONDS

        client = user_clients.get(user_id)
        if not client:
            return
        if not await _ensure_client_healthy(user_id, client):
            return

        for set_info in search_info['all_sets']:
            if search_info['cancelled'] or search_info['found_otp']:
                return

            set_name = set_info['set_name']

            group_pattern_map: Dict[int, List] = {}
            for pattern_info in set_info['patterns']:
                pattern = pattern_info.get('pattern', '')
                if not pattern:
                    continue
                for group_id in pattern_info.get('group_ids', []):
                    group_pattern_map.setdefault(group_id, [])
                    if pattern not in group_pattern_map[group_id]:
                        group_pattern_map[group_id].append(pattern)

            for group_id, patterns in group_pattern_map.items():
                if search_info['cancelled'] or search_info['found_otp']:
                    return

                gid_chat, gid_topic = parse_chat_id(group_id)
                try:
                    entity = await client.get_input_entity(gid_chat)
                except Exception as e:
                    logger.debug(f"[Backfill] Failed to get entity for group {group_id}: {e}")
                    continue

                try:
                    iter_kwargs: dict = {"limit": OTP_BACKFILL_MAX_MESSAGES}
                    if gid_topic is not None:
                        iter_kwargs["reply_to"] = gid_topic

                    async for message in client.iter_messages(entity, **iter_kwargs):
                        if search_info['cancelled'] or search_info['found_otp']:
                            return

                        if message.date:
                            msg_time = message.date.replace(tzinfo=timezone.utc).timestamp()
                            if msg_time < time_window_start:
                                break

                        msg_text = extract_message_text_only(message)
                        if not msg_text:
                            continue

                        for pattern in patterns:
                            if pattern in msg_text:
                                otp = OTPFormatParser.extract_otp(extract_full_message(message))
                                if otp:
                                    logger.info(
                                        f"✅ [Backfill] OTP '{otp}' found in group {group_id} "
                                        f"(pattern '{pattern}', set '{set_name}')"
                                    )
                                    await self._handle_otp_found(search_key, otp, set_name)
                                    return

                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug(f"[Backfill] Error scanning group {group_id}: {e}")
                    continue

        logger.debug(f"[Backfill] Complete for {search_key} — OTP not in history, live handler watching")

    async def _timeout_search(self, search_key: str):
        """Lightweight timeout: no API calls, just sleep and cancel."""
        search_info = self.searches.get(search_key)
        if not search_info:
            return

        wait = search_info['end_time'] - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        if search_key in self.active_searches and self.searches.get(search_key) is search_info:
            logger.debug(f"OTP search timed out for {search_key}")
            await self.cancel_search(search_key)

    async def check_message(self, user_id: int, group_id: int, message) -> bool:
        """
        Called by the live OTP group handler for every new message in an OTP group.
        Checks the message against all active searches for this user.
        Zero API calls — pure in-memory pattern matching.
        """
        msg_text = extract_message_text_only(message)
        if not msg_text:
            return False

        for search_key in list(self.active_searches):
            search_info = self.searches.get(search_key)
            if not search_info:
                continue
            if search_info['user_id'] != user_id:
                continue
            if search_info['cancelled'] or search_info['found_otp']:
                continue
            if time.time() > search_info['end_time']:
                continue

            for set_info in search_info['all_sets']:
                for pattern_info in set_info['patterns']:
                    raw_gids = pattern_info.get('group_ids', [])
                    if not any(parse_chat_id(gid)[0] == group_id for gid in raw_gids):
                        continue
                    pattern = pattern_info.get('pattern', '')
                    if not pattern or pattern not in msg_text:
                        continue

                    otp = OTPFormatParser.extract_otp(extract_full_message(message))
                    if otp:
                        logger.info(
                            f"✅ [Live] OTP '{otp}' found for {search_key} "
                            f"in group {group_id} (pattern '{pattern}', set '{set_info['set_name']}')"
                        )
                        await self._handle_otp_found(search_key, otp, set_info['set_name'])
                        return True

        return False

    async def _handle_otp_found(self, search_key: str, otp: str, set_name: str):
        """Marks the search as done and hands the OTP to whoever is waiting on the future."""
        clean_otp = otp.replace('-', '')

        async with self._get_key_lock(search_key):
            search_info = self.searches.get(search_key)
            if not search_info or search_info.get('found_otp'):
                return
            search_info['found_otp'] = True
            search_info['cancelled'] = True
            async with self._meta_lock:
                self.active_searches.discard(search_key)

            future = search_info.get('future')
            if future is not None and not future.done():
                future.set_result((clean_otp, set_name))

            self.searches.pop(search_key, None)
            self._key_locks.pop(search_key, None)

    async def cancel_search(self, search_key: str):
        async with self._get_key_lock(search_key):
            search_info = self.searches.pop(search_key, None)
            async with self._meta_lock:
                self.active_searches.discard(search_key)
            self._key_locks.pop(search_key, None)
            if search_info:
                search_info['cancelled'] = True
                future = search_info.get('future')
                if future is not None and not future.done():
                    future.set_result(None)
            logger.debug(f"Cancelled OTP search for {search_key}")


otp_search_manager = AutoOTPSearchManager()


# ------------------------------------------------------------
# Live handlers (fordet style: permanent NewMessage handlers on the user's client)
# ------------------------------------------------------------
async def update_otp_handlers_for_user(user_id: int):
    """
    Register one NewMessage handler per unique OTP group for this user.
    Called at login, session restore, reconnect, and whenever OTP sets change.
    Handlers are permanent — they cost nothing until a message arrives.
    The handler calls otp_search_manager.check_message() which is pure
    in-memory pattern matching against active_searches: zero API calls.
    """
    client = user_clients.get(user_id)
    if not client:
        return

    _clear_otp_handlers(user_id, client)

    otp_sets = otp_sets_cache.get(user_id, [])
    if not otp_sets:
        return

    group_ids: Set[int] = set()
    for otp_set in otp_sets:
        for gid in otp_set.get('group_ids', []):
            physical_gid, _ = parse_chat_id(gid)
            group_ids.add(physical_gid)

    if not group_ids:
        return

    registered = 0
    for group_id in group_ids:
        def _make_handler(gid: int):
            async def _otp_live_handler(event):
                try:
                    message = event.message
                    if not message:
                        return
                    await otp_search_manager.check_message(user_id, gid, message)
                except Exception as e:
                    logger.debug(f"OTP live handler error (user {user_id}, group {gid}): {e}")
            return _otp_live_handler

        handler = _make_handler(group_id)
        try:
            client.add_event_handler(handler, events.NewMessage(chats=group_id))
            otp_live_handler_registered.setdefault(user_id, []).append(handler)
            registered += 1
        except Exception as e:
            logger.debug(f"Failed to register OTP live handler for group {group_id}: {e}")

    logger.info(f"✅ OTP live handlers registered for user {user_id}: {registered}/{len(group_ids)} group(s)")


def _clear_result_handlers(user_id: int, client):
    """Remove the live SUCCESS/FAILED handler for a user."""
    handlers = result_live_handler_registered.pop(user_id, [])
    result_live_handler_chat.pop(user_id, None)
    for handler in handlers:
        try:
            client.remove_event_handler(handler)
        except Exception:
            pass


def check_result_message(user_id: int, text: str) -> bool:
    """
    Pure in-memory SUCCESS/FAILED matching against the numbers currently waiting
    for a decision. Zero API calls. Returns True if a waiting number was resolved.
    """
    if not text or ("SUCCESS" not in text and "FAILED" not in text):
        return False
    hit = False
    for key, ev in list(live_success_failure_events.items()):
        uid, num = key
        if uid != user_id or num not in text:
            continue
        if "SUCCESS" in text:
            live_success_failure_results[key] = "SUCCESS"
        elif "FAILED" in text:
            live_success_failure_results[key] = "FAILED"
        ev.set()
        hit = True
    return hit


async def update_result_handler_for_user(user_id: int):
    """
    Register the permanent NewMessage handler on the user's target chat that
    decides SUCCESS / FAILED for the number currently being processed.
    Called at login, session restore, reconnect and whenever /setchatid changes.
    """
    client = user_clients.get(user_id)
    if not client:
        return

    _clear_result_handlers(user_id, client)

    chat_id = await _get_user_target_chat(user_id)
    if not chat_id:
        return

    async def _result_live_handler(event):
        try:
            message = event.message
            if not message:
                return
            check_result_message(user_id, extract_message_text_only(message))
        except Exception as e:
            logger.debug(f"Result live handler error (user {user_id}, chat {chat_id}): {e}")

    try:
        client.add_event_handler(_result_live_handler, events.NewMessage(chats=chat_id))
        result_live_handler_registered.setdefault(user_id, []).append(_result_live_handler)
        result_live_handler_chat[user_id] = chat_id
        logger.info(f"✅ SUCCESS/FAILED live handler registered for user {user_id} on chat {chat_id}")
    except Exception as e:
        logger.warning(f"Failed to register SUCCESS/FAILED live handler for chat {chat_id}: {e}")


async def start_live_handlers_for_user(user_id: int):
    """Everything a freshly logged-in / restored client needs registered."""
    sets = await db_get_otp_sets(user_id)
    otp_sets_cache[user_id] = sets
    await update_otp_handlers_for_user(user_id)
    await update_result_handler_for_user(user_id)


# ------------------------------------------------------------
# No-tag send (fordet forward_message_direct, forward_tag=False path)
#
# fordet queues the send and returns immediately. The batch flow here needs to
# know the number was actually delivered before it starts waiting for
# SUCCESS/FAILED, so the same logic (flood-wait check, health check, entity
# resolve, topic reply_to, FloodWait handling) runs inline with bounded retries.
# ------------------------------------------------------------
SEND_MAX_ATTEMPTS = int(os.getenv("SEND_MAX_ATTEMPTS", "5"))
SEND_RETRY_SLEEP = float(os.getenv("SEND_RETRY_SLEEP", "1"))


def get_bot():
    if bot_instance is None:
        raise RuntimeError("get_bot() called before the bot was initialised")
    return bot_instance


async def forward_message_direct(user_id: int, target_id, message_text: str) -> Tuple[bool, str]:
    """Send message_text to target_id as the logged-in user, with no forward tag.
    Returns (ok, reason)."""
    last_reason = "unknown"
    for _attempt in range(SEND_MAX_ATTEMPTS):
        in_flood_wait, wait_left, should_notify_end = flood_wait_manager.is_in_flood_wait(user_id)
        if should_notify_end:
            asyncio.create_task(notify_user_flood_wait_ended(user_id))
        if in_flood_wait:
            last_reason = "flood_wait"
            await asyncio.sleep(wait_left + 1)
            continue

        client = user_clients.get(user_id)
        if not client:
            return False, "not_logged_in"

        if not await _ensure_client_healthy(user_id, client):
            last_reason = "connection"
            await asyncio.sleep(SEND_RETRY_SLEEP)
            continue

        entity = _get_cached_target(user_id, str(target_id))
        if not entity:
            entity = await resolve_target_entity_once(user_id, client, target_id)
        if not entity:
            last_reason = "entity_unresolved"
            await asyncio.sleep(SEND_RETRY_SLEEP)
            continue

        _, target_topic_id = parse_chat_id(target_id)
        send_kwargs = {}
        if target_topic_id is not None:
            send_kwargs['reply_to'] = target_topic_id

        try:
            await client.send_message(entity, message_text, **send_kwargs)

            in_fw, _, should_notify_end = flood_wait_manager.is_in_flood_wait(user_id)
            flood_wait_manager.clear_flood_wait(user_id)
            if should_notify_end:
                asyncio.create_task(notify_user_flood_wait_ended(user_id))
            return True, ""

        except FloodWaitError as fwe:
            wait = int(getattr(fwe, "seconds", 10))
            should_notify_start, wait_time = flood_wait_manager.set_flood_wait(user_id, wait)
            if should_notify_start and wait_time > 30:
                asyncio.create_task(notify_user_flood_wait(user_id, wait_time))
            last_reason = "flood_wait"
            continue

        except Exception as e:
            logger.debug(f"Send to {target_id} failed for user {user_id}: {e}")
            last_reason = "send_error"
            await asyncio.sleep(SEND_RETRY_SLEEP)
            continue

    return False, last_reason


# ------------------------------------------------------------
# fordet UI: login, logout, getallid, otpgroups (copied verbatim from fordet.py; gates removed)
# ------------------------------------------------------------
async def otpgroups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in otp_sets_cache:
        otp_sets = await db_call(db.get_otp_sets, user_id)
        otp_sets_cache[user_id] = otp_sets
    
    await show_otp_main_menu(update, context, user_id)


async def show_otp_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    message = update.message if update.message else update.callback_query.message
    is_callback = bool(update.callback_query)
    
    otp_sets = otp_sets_cache.get(user_id, [])
    total_sets = len(otp_sets)
    
    page = context.user_data.get('otp_page', 0)
    items_per_page = 10
    total_pages = max(1, (total_sets + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_sets)
    page_sets = otp_sets[start_idx:end_idx]
    
    if total_sets == 0:
        examples = OTPFormatParser.format_display_examples()
        text = (
            "📋 **OTP Groups Manager**\n\n"
            "No OTP search sets configured.\n\n"
            f"{examples}\n\n"
            "**Format:** `Name = [pattern] = [group IDs]`\n"
            "**Multiple formats allowed!** Separate with spaces.\n\n"
            "**Need group IDs?** Use /getallid to find them.\n\n"
            "**Example with multiple formats:**\n"
            "`WORK = 5|***|2 2|TGT|3 = -100987654321`"
        )
    else:
        text = f"📋 **OTP Groups Manager**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        if total_pages > 1:
            text += f" (Page {page + 1}/{total_pages})"
        text += f"\n\n{total_sets} set{'s' if total_sets != 1 else ''} configured:\n\n"
        
        for i, s in enumerate(page_sets, start_idx + 1):
            formats = '`, `'.join(s['formats'])
            groups = ', '.join(str(g) for g in s['group_ids'][:2])
            if len(s['group_ids']) > 2:
                groups += f" +{len(s['group_ids']) - 2} more"
            text += f"**#{i} {s['name']}**\n"
            text += f"📋 Formats ({len(s['formats'])}): `{formats}`\n"
            text += f"🔍 Groups: `{groups}`\n\n"
    
    keyboard = []
    
    if total_sets > 0:
        keyboard.append([
            InlineKeyboardButton("➕ Add", callback_data="otp_add"),
            InlineKeyboardButton("✏️ Edit", callback_data="otp_edit"),
            InlineKeyboardButton("❌ Delete", callback_data="otp_delete")
        ])
        keyboard.append([InlineKeyboardButton("🗑 Delete All", callback_data="otp_delete_all")])
        
        if total_pages > 1:
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="otp_page_prev"))
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="otp_page_next"))
            if nav_row:
                keyboard.append(nav_row)
    else:
        keyboard.append([InlineKeyboardButton("➕ Add New OTP Set", callback_data="otp_add")])
    
    keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")])
    
    if is_callback:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        await update.callback_query.answer()
    else:
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def show_otp_add_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    examples = OTPFormatParser.format_display_examples()
    
    text = (
        "➕ **Add New OTP Set**\n\n"
        "**📝 Multiple Sets Supported!**\n"
        "You can add multiple sets at once by putting each on a new line.\n\n"
        "**🔄 Overwrite Feature:**\n"
        "If you use a name that already exists, it will **update** that set with the new formats and groups.\n\n"
        "**Format for each set:**\n"
        "`Name = [pattern] = [group IDs]`\n\n"
        f"{examples}\n\n"
        "**💡 Multiple Formats Per Set Allowed:**\n"
        "Separate multiple formats with spaces.\n\n"
        "**🔍 Group IDs:**\n"
        "Need IDs? Use /getallid to find chat IDs for OTP groups.\n\n"
        "**📝 Example (Multiple Sets at Once):**\n"
        "```\n"
        "HK = 2|TGT|0 = -100123456789\n"
        "CWF = 4|eye3|2 = -100123459876\n"
        "WORK = 5|***|2 2|TGT|3 = -100987654321 -100987651234\n"
        "```\n\n"
        "**Copy and paste the entire block above, or type your own!**"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="otp_main")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['otp_action'] = 'adding'


async def show_otp_edit_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    total_sets = len(otp_sets)
    
    if total_sets == 0:
        await query.edit_message_text(
            "📋 **No OTP sets to edit.**\n\nUse /otpgroups to add one first.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="otp_main")]]),
            parse_mode="Markdown"
        )
        return
    
    page = context.user_data.get('otp_edit_page', 0)
    items_per_page = 10
    total_pages = max(1, (total_sets + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_sets)
    page_sets = otp_sets[start_idx:end_idx]
    
    text = f"✏️ **Edit OTP Set**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if total_pages > 1:
        text += f" (Page {page + 1}/{total_pages})"
    text += f"\n\nSelect set to edit:\n\n"
    
    for i, s in enumerate(page_sets, start_idx + 1):
        formats = ', '.join(s['formats'])
        groups = ', '.join(str(g) for g in s['group_ids'][:1])
        if len(s['group_ids']) > 1:
            groups += f" +{len(s['group_ids']) - 1} more"
        text += f"#{i} **{s['name']}**\n"
        text += f"   📋 Formats: `{formats}`\n"
        text += f"   🔍 Groups: `{groups}`\n\n"
    
    keyboard = []
    
    first_row = []
    second_row = []
    
    for idx, s in enumerate(page_sets, start_idx + 1):
        button = InlineKeyboardButton(str(idx), callback_data=f"otp_edit_set_{s['set_index']}")
        if idx - start_idx <= 5:
            first_row.append(button)
        else:
            second_row.append(button)
    
    if first_row:
        keyboard.append(first_row)
    if second_row:
        keyboard.append(second_row)
    
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="otp_edit_page_prev"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="otp_edit_page_next"))
        if nav_row:
            keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="otp_main")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_otp_edit_options(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.answer("Set not found!", show_alert=True)
        return
    
    set_number = next((i + 1 for i, s in enumerate(otp_sets) if s['set_index'] == set_index), set_index + 1)
    
    formats_display = '`, `'.join(target_set['formats'])
    groups_display = ', '.join(str(g) for g in target_set['group_ids'])
    
    text = (
        f"✏️ **Set #{set_number} - {target_set['name']}**\n\n"
        f"**Formats ({len(target_set['formats'])} total):**\n"
        f"`{formats_display}`\n\n"
        f"**Groups:**\n"
        f"`{groups_display}`\n\n"
        f"**Need group IDs?** Use /getallid to find them."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("🧩 Edit Format", callback_data=f"otp_edit_format_{set_index}"),
            InlineKeyboardButton("🏷 Edit Groups", callback_data=f"otp_edit_groups_{set_index}")
        ],
        [InlineKeyboardButton("📝 Rename", callback_data=f"otp_rename_{set_index}")],
        [InlineKeyboardButton("🔙 Back to List", callback_data="otp_edit")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['otp_edit_set'] = set_index


async def show_otp_edit_format_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.answer("Set not found!", show_alert=True)
        return
    
    set_number = next((i + 1 for i, s in enumerate(otp_sets) if s['set_index'] == set_index), set_index + 1)
    current = ', '.join(target_set['formats'])
    examples = OTPFormatParser.format_display_examples()
    
    text = (
        f"🧩 **Edit Format** – Set #{set_number} - {target_set['name']}\n\n"
        f"**Current Format{'s' if len(target_set['formats']) > 1 else ''}:**\n"
        f"`{current}`\n\n"
        f"**Total Formats:** {len(target_set['formats'])}\n\n"
        f"{examples}\n\n"
        f"**💡 Multiple Formats Allowed:**\n"
        f"Send new format pattern(s) separated by spaces.\n"
        f"Example: `5|***|2 2|TGT|3`\n\n"
        f"**Current formats will be REPLACED** with your new input."
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['otp_action'] = 'editing_format'
    context.user_data['otp_edit_set'] = set_index


async def show_otp_edit_groups_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.answer("Set not found!", show_alert=True)
        return
    
    set_number = next((i + 1 for i, s in enumerate(otp_sets) if s['set_index'] == set_index), set_index + 1)
    current = ', '.join(str(g) for g in target_set['group_ids'])
    
    text = (
        f"🏷 **Edit Groups** – Set #{set_number} - {target_set['name']}\n\n"
        f"Current: `{current}`\n\n"
        f"Send new group ID(s):\n\n"
        f"Need IDs? Use /getallid to find chat IDs for OTP groups.\n\n"
        f"**Examples:**\n"
        f"• Single: `-100123456789`\n"
        f"• Multiple: `-100123456789 -100987654321`"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['otp_action'] = 'editing_groups'
    context.user_data['otp_edit_set'] = set_index


async def show_otp_rename_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.answer("Set not found!", show_alert=True)
        return
    
    set_number = next((i + 1 for i, s in enumerate(otp_sets) if s['set_index'] == set_index), set_index + 1)
    
    text = (
        f"📝 **Rename Set** – Set #{set_number}\n\n"
        f"Current name: **{target_set['name']}**\n\n"
        f"Send new name for this set:"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['otp_action'] = 'renaming'
    context.user_data['otp_edit_set'] = set_index


async def show_otp_delete_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    total_sets = len(otp_sets)
    
    if total_sets == 0:
        await query.edit_message_text(
            "📋 **No OTP sets to delete.**\n\nUse /otpgroups to add one first.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="otp_main")]]),
            parse_mode="Markdown"
        )
        return
    
    page = context.user_data.get('otp_delete_page', 0)
    items_per_page = 10
    total_pages = max(1, (total_sets + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_sets)
    page_sets = otp_sets[start_idx:end_idx]
    
    text = f"❌ **Delete OTP Set**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if total_pages > 1:
        text += f" (Page {page + 1}/{total_pages})"
    text += f"\n\nSelect set to delete:\n\n⚠️ Deleted sets cannot be recovered!\n\n"
    
    for i, s in enumerate(page_sets, start_idx + 1):
        formats = ', '.join(s['formats'])
        groups = ', '.join(str(g) for g in s['group_ids'][:1])
        if len(s['group_ids']) > 1:
            groups += f" +{len(s['group_ids']) - 1} more"
        text += f"#{i} **{s['name']}** `{formats}` → `{groups}`\n"
    
    keyboard = []
    
    first_row = []
    second_row = []
    
    for idx, s in enumerate(page_sets, start_idx + 1):
        button = InlineKeyboardButton(str(idx), callback_data=f"otp_delete_set_{s['set_index']}")
        if idx - start_idx <= 5:
            first_row.append(button)
        else:
            second_row.append(button)
    
    if first_row:
        keyboard.append(first_row)
    if second_row:
        keyboard.append(second_row)
    
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="otp_delete_page_prev"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="otp_delete_page_next"))
        if nav_row:
            keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="otp_main")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_otp_delete_all_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    otp_sets = otp_sets_cache.get(user_id, [])
    total_sets = len(otp_sets)

    if total_sets == 0:
        await query.answer("No OTP sets to delete!", show_alert=True)
        return

    text = (
        f"⚠️ **Confirm Delete All**\n\n"
        f"This will permanently delete **all {total_sets} OTP set{'s' if total_sets != 1 else ''}**.\n\n"
        f"This cannot be undone."
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete All", callback_data="otp_confirm_delete_all"),
            InlineKeyboardButton("❌ No", callback_data="otp_main")
        ]
    ]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def confirm_otp_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    deleted_count = await db_call(db.delete_all_otp_sets, user_id)

    if user_id in otp_sets_cache:
        del otp_sets_cache[user_id]

    fresh_sets = await db_call(db.get_otp_sets, user_id)
    otp_sets_cache[user_id] = fresh_sets
    asyncio.create_task(update_otp_handlers_for_user(user_id))

    context.user_data['otp_page'] = 0

    await query.edit_message_text(
        f"✅ **All OTP sets deleted successfully!**\n\n"
        f"{deleted_count} set{'s' if deleted_count != 1 else ''} removed.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")]]),
        parse_mode="Markdown"
    )

    logger.info(f"User {user_id} successfully deleted all ({deleted_count}) OTP sets")


async def show_otp_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.answer("Set not found!", show_alert=True)
        return
    
    set_number = next((i + 1 for i, s in enumerate(otp_sets) if s['set_index'] == set_index), set_index + 1)
    
    formats = ', '.join(target_set['formats'])
    groups = ', '.join(str(g) for g in target_set['group_ids'])
    
    text = (
        f"⚠️ **Confirm Deletion**\n\n"
        f"Set #{set_number} - **{target_set['name']}**\n"
        f"Format: `{formats}`\n"
        f"Groups: `{groups}`\n\n"
        f"Delete this set? This cannot be undone."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"otp_confirm_delete_{set_index}"),
            InlineKeyboardButton("❌ No", callback_data="otp_delete")
        ]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def confirm_otp_delete(update: Update, context: ContextTypes.DEFAULT_TYPE, set_index: int):
    query = update.callback_query
    user_id = query.from_user.id
    
    otp_sets = otp_sets_cache.get(user_id, [])
    target_set = next((s for s in otp_sets if s['set_index'] == set_index), None)
    
    if not target_set:
        await query.edit_message_text(
            "❌ **Set not found!**\n\nIt may have been already deleted.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")]]),
            parse_mode="Markdown"
        )
        return
    
    deleted = await db_call(db.delete_otp_set, user_id, set_index)
    
    if deleted:
        if user_id in otp_sets_cache:
            del otp_sets_cache[user_id]
        
        current_page = context.user_data.get('otp_delete_page', 0)
        
        fresh_sets = await db_call(db.get_otp_sets, user_id)
        otp_sets_cache[user_id] = fresh_sets
        asyncio.create_task(update_otp_handlers_for_user(user_id))
        
        items_per_page = 10
        total_sets = len(fresh_sets)
        total_pages = max(1, (total_sets + items_per_page - 1) // items_per_page)
        
        if current_page >= total_pages:
            context.user_data['otp_delete_page'] = max(0, total_pages - 1)
        
        await query.edit_message_text(
            f"✅ **Set #{set_index + 1} - {target_set['name']} deleted successfully!**\n\n"
            f"Remaining sets: {total_sets}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")]]),
            parse_mode="Markdown"
        )
        
        logger.info(f"User {user_id} successfully deleted OTP set {set_index}")
    else:
        await query.edit_message_text(
            "❌ **Failed to delete set!**\n\nPlease try again later.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")]]),
            parse_mode="Markdown"
        )


async def handle_otp_add_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    lines = text.split('\n')
    lines = [line.strip() for line in lines if line.strip()]
    
    if not lines:
        examples = OTPFormatParser.format_display_examples()
        await update.message.reply_text(
            f"❌ **No input provided!**\n\n"
            f"Use format for each set on a new line:\n"
            f"`Name = [pattern] = [group IDs]`\n\n"
            f"{examples}\n\n"
            f"**Format:** `Name = [pattern] = [group IDs]`\n"
            f"**Multiple formats allowed!** Separate with spaces.\n\n"
            f"**Need group IDs?** Use /getallid to find them.\n\n"
            f"**Example with multiple formats:**\n"
            f"`WORK = 5|***|2 2|TGT|3 = -100987654321`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="otp_main")]])
        )
        return
    
    parsed_sets = []
    failed_sets = []
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
            
        if '=' not in line:
            failed_sets.append(f"Line {line_num}: Missing '=' separators - `{line}`")
            continue
        
        parts = line.split('=')
        if len(parts) != 3:
            failed_sets.append(f"Line {line_num}: Must have exactly two '=' symbols - `{line}`")
            continue
        
        name = parts[0].strip()
        format_part = parts[1].strip()
        groups_part = parts[2].strip()
        
        if not name:
            failed_sets.append(f"Line {line_num}: Name cannot be empty - `{line}`")
            continue
        
        formats = format_part.split()
        if not formats:
            failed_sets.append(f"Line {line_num}: No format provided for '{name}'")
            continue
        
        valid_formats = []
        invalid_formats = []
        for f in formats:
            if f.count('|') == 2:
                valid_formats.append(f)
            else:
                invalid_formats.append(f)
        
        if invalid_formats:
            failed_sets.append(f"Line {line_num}: Invalid format{'s' if len(invalid_formats) > 1 else ''} for '{name}': `{'`, `'.join(invalid_formats)}`")
            continue
        
        try:
            group_ids = []
            for gid in groups_part.split():
                parsed = _parse_id_token(gid)
                if parsed is not None:
                    group_ids.append(parsed)
            
            if not group_ids:
                failed_sets.append(f"Line {line_num}: No valid group IDs for '{name}' - `{groups_part}`")
                continue
        except ValueError:
            failed_sets.append(f"Line {line_num}: Invalid group IDs for '{name}' - `{groups_part}`")
            continue
        
        parsed_sets.append({
            'line_num': line_num,
            'name': name,
            'formats': valid_formats,
            'group_ids': group_ids
        })
    
    if not parsed_sets:
        response = "❌ **Validation failed for all lines:**\n\n"
        for fail in failed_sets:
            response += f"• {fail}\n"
        await update.message.reply_text(
            response,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")]])
        )
        return
    
    existing_sets = await db_call(db.get_otp_sets_fresh, user_id)
    otp_sets_cache[user_id] = existing_sets
    existing_lower_map = {s['name'].lower(): s for s in existing_sets}
    
    to_update = []
    to_add = []
    
    for parsed in parsed_sets:
        existing = existing_lower_map.get(parsed['name'].lower())
        if existing:
            to_update.append((existing, parsed))
        else:
            to_add.append(parsed)
    
    successful_sets = []
    
    for existing, parsed in to_update:
        updated = await db_call(
            db.update_otp_set, 
            user_id, 
            existing['set_index'],
            formats=parsed['formats'], 
            group_ids=parsed['group_ids']
        )
        
        if updated:
            successful_sets.append({
                'name': parsed['name'],
                'formats': parsed['formats'],
                'formats_display': '`, `'.join(parsed['formats']),
                'groups_display': ', '.join(str(g) for g in parsed['group_ids']),
                'format_count': len(parsed['formats']),
                'group_count': len(parsed['group_ids']),
                'updated': True,
                'set_index': existing['set_index']
            })
        else:
            failed_sets.append(f"Line {parsed['line_num']}: Failed to update existing set '{parsed['name']}'")
    
    for parsed in to_add:
        added = await db_call(
            db.add_otp_set, 
            user_id, 
            parsed['name'], 
            parsed['formats'], 
            parsed['group_ids']
        )
        
        if added:
            successful_sets.append({
                'name': parsed['name'],
                'formats': parsed['formats'],
                'formats_display': '`, `'.join(parsed['formats']),
                'groups_display': ', '.join(str(g) for g in parsed['group_ids']),
                'format_count': len(parsed['formats']),
                'group_count': len(parsed['group_ids']),
                'updated': False
            })
        else:
            failed_sets.append(f"Line {parsed['line_num']}: Failed to add '{parsed['name']}' - database error")
    
    if successful_sets:
        fresh_sets = await db_call(db.get_otp_sets_fresh, user_id)
        otp_sets_cache[user_id] = fresh_sets
        asyncio.create_task(update_otp_handlers_for_user(user_id))
    
    response = ""
    
    if successful_sets:
        updated_count = sum(1 for s in successful_sets if s.get('updated'))
        added_count = len(successful_sets) - updated_count
        
        if added_count > 0 and updated_count > 0:
            response += f"✅ **OTP Sets Processed Successfully!**\n\n"
        elif updated_count > 0:
            response += f"✅ **OTP Sets Updated Successfully!**\n\n"
        else:
            response += f"✅ **OTP Sets Added Successfully!**\n\n"
            
        response += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for i, set_info in enumerate(successful_sets, 1):
            action_emoji = "🔄" if set_info.get('updated') else "➕"
            action_text = "Updated" if set_info.get('updated') else "Added"
            
            response += f"{action_emoji} **Set #{i}: {set_info['name']}** ({action_text})\n"
            response += f"📋 **Formats ({set_info['format_count']}):** `{set_info['formats_display']}`\n"
            response += f"🔍 **Groups ({set_info['group_count']}):** `{set_info['groups_display']}`\n\n"
        
        response += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        if added_count > 0 and updated_count > 0:
            response += f"📊 **Total:** {added_count} added, {updated_count} updated\n\n"
        else:
            response += f"📊 **Total:** {len(successful_sets)} set{'s' if len(successful_sets) > 1 else ''}\n\n"
        response += "💡 Multiple formats will be tried in order when searching for OTP."
    
    if failed_sets:
        if successful_sets:
            response += "\n\n"
        response += "❌ **Failed to Process:**\n"
        for fail in failed_sets[:10]:
            response += f"• {fail}\n"
        
        if len(failed_sets) > 10:
            response += f"• ... and {len(failed_sets) - 10} more errors\n"
        
        response += f"\n📊 **Failed:** {len(failed_sets)} set{'s' if len(failed_sets) > 1 else ''}"
    
    if successful_sets and failed_sets:
        response += f"\n\n📊 **Summary:** {len(successful_sets)} successful, {len(failed_sets)} failed"
    
    keyboard = []
    if successful_sets:
        keyboard.append([InlineKeyboardButton("➕ Add More", callback_data="otp_add")])
    keyboard.append([InlineKeyboardButton("🔙 Back to Manager", callback_data="otp_main")])
    
    await update.message.reply_text(
        response,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data.pop('otp_action', None)


async def handle_otp_edit_format_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    set_index = context.user_data.get('otp_edit_set')
    
    if set_index is None:
        return
    
    new_format_input = update.message.text.strip()
    
    new_formats = new_format_input.split()
    
    if not new_formats:
        examples = OTPFormatParser.format_display_examples()
        await update.message.reply_text(
            f"❌ **No formats provided!**\n\n"
            f"Please enter at least one format.\n\n"
            f"{examples}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data=f"otp_edit_set_{set_index}")]])
        )
        return
    
    valid_formats = []
    invalid_formats = []
    for f in new_formats:
        if f.count('|') == 2:
            valid_formats.append(f)
        else:
            invalid_formats.append(f)
    
    if invalid_formats:
        examples = OTPFormatParser.format_display_examples()
        await update.message.reply_text(
            f"❌ **Invalid format{'s' if len(invalid_formats) > 1 else ''}:** `{'`, `'.join(invalid_formats)}`\n\n"
            f"Each format must contain exactly 2 pipe symbols.\n\n"
            f"{examples}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data=f"otp_edit_set_{set_index}")]])
        )
        return
    
    updated = await db_call(db.update_otp_set, user_id, set_index, formats=valid_formats)
    
    if updated:
        fresh_sets = await db_call(db.get_otp_sets_fresh, user_id)
        otp_sets_cache[user_id] = fresh_sets
        asyncio.create_task(update_otp_handlers_for_user(user_id))
        
        formats_display = '`, `'.join(valid_formats)
        await update.message.reply_text(
            f"✅ **Format{'s' if len(valid_formats) > 1 else ''} Updated!**\n\n"
            f"Set #{set_index + 1} now has {len(valid_formats)} format{'s' if len(valid_formats) > 1 else ''}:\n"
            f"`{formats_display}`\n\n"
            f"These formats will be tried in order when searching for OTP.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "❌ **Failed to update formats!**",
            parse_mode="Markdown"
        )
    
    context.user_data.pop('otp_action', None)


async def handle_otp_edit_groups_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    set_index = context.user_data.get('otp_edit_set')
    
    if set_index is None:
        return
    
    groups_input = update.message.text.strip()
    
    try:
        new_groups = [_parse_id_token(t) for t in groups_input.split() if _is_valid_id_token(t)]
        new_groups = [x for x in new_groups if x is not None]
        if not new_groups:
            await update.message.reply_text(
                "❌ **No valid group IDs!**\n\nUse plain IDs (`-100123456789`) or topic IDs (`-100123456789:36615`).\nNeed IDs? Use /getallid.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data=f"otp_edit_set_{set_index}")]])
            )
            return
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid group IDs!**\n\nUse plain IDs or topic IDs (`-100123456789:36615`).",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data=f"otp_edit_set_{set_index}")]])
        )
        return
    
    updated = await db_call(db.update_otp_set, user_id, set_index, group_ids=new_groups)
    
    if updated:
        fresh_sets = await db_call(db.get_otp_sets_fresh, user_id)
        otp_sets_cache[user_id] = fresh_sets
        asyncio.create_task(update_otp_handlers_for_user(user_id))
        
        groups_display = ', '.join(str(g) for g in new_groups)
        await update.message.reply_text(
            f"✅ **Groups Updated!**\n\nSet #{set_index + 1} now: `{groups_display}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "❌ **Failed to update groups!**",
            parse_mode="Markdown"
        )
    
    context.user_data.pop('otp_action', None)


async def handle_otp_rename_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    set_index = context.user_data.get('otp_edit_set')
    
    if set_index is None:
        return
    
    new_name = update.message.text.strip()
    
    if not new_name:
        await update.message.reply_text(
            "❌ **Name cannot be empty!**",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data=f"otp_edit_set_{set_index}")]])
        )
        return
    
    updated = await db_call(db.update_otp_set, user_id, set_index, name=new_name)
    
    if updated:
        fresh_sets = await db_call(db.get_otp_sets_fresh, user_id)
        otp_sets_cache[user_id] = fresh_sets
        
        await update.message.reply_text(
            f"✅ **Set Renamed!**\n\nSet #{set_index + 1} now: **{new_name}**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"otp_edit_set_{set_index}")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "❌ **Failed to rename set!**",
            parse_mode="Markdown"
        )
    
    context.user_data.pop('otp_action', None)


async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\nUse /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)


async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = """🗂️ **Chat ID Categories**

📋 Choose which type of chat IDs you want to see:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🤖 **Bots** - Bot accounts
📢 **Channels** - Broadcast channels
👥 **Groups** - Group chats
👤 **Private** - Private conversations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 Select a category below:"""

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_load_bots"), InlineKeyboardButton("📢 Channels", callback_data="chatids_load_channels")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_load_groups"), InlineKeyboardButton("👤 Private", callback_data="chatids_load_private")],
    ]

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE, reload: bool = False):
    from telethon.tl.types import User, Channel, Chat

    if user_id not in user_clients:
        return

    if "chatids_cache" not in context.user_data:
        context.user_data["chatids_cache"] = {}

    if reload or category not in context.user_data["chatids_cache"]:
        client = user_clients[user_id]
        if not await _ensure_client_healthy(user_id, client):
            return
        cached = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if category == "bots":
                if isinstance(entity, User) and entity.bot:
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "channels":
                if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "groups":
                if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "private":
                if isinstance(entity, User) and not entity.bot:
                    cached.append({"name": dialog.name, "id": dialog.id})

        context.user_data["chatids_cache"][category] = cached

    categorized_dialogs = context.user_data["chatids_cache"][category]

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}

    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")

    if not categorized_dialogs:
        chat_list = f"{emoji} **{name}**\n\n📭 **No {name.lower()} found!**\n\nTry another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = (dialog["name"] or "Unknown")[:30]
            escaped_chat_name = escape_markdown(chat_name, version=2)
            chat_list += f"{i}. **{escaped_chat_name}**\n   🆔 `{dialog['id']}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

    keyboard = []

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_nav_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_nav_{category}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])

    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    message = update.message if update.message else update.callback_query.message

    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\nToo many users are currently connected. Please try again later.",
            parse_mode="Markdown",
        )
        return

    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in") and user.get("session_data"):
        if user_id in user_clients:
            await message.reply_text(
                "✅ **You are already logged in!**\n\n"
                f"📱 Phone: `{user['phone'] or 'Not set'}`\n"
                f"👤 Name: `{user['name'] or 'User'}`\n\n"
                "Use /logout if you want to disconnect.",
                parse_mode="Markdown",
            )
            return
        else:
            logger.warning(f"User {user_id} has session in DB but not in memory, cleaning up")
            try:
                await db_call(db.purge_user_data, user_id)
            except Exception:
                pass

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    try:
        await client.connect()
    except Exception as e:
        logger.error(f"Telethon connection failed: {e}")
        await message.reply_text(
            f"❌ **Connection failed:** {str(e)}\n\nPlease try again in a few minutes.",
            parse_mode="Markdown",
        )
        return

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Login Process**\n\n1️⃣ **Enter your phone number** (with country code):\n\n**Examples:**\n• `+1234567890`\n• `+447911123456`\n• `+4915112345678`\n\n⚠️ **Important:**\n• Include the `+` sign\n• Use international format\n• No spaces or dashes\n\n**Type your phone number now:**",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
        parse_mode="Markdown",
    )


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\nUse /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        f"⚠️ **Confirm Logout**\n\n📱 **Enter your phone number to confirm disconnection:**\n\nYour connected phone: `{user['phone']}`\n\nType your phone number exactly to confirm logout.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_logout")]]),
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id not in login_states:
        return

    state = login_states[user_id]
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            if not text.startswith('+'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPhone number must start with `+`\nExample: `+1234567890`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return
            
            clean_phone = _clean_phone_number(text)
            
            if len(clean_phone) < 8:
                await update.message.reply_text(
                    "❌ **Invalid phone number!**\n\nPhone number seems too short.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            processing_msg = await update.message.reply_text(
                "⏳ **Sending verification code...**\n\nThis may take a few seconds.",
                parse_mode="Markdown",
            )

            try:
                result = await client.send_code_request(clean_phone)
                
                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"

                await processing_msg.edit_text(
                    f"✅ **Verification code sent!**\n\n📱 **Code sent to:** `{clean_phone}`\n\n2️⃣ **Enter the verification code:**\n\n**Format:** `verify12345`\n• Type `verify` followed by your 5-digit code\n• No spaces, no brackets\n\n**Example:** If your code is `54321`, type:\n`verify54321`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error sending code for user {user_id}: {error_msg}")
                
                if "PHONE_NUMBER_INVALID" in error_msg:
                    error_text = "❌ **Invalid phone number!**"
                elif "PHONE_NUMBER_BANNED" in error_msg:
                    error_text = "❌ **Phone number banned!**"
                elif "FLOOD" in error_msg or "Too many" in error_msg:
                    error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease start over."
                else:
                    error_text = f"❌ **Error:** {error_msg}"
                
                await processing_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )
                
                try:
                    await client.disconnect()
                except:
                    pass
                
                if user_id in login_states:
                    del login_states[user_id]
                return

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `verify12345`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            code = text[6:]
            
            if not code or not code.isdigit() or len(code) != 5:
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\nCode must be 5 digits.\n**Example:** `verify12345`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying code...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()


                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = ClientWrapper(client)
                await start_live_handlers_for_user(user_id)
                
                asyncio.create_task(_preload_user_dialogs(user_id, client, me.first_name or "User"))
                
                _ensure_user_target_cache(user_id)
                

                del login_states[user_id]

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\n**Now you can:**\n• Configure OTP groups with /otpgroups\n• Set the target chat with /setchatid\n• Get chat IDs with /getallid\n• Send numbers to start a batch",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2-Step Verification Required**\n\n3️⃣ **Enter your 2FA password:**\n\n**Format:** `passwordYourPassword123`\n• Type `password` followed by your 2FA password\n• No spaces, no brackets\n\n**Example:** If your password is `mypass123`, type:\n`passwordmypass123`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
                    parse_mode="Markdown",
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying code for user {user_id}: {error_msg}")
                
                if "PHONE_CODE_INVALID" in error_msg:
                    error_text = "❌ **Invalid code!**"
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**"
                else:
                    error_text = f"❌ **Verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `passwordYourPassword123`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA password...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(password=password)

                me = await client.get_me()
                session_string = client.session.save()


                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = ClientWrapper(client)
                await start_live_handlers_for_user(user_id)
                
                asyncio.create_task(_preload_user_dialogs(user_id, client, me.first_name or "User"))
                
                _ensure_user_target_cache(user_id)
                

                del login_states[user_id]

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected with 2FA!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\nYour account is now securely connected! 🔐",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")
                
                if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                    error_text = "❌ **Invalid 2FA password!**"
                else:
                    error_text = f"❌ **2FA verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

    except Exception as e:
        logger.exception("Unexpected error during login")
        await update.message.reply_text(
            f"❌ **Unexpected error:** {str(e)[:100]}\n\nPlease try /login again.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                pass
            del login_states[user_id]


# ------------------------------------------------------------
# Session restore (fordet restore_single_session, DB path)
# ------------------------------------------------------------
async def restore_single_session(user_id: int, session_data: str) -> bool:
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            await db_call(db.save_user, user_id, None, None, None, False)
            await client.disconnect()
            return False

        if len(user_clients) >= MAX_CONCURRENT_USERS:
            try:
                await client.disconnect()
            except Exception:
                pass
            await db_call(db.save_user, user_id, None, None, None, True)
            logger.warning(f"Max users reached, skipping restoration for user {user_id}")
            return False

        user_clients[user_id] = ClientWrapper(client)

        try:
            me = await client.get_me()
            user_name = me.first_name or "User"

            user = await db_call(db.get_user, user_id)

            await db_call(db.save_user, user_id,
                          user["phone"] if user else None,
                          user_name,
                          session_data,
                          True)

            logger.info(f"✅ Restored session for user {user_id} ({user_name}) from database")

            asyncio.create_task(_preload_user_dialogs(user_id, client, user_name))
            await start_live_handlers_for_user(user_id)

            return True

        except Exception as e:
            logger.exception(f"Error in restore_single_session for user {user_id}: {e}")
            return False

    except Exception as e:
        logger.exception(f"Failed to restore session for user {user_id}: {e}")
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            pass
        return False


async def _load_saved_sessions_on_startup():
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, session_data FROM users WHERE is_logged_in = TRUE AND session_data IS NOT NULL"
        )
    for row in rows:
        await restore_single_session(row["user_id"], row["session_data"])


async def _get_user_target_chat(user_id: int) -> Optional[int]:
    if user_id in user_target_chat_ids:
        return user_target_chat_ids[user_id]

    if not db_pool:
        return target_chat_id

    async with db_pool.acquire() as conn:
        chat_id = await conn.fetchval("SELECT chat_id FROM bot_settings WHERE user_id = $1", user_id)
    if chat_id:
        user_target_chat_ids[user_id] = int(chat_id)
        return int(chat_id)

    return target_chat_id


# ------------------------------------------------------------
# Main menu (target of every "🔙 Main Menu" button) and logout confirmation
# ------------------------------------------------------------
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    user = await db_call(db.get_user, user_id)

    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"
    current_time = format_datetime_utc1()

    message_text = f"""╔════════════════════════════════════╗
║      🛡️ FORDET-STYLE GUARDIAN BOT      ║
╚════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}
🕐 **Time:** {current_time}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  /login - Connect your Telegram account
  /logout - Disconnect your account

🤖 **Auto-OTP System:**
  /otpgroups - Configure OTP search groups

🎯 **Target Chat:**
  /setchatid - Set the SUCCESS/FAILED chat

📂 **Number Files:**
  /numberfiles - Saved .txt batches (kept 48h)

🆔 **Utilities:**
  /getallid - Get all your chat IDs

⚙️ **Batch Control:**
  /pause | /resume | /next | /stats | /cancel

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("🤖 OTP Groups", callback_data="otp_main")])
        keyboard.append([InlineKeyboardButton("📂 Number Files", callback_data="nf_page_0")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    if update.callback_query:
        await update.callback_query.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            f"❌ **Phone number doesn't match!**\n\nExpected: `{stored_phone}`\nYou entered: `{text}`",
            parse_mode="Markdown",
        )
        return True

    for key, search in list(otp_search_manager.searches.items()):
        if search.get('user_id') == user_id:
            await otp_search_manager.cancel_search(key)

    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            _clear_otp_handlers(user_id, client)
            _clear_result_handlers(user_id, client)
            await client.disconnect()
        except Exception:
            pass
        finally:
            user_clients.pop(user_id, None)

    try:
        success = await db_call(db.purge_user_data, user_id)
        if not success:
            logger.error(f"Failed to purge user data for {user_id}")
    except Exception as e:
        logger.exception(f"Error purging user data for {user_id}: {e}")

    otp_sets_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    result_live_handler_chat.pop(user_id, None)
    logout_states.pop(user_id, None)
    user_data_store.pop(user_id, None)
    pending_batches.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n✅ All your sessions, tasks, and data have been cleared.\n🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True


# ------------------------------------------------------------
# Commands
# ------------------------------------------------------------
@router.message(Command("start"))
async def start_handler(message: Message):
    help_text = (
        "FORDET-Style Guardian Bot\n\n"
        "Commands:\n"
        "/login - connect your Telegram account (phone -> code -> 2FA)\n"
        "/logout - disconnect and remove your saved session\n"
        "/setchatid <chat_id> - set the target chat that receives numbers and reports SUCCESS/FAILED\n"
        "/otpgroups - manage OTP search sets (Name = pattern = group IDs)\n"
        "/numberfiles - saved .txt number files (kept 48h, resume where you stopped)\n"
        "/getallid - list the chat IDs of your logged-in account\n"
        "/pause | /resume | /next | /stats | /cancel\n\n"
        "How it works:\n"
        "1) /login and complete the interactive login flow\n"
        "2) /otpgroups to add OTP sets, e.g. HK = 2|TGT|3 = -100123456789\n"
        "3) Send numbers as text, or send a .txt file, then pick which OTP group to search (or All Groups). Files are saved while you are logged in, so you can resume them\n"
        "4) If the device requests a code, the bot searches your OTP groups (short backfill + live handlers) "
        "for the number-derived pattern and sends the OTP it finds to the device\n"
        "5) When the device says continue, your logged-in account sends the plain number (no forward tag) to the "
        "target chat and a live handler waits for exact case-sensitive SUCCESS/FAILED messages."
    )
    await message.answer(help_text)


@router.message(Command("login"))
async def login_cmd_handler(message: Message):
    update, context = ptb_from_message(message)
    await login_command(update, context)


@router.message(Command("logout"))
async def logout_cmd_handler(message: Message):
    update, context = ptb_from_message(message)
    await logout_command(update, context)


@router.message(Command("getallid"))
async def getallid_cmd_handler(message: Message):
    update, context = ptb_from_message(message)
    await getallid_command(update, context)


@router.message(Command("otpgroups"))
async def otpgroups_cmd_handler(message: Message):
    update, context = ptb_from_message(message)
    await otpgroups_command(update, context)


@router.message(Command("setchatid"))
async def setchatid_handler(message: Message):
    global target_chat_id
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(f"Current target chat id: `{target_chat_id}`\nUsage: `/setchatid -100xxxxxxxxxx`", parse_mode="Markdown")
        return

    try:
        chat_id = int(args[1])
        user_target_chat_ids[message.from_user.id] = chat_id
        target_chat_id = chat_id
        if db_pool:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO bot_settings (user_id, chat_id)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id)
                    DO UPDATE SET chat_id = EXCLUDED.chat_id
                    """,
                    message.from_user.id,
                    chat_id,
                )
        # the SUCCESS/FAILED live handler follows the target chat
        await update_result_handler_for_user(message.from_user.id)
        await message.answer(f"✅ Target chat ID successfully set to `{chat_id}`", parse_mode="Markdown")
    except ValueError:
        await message.answer("❌ Invalid Chat ID format. Must be an integer.")


# ------------------------------------------------------------
# Inline buttons (fordet button_handler branches for login / logout /
# getallid / otpgroups, copied verbatim)
# ------------------------------------------------------------
@router.callback_query(~F.data.startswith("nf_"), ~F.data.startswith("otpsel_"))
async def button_handler(callback: CallbackQuery):
    update, context = ptb_from_callback(callback)
    query = update.callback_query
    user_id = query.from_user.id

    await query.answer()

    action = query.data

    if action == "login":
        await query.message.delete()
        await login_command(update, context)
    elif action == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif action == "back_to_main":
        await show_main_menu(update, context, user_id)
    elif action == "otp_main":
        context.user_data.pop('otp_page', None)
        context.user_data.pop('otp_action', None)
        context.user_data.pop('otp_edit_set', None)
        if user_id not in otp_sets_cache:
            _fresh = await db_call(db.get_otp_sets, user_id)
            otp_sets_cache[user_id] = _fresh
        await show_otp_main_menu(update, context, user_id)
    elif action == "otp_add":
        await show_otp_add_prompt(update, context)
    elif action == "otp_edit":
        await show_otp_edit_list(update, context)
    elif action == "otp_delete":
        await show_otp_delete_list(update, context)
    elif action == "otp_delete_all":
        await show_otp_delete_all_confirm(update, context)
    elif action == "otp_confirm_delete_all":
        await confirm_otp_delete_all(update, context)
    elif action == "otp_page_prev":
        context.user_data['otp_page'] = context.user_data.get('otp_page', 0) - 1
        await show_otp_main_menu(update, context, user_id)
    elif action == "otp_page_next":
        context.user_data['otp_page'] = context.user_data.get('otp_page', 0) + 1
        await show_otp_main_menu(update, context, user_id)
    elif action == "otp_edit_page_prev":
        context.user_data['otp_edit_page'] = context.user_data.get('otp_edit_page', 0) - 1
        await show_otp_edit_list(update, context)
    elif action == "otp_edit_page_next":
        context.user_data['otp_edit_page'] = context.user_data.get('otp_edit_page', 0) + 1
        await show_otp_edit_list(update, context)
    elif action == "otp_delete_page_prev":
        context.user_data['otp_delete_page'] = context.user_data.get('otp_delete_page', 0) - 1
        await show_otp_delete_list(update, context)
    elif action == "otp_delete_page_next":
        context.user_data['otp_delete_page'] = context.user_data.get('otp_delete_page', 0) + 1
        await show_otp_delete_list(update, context)
    elif action.startswith("otp_edit_set_"):
        set_index = int(action.replace("otp_edit_set_", ""))
        await show_otp_edit_options(update, context, set_index)
    elif action.startswith("otp_edit_format_"):
        set_index = int(action.replace("otp_edit_format_", ""))
        await show_otp_edit_format_prompt(update, context, set_index)
    elif action.startswith("otp_edit_groups_"):
        set_index = int(action.replace("otp_edit_groups_", ""))
        await show_otp_edit_groups_prompt(update, context, set_index)
    elif action.startswith("otp_rename_"):
        set_index = int(action.replace("otp_rename_", ""))
        await show_otp_rename_prompt(update, context, set_index)
    elif action.startswith("otp_delete_set_"):
        set_index = int(action.replace("otp_delete_set_", ""))
        await show_otp_delete_confirm(update, context, set_index)
    elif action.startswith("otp_confirm_delete_"):
        set_index = int(action.replace("otp_confirm_delete_", ""))
        await confirm_otp_delete(update, context, set_index)
    elif action.startswith("chatids_"):
        if action == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        elif action.startswith("chatids_load_"):
            category = action[len("chatids_load_"):]
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, 0, context, reload=True)
        elif action.startswith("chatids_nav_"):
            parts = action.split("_")
            category = parts[2]
            page = int(parts[3])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context, reload=False)
    elif action == "cancel_login":
        if user_id in login_states:
            try:
                client = login_states[user_id].get("client")
                if client:
                    await client.disconnect()
            except:
                pass
            del login_states[user_id]
        await query.edit_message_text(
            "❌ **Login cancelled.** You can start again with /login anytime.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )
    elif action == "cancel_logout":
        if user_id in logout_states:
            del logout_states[user_id]
        await query.edit_message_text(
            "❌ **Logout cancelled.** Your session remains active.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )


# ------------------------------------------------------------
# Text input routing (same precedence as fordet's handle_all_text_messages:
# login -> logout confirmation -> OTP manager input -> numbers)
# ------------------------------------------------------------
_TEXT_ONLY = F.text & ~F.text.startswith("/")
_OTP_INPUT_ACTIONS = ("adding", "editing_format", "editing_groups", "renaming")


def _login_active(message: Message) -> bool:
    return bool(message.from_user) and message.from_user.id in login_states


def _logout_active(message: Message) -> bool:
    return bool(message.from_user) and message.from_user.id in logout_states


def _otp_input_active(message: Message) -> bool:
    return bool(message.from_user) and \
        user_data_store.get(message.from_user.id, {}).get('otp_action') in _OTP_INPUT_ACTIONS


@router.message(_TEXT_ONLY, _login_active)
async def login_text_handler(message: Message):
    update, context = ptb_from_message(message)
    await handle_login_process(update, context)


@router.message(_TEXT_ONLY, _logout_active)
async def logout_text_handler(message: Message):
    update, context = ptb_from_message(message)
    await handle_logout_confirmation(update, context)


@router.message(_TEXT_ONLY, _otp_input_active)
async def otp_text_handler(message: Message):
    update, context = ptb_from_message(message)
    action = context.user_data.get('otp_action')
    if action == 'adding':
        await handle_otp_add_input(update, context)
    elif action == 'editing_format':
        await handle_otp_edit_format_input(update, context)
    elif action == 'editing_groups':
        await handle_otp_edit_groups_input(update, context)
    elif action == 'renaming':
        await handle_otp_rename_input(update, context)


# ------------------------------------------------------------
# Number processing flow
# ------------------------------------------------------------
# ------------------------------------------------------------
# Number files: persistent .txt batches (kept NUMBER_FILE_TTL_HOURS, default 48h)
# ------------------------------------------------------------
def _unique_file_name(name: str, taken: Set[str]) -> str:
    """'a.txt' -> 'a (2).txt' -> 'a (3).txt' ... when the name is already in use."""
    if name not in taken:
        return name
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem, ext = name, ""
    i = 2
    while True:
        candidate = f"{stem} ({i}){'.' + ext if ext else ''}"
        if candidate not in taken:
            return candidate
        i += 1


async def db_add_number_file(user_id: int, name: str, numbers: List[str]) -> Optional[Dict]:
    if not db_pool:
        return None
    expires_at = datetime.now(timezone.utc) + timedelta(hours=NUMBER_FILE_TTL_HOURS)
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT name FROM number_files WHERE user_id = $1 AND expires_at > NOW()", user_id
                )
                final_name = _unique_file_name(name, {r["name"] for r in rows})
                row = await conn.fetchrow(
                    """
                    INSERT INTO number_files (user_id, name, numbers, total, next_index, expires_at)
                    VALUES ($1, $2, $3::jsonb, $4, 0, $5)
                    RETURNING id, name, total, next_index, created_at, expires_at
                    """,
                    user_id, final_name, json.dumps(numbers), len(numbers), expires_at,
                )
        return dict(row)
    except Exception as e:
        logger.exception(f"Error adding number file for user {user_id}: {e}")
        return None


async def db_list_number_files(user_id: int) -> List[Dict]:
    if not db_pool:
        return []
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, total, next_index, created_at, expires_at
                FROM number_files
                WHERE user_id = $1 AND expires_at > NOW()
                ORDER BY created_at DESC, id DESC
                """,
                user_id,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.exception(f"Error listing number files for user {user_id}: {e}")
        return []


async def db_get_number_file(user_id: int, file_id: int, with_numbers: bool = False) -> Optional[Dict]:
    if not db_pool:
        return None
    cols = "id, name, total, next_index, created_at, expires_at"
    if with_numbers:
        cols += ", numbers::text AS numbers"
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT {cols} FROM number_files WHERE id = $1 AND user_id = $2 AND expires_at > NOW()",
                file_id, user_id,
            )
        if not row:
            return None
        rec = dict(row)
        if with_numbers:
            rec["numbers"] = json.loads(rec["numbers"])
        return rec
    except Exception as e:
        logger.exception(f"Error getting number file {file_id}: {e}")
        return None


async def db_set_file_progress(file_id: int, next_index: int) -> None:
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE number_files SET next_index = $2 WHERE id = $1", file_id, next_index)
    except Exception as e:
        logger.warning(f"Could not save progress for file {file_id}: {e}")


async def db_delete_number_file(user_id: int, file_id: int) -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM number_files WHERE id = $1 AND user_id = $2", file_id, user_id
            )
        return int(status.split()[-1]) > 0
    except Exception as e:
        logger.exception(f"Error deleting number file {file_id}: {e}")
        return False


async def db_purge_expired_files() -> int:
    if not db_pool:
        return 0
    async with db_pool.acquire() as conn:
        status = await conn.execute("DELETE FROM number_files WHERE expires_at <= NOW()")
    return int(status.split()[-1])


async def _expired_files_cleanup_loop():
    """Expired files are already invisible (every query filters on expires_at); this frees the storage."""
    while True:
        try:
            purged = await db_purge_expired_files()
            if purged:
                logger.info(f"Purged {purged} expired number file(s)")
        except Exception as e:
            logger.warning(f"Expired-file cleanup failed: {e}")
        await asyncio.sleep(600)


# ------------------------------------------------------------
# Batch runner (shared by pasted text, sent files and /numberfiles → Resume)
# ------------------------------------------------------------
def parse_numbers(text: str) -> List[str]:
    return [n.strip() for n in re.split(r"[,\s]+", text) if n.strip()]


def decode_file_bytes(raw: bytes) -> str:
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _claim_batch() -> bool:
    """Synchronous check-and-set so two requests can never both start a batch."""
    global is_processing
    if is_processing:
        return False
    is_processing = True
    return True


async def _batch_preconditions(user_id: int) -> Optional[Tuple[str, Optional[str]]]:
    if user_id not in user_clients:
        return ("❌ **You need to connect your account first!**\n\nUse /login to connect.", "Markdown")
    if user_id not in otp_sets_cache:
        otp_sets_cache[user_id] = await db_get_otp_sets(user_id)
    if not otp_sets_cache.get(user_id):
        return ("❌ No OTP sets found! Please configure one using /otpgroups.", None)
    return None


def _scope_label(otp_set_name: Optional[str]) -> str:
    return otp_set_name if otp_set_name else "All Groups"


def _search_interrupt_from_status() -> Optional[str]:
    """blocked / try_again from the device end the OTP search immediately."""
    status = (device_status or "").lower()
    if "blocked" in status:
        return "blocked"
    if "try_again" in status:
        return "try_again"
    return None


async def process_number(message: Message, user_id: int, target_chat: Optional[int], num: str,
                         otp_set_name: Optional[str] = None) -> None:
    """Runs one number through device check -> OTP search -> send code -> no-tag forward -> SUCCESS/FAILED."""
    global device_status
    num_msg = await message.answer(f"{num} ⏳ Checking on device...")

    device_event.clear()
    device_status = None

    success = await call_device_api(num, payload_type="check")
    if not success:
        await num_msg.edit_text(f"{num} 🔴 Error (Webhook failed)")
        await log_number_status(num, "webhook_error")
        return

    try:
        await asyncio.wait_for(device_event.wait(), timeout=300)
    except asyncio.TimeoutError:
        if skip_current_number:
            await num_msg.edit_text(f"{num} ⏭️ Skipped")
            await log_number_status(num, "skipped")
            return
        await num_msg.edit_text(f"{num} 🟠 Ended (Device timeout - 5 mins)")
        await log_number_status(num, "device_timeout")
        return

    if skip_current_number:
        await num_msg.edit_text(f"{num} ⏭️ Skipped")
        await log_number_status(num, "skipped")
        return

    result_text = (device_status or "").lower()

    if "blocked" in result_text:
        await num_msg.edit_text(f"{num} ⚫ Bad")
        await log_number_status(num, "blocked")
        return

    if "try_again" in result_text:
        await num_msg.edit_text(f"{num} 🟡 Try Later")
        await log_number_status(num, "try_again")
        await asyncio.sleep(5)
        return

    if "error" in result_text and "request_code" not in result_text:
        await num_msg.edit_text(f"{num} 🔴 Error Code")
        await log_number_status(num, "error")
        return

    if "request_code" in result_text or "requestcode" in result_text or "request-code" in result_text:
        scope_label = _scope_label(otp_set_name)
        await num_msg.edit_text(f"{num} ⚪ Device requested code. Searching OTP groups ({scope_label})...")

        # fordet auto-OTP search: backfill + live handlers, resolves when the OTP shows up
        search_key = f"{user_id}:{num}"
        fut = await otp_search_manager.start_search(user_id, search_key, num, task_label="batch", set_name=otp_set_name)
        code = None
        set_name = None
        interrupt = None

        if fut is None:
            await num_msg.edit_text(
                f"{num} ⚠️ No usable OTP search pattern for this number ({scope_label}). Check your formats in /otpgroups."
            )
        else:
            backfill_done = (otp_search_manager.searches.get(search_key) or {}).get('backfill_done')
            deadline = time.time() + OTP_SEARCH_DURATION + 5
            no_code_sent = False
            device_event.clear()
            device_status = None
            try:
                # keeps searching until: OTP found, device says blocked/try_again, /next, /cancel or timeout.
                # Any other device callback only re-arms the wait.
                while True:
                    interrupt = _search_interrupt_from_status()
                    if interrupt or fut.done() or skip_current_number or time.time() >= deadline:
                        break

                    # history scan came back empty: tell the device once ("no_code"), but keep searching
                    if backfill_done is not None and backfill_done.is_set() and not no_code_sent:
                        no_code_sent = True
                        await num_msg.edit_text(f"{num} ⚪ No code yet. Still searching ({scope_label})...")
                        await call_device_api(num, payload_type="no_code")
                        continue

                    device_event.clear()
                    helpers = [asyncio.ensure_future(device_event.wait())]
                    if backfill_done is not None and not no_code_sent:
                        helpers.append(asyncio.ensure_future(backfill_done.wait()))
                    try:
                        await asyncio.wait(
                            {fut, *helpers},
                            timeout=max(0.0, deadline - time.time()),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for helper in helpers:
                            helper.cancel()
            finally:
                await otp_search_manager.cancel_search(search_key)

            if skip_current_number:
                await num_msg.edit_text(f"{num} ⏭️ Skipped")
                await log_number_status(num, "skipped")
                return

            if interrupt == "blocked":
                await num_msg.edit_text(f"{num} ⚫ Bad")
                await log_number_status(num, "blocked")
                return

            if interrupt == "try_again":
                await num_msg.edit_text(f"{num} 🟡 Try Later")
                await log_number_status(num, "try_again")
                await asyncio.sleep(5)
                return

            if fut.done() and not fut.cancelled():
                found = fut.result()
                if found:
                    code, set_name = found

            if code:
                await num_msg.edit_text(
                    f"{num} ⚪ OTP found (set {html.escape(set_name)}): <code>{code}</code>", parse_mode="HTML"
                )
            else:
                await num_msg.edit_text(
                    f"{num} ⚠️ Could not find OTP in your OTP groups. Please ensure your logged-in account is a member and can read them."
                )

        if not code:
            await num_msg.edit_text(f"{num} 🔴 No OTP available. Skipping.")
            await log_number_status(num, "no_otp_found")
            return

        device_event.clear()
        device_status = None
        await call_device_api(num, payload_type="send_code", code=code)

        try:
            await asyncio.wait_for(device_event.wait(), timeout=300)
            if skip_current_number:
                await num_msg.edit_text(f"{num} ⏭️ Skipped")
                await log_number_status(num, "skipped")
                return
        except asyncio.TimeoutError:
            if skip_current_number:
                await num_msg.edit_text(f"{num} ⏭️ Skipped")
                await log_number_status(num, "skipped")
                return
            await num_msg.edit_text(f"{num} 🟠 Ended (Device timeout after sending code - 5 mins)")
            await log_number_status(num, "device_timeout_after_code")
            return

    result_after = (device_status or "").lower()
    if "continue" not in result_after and "continued" not in result_after and "proceed" not in result_after:
        await num_msg.edit_text(f"{num} ⚠️ Device did not signal to continue. Status: `{device_status}`", parse_mode="Markdown")
        await log_number_status(num, "no_continue")
        return

    await num_msg.edit_text(f"{num} 🟢 Continued. Forwarding internally and awaiting SUCCESS/FAILED...")

    if target_chat:
        key = (user_id, num)
        ev = asyncio.Event()
        # register the waiter BEFORE sending so a fast SUCCESS/FAILED can't be missed
        live_success_failure_events[key] = ev
        live_success_failure_results.pop(key, None)
        try:
            ok, reason = await forward_message_direct(user_id, target_chat, num)
            if not ok:
                logger.error("Failed to forward to target chat %s (%s)", target_chat, reason)
                await num_msg.edit_text(f"{num} ❌ Error forwarding to target chat ID.")
                return

            try:
                await asyncio.wait_for(ev.wait(), timeout=300)
                decision = live_success_failure_results.get(key, "UNKNOWN")
                await num_msg.edit_text(f"{num} 🏁 Result Decided: {decision}")
                await log_number_status(num, decision.lower())
            except asyncio.TimeoutError:
                await num_msg.edit_text(f"{num} 🟠 Live decision timeout (No SUCCESS/FAILED received)")
                await log_number_status(num, "decision_timeout")
        finally:
            live_success_failure_events.pop(key, None)
            live_success_failure_results.pop(key, None)
    else:
        await num_msg.edit_text(f"{num} ⚠️ Target chat ID not set! Use /setchatid.")


async def run_batch(message: Message, user_id: int, numbers: List[str],
                    file_id: Optional[int] = None, start_index: int = 0,
                    file_name: Optional[str] = None, otp_set_name: Optional[str] = None) -> None:
    """
    Caller must have won _claim_batch(). Always releases the batch state when done.
    For file batches the position is saved after every finished number, so a /cancel,
    crash or restart resumes at the number that was in flight (it is redone, never skipped).
    """
    global is_processing, is_paused, current_task, skip_current_number, current_index, \
        total_numbers_count, active_number, active_file_id

    total = len(numbers)
    completed = start_index
    try:
        problem = await _batch_preconditions(user_id)
        if problem:
            text, parse_mode = problem
            if file_id is not None:
                text += "\n\n📂 Your file is saved — resume it from /numberfiles once this is fixed."
            await message.answer(text, parse_mode=parse_mode)
            return

        target_chat = await _get_user_target_chat(user_id)
        if target_chat is None:
            target_chat = target_chat_id

        # make sure the live SUCCESS/FAILED handler is watching the current target chat
        if target_chat and result_live_handler_chat.get(user_id) != target_chat:
            await update_result_handler_for_user(user_id)

        device_event.clear()
        total_numbers_count = total
        current_task = asyncio.current_task()
        active_file_id = file_id

        if file_id is not None and start_index == 0:
            await db_set_file_progress(file_id, 0)

        if file_id is None:
            header = f"Processing {total} numbers..."
        else:
            header = f"Processing file {file_name}: {total} numbers"
            if start_index:
                header += f"\nResuming from number {start_index + 1}"
        header += f"\nOTP group: {_scope_label(otp_set_name)}"
        status_msg = await message.answer(header)

        for idx in range(start_index, total):
            num = numbers[idx]
            current_index = idx + 1
            active_number = num
            skip_current_number = False

            await pause_event.wait()
            await status_msg.edit_text(f"Processing batch\n\nProgress: {idx + 1}/{total}\nTarget: {num}")

            await process_number(message, user_id, target_chat, num, otp_set_name)

            completed = idx + 1
            if file_id is not None:
                await db_set_file_progress(file_id, completed)

        done_text = f"Batch completed.\nProcessed: {total}/{total}"
        if file_name:
            done_text += f"\n📂 File: {file_name}"
        await status_msg.edit_text(done_text)

    except asyncio.CancelledError:
        logger.info("Task cancelled by user request.")
        if file_id is not None:
            try:
                await message.answer(f"⏹️ Stopped at {completed}/{total}. Open /numberfiles to resume {file_name}.")
            except Exception:
                pass
    except Exception:
        logger.exception("Error during processing")
        try:
            await message.answer("An error occurred during processing.")
        except Exception:
            pass
    finally:
        is_processing = False
        is_paused = False
        current_task = None
        active_number = None
        active_file_id = None
        skip_current_number = False
        pause_event.set()


# ------------------------------------------------------------
# OTP group picker: choose which OTP set to search before a batch starts or resumes
# ("All Groups" searches every set, a specific set searches only that one)
# ------------------------------------------------------------
async def offer_batch(message: Message, user_id: int, numbers: List[str],
                      file_id: Optional[int] = None, start_index: int = 0,
                      file_name: Optional[str] = None, header: Optional[str] = None) -> None:
    """Checks the batch can run, remembers it, and shows the OTP group picker."""
    problem = await _batch_preconditions(user_id)
    if problem:
        text, parse_mode = problem
        if file_id is not None:
            text += "\n\n📂 Your file is saved — resume it from /numberfiles once this is fixed."
        await message.answer(text, parse_mode=parse_mode)
        return

    pending_batches[user_id] = dict(numbers=numbers, file_id=file_id, start_index=start_index,
                                    file_name=file_name, ts=time.time())
    text, markup = await build_otp_picker_view(user_id, 0, header)
    await message.answer(text, reply_markup=markup, parse_mode="HTML")


def _pending_summary(p: Dict) -> str:
    count = f"{len(p['numbers']):,} numbers"
    if p['file_id'] is None:
        return f"📝 Pasted batch: {count}"
    where = f"resuming from number {p['start_index'] + 1}" if p['start_index'] else "from the beginning"
    return f"📄 <b>{html.escape(p['file_name'])}</b>: {count}, {where}"


async def build_otp_picker_view(user_id: int, page: int, header: Optional[str] = None):
    p = pending_batches.get(user_id)
    sets = sorted(await db_get_otp_sets(user_id), key=lambda s: s['set_index'])
    pages = max(1, (len(sets) + OTP_PICK_PER_PAGE - 1) // OTP_PICK_PER_PAGE)
    page = max(0, min(page, pages - 1))

    lines = []
    if header:
        lines += [header, ""]
    lines += ["🤖 <b>Select OTP Group</b>", ""]
    if p:
        lines += [_pending_summary(p), ""]
    lines += ["Choose which OTP set to search for codes, or <b>All Groups</b> to search every set.", ""]

    keyboard = []
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"otpsel_page_{page - 1}"))
        nav.append(InlineKeyboardButton(f"📄 {page + 1}/{pages}", callback_data="otpsel_noop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"otpsel_page_{page + 1}"))
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🌐 All Groups", callback_data="otpsel_all")])

    row = []
    for s in sets[page * OTP_PICK_PER_PAGE:(page + 1) * OTP_PICK_PER_PAGE]:
        number = s['set_index'] + 1
        lines.append(
            f"{number}. <b>{html.escape(s['name'])}</b>\n"
            f"    🧩 {len(s.get('formats', []))} format(s) • 👥 {len(s.get('group_ids', []))} group(s)"
        )
        row.append(InlineKeyboardButton(_btn_label(number, s['name']), callback_data=f"otpsel_{s['set_index']}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="otpsel_cancel")])
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)


@router.callback_query(F.data.startswith("otpsel_"))
async def otpsel_callback_handler(callback: CallbackQuery):
    update, context = ptb_from_callback(callback)
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data or ""

    if action == "otpsel_noop":
        await query.answer()
        return

    pending = pending_batches.get(user_id)
    if not pending or time.time() - pending['ts'] > PENDING_BATCH_TTL:
        pending_batches.pop(user_id, None)
        await query.answer("This selection expired. Send your numbers or open /numberfiles again.", show_alert=True)
        try:
            await query.edit_message_text("⌛ This selection expired.")
        except Exception:
            pass
        return

    if action.startswith("otpsel_page_"):
        await query.answer()
        try:
            page = int(action[len("otpsel_page_"):])
        except ValueError:
            page = 0
        text, markup = await build_otp_picker_view(user_id, page)
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
        return

    if action == "otpsel_cancel":
        pending_batches.pop(user_id, None)
        await query.answer()
        await query.edit_message_text("❎ Cancelled. Nothing was started.")
        return

    # otpsel_all  or  otpsel_<set_index>
    otp_set_name = None
    if action != "otpsel_all":
        try:
            set_index = int(action[len("otpsel_"):])
        except ValueError:
            await query.answer()
            return
        match = next((s for s in await db_get_otp_sets(user_id) if s['set_index'] == set_index), None)
        if not match:
            await query.answer("That OTP set no longer exists. Pick again.", show_alert=True)
            text, markup = await build_otp_picker_view(user_id, 0)
            await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
            return
        otp_set_name = match['name']

    if not _claim_batch():
        await query.answer("⚠️ Another batch is running. Use /cancel to stop it first.", show_alert=True)
        return

    pending_batches.pop(user_id, None)
    task = asyncio.create_task(run_batch(
        callback.message, user_id, pending['numbers'],
        file_id=pending['file_id'], start_index=pending['start_index'],
        file_name=pending['file_name'], otp_set_name=otp_set_name,
    ))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    await query.answer()
    try:
        await query.edit_message_text(f"✅ OTP group: <b>{html.escape(_scope_label(otp_set_name))}</b>. Starting…",
                                      parse_mode="HTML")
    except Exception:
        pass


@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    user_id = message.from_user.id
    user_data_store.get(user_id, {}).pop('nf_action', None)

    if is_processing:
        await message.answer("Another processing session is running. Use /cancel to stop it first.")
        return

    numbers = parse_numbers(message.text)
    if not numbers:
        await message.answer("No valid numbers found in message.")
        return

    await offer_batch(message, user_id, numbers)


# ------------------------------------------------------------
# Files sent to the bot
# ------------------------------------------------------------
@router.message(F.document)
async def handle_document(message: Message):
    user_id = message.from_user.id
    doc = message.document
    file_name = doc.file_name or "numbers.txt"

    if user_id not in user_clients:
        await message.answer(
            "❌ **You need to connect your account first!**\n\nUse /login to connect. Files can only be saved while you're logged in.",
            parse_mode="Markdown",
        )
        return

    if not (file_name.lower().endswith(".txt") or (doc.mime_type or "").startswith("text/plain")):
        await message.answer("❌ Please send a plain .txt file (numbers separated by commas, spaces or new lines).")
        return

    if doc.file_size and doc.file_size > MAX_FILE_BYTES:
        await message.answer(f"❌ That file is too large (limit {MAX_FILE_BYTES // (1024 * 1024)} MB).")
        return

    try:
        buf = await get_bot().download(doc)
        text = decode_file_bytes(buf.getvalue())
    except Exception as e:
        logger.warning(f"Could not download file for user {user_id}: {e}")
        await message.answer("❌ Couldn't download that file. Please try again.")
        return

    numbers = parse_numbers(text)
    if not numbers:
        await message.answer("❌ No numbers found in that file.")
        return

    saved = await db_add_number_file(user_id, file_name, numbers)
    if not saved:
        await message.answer("❌ Couldn't save that file. Please try again.")
        return

    add_only = user_data_store.get(user_id, {}).pop('nf_action', None) == 'adding_file'
    kept = (f"✅ <b>{html.escape(saved['name'])}</b> saved: {saved['total']:,} numbers, "
            f"kept for {NUMBER_FILE_TTL_HOURS} hours.")

    if add_only:
        await message.answer(
            kept + "\n\nOpen it from /numberfiles whenever you want to start.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📂 Number Files", callback_data="nf_page_0")]]),
        )
        return

    if is_processing:
        await message.answer(
            kept + "\n\nAnother batch is running, so it was not started. Resume it from /numberfiles when it finishes.",
            parse_mode="HTML",
        )
        return

    await offer_batch(message, user_id, numbers, file_id=saved['id'], start_index=0, file_name=saved['name'],
                      header=kept + "\nIt starts from the beginning once you pick an OTP group.")


# ------------------------------------------------------------
# /numberfiles UI
# ------------------------------------------------------------
def _fmt_time_left(expires_at) -> str:
    secs = int((expires_at - datetime.now(timezone.utc)).total_seconds())
    if secs <= 0:
        return "expired"
    hours, minutes = divmod(secs // 60, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m" if minutes else "<1m"


def _file_progress_text(rec: Dict) -> str:
    total, done = rec["total"], rec["next_index"]
    if is_processing and active_file_id == rec["id"]:
        return "🔄 running"
    if done <= 0:
        return "🆕 not started"
    if done >= total:
        return "✅ completed"
    return f"▶️ {done:,}/{total:,} ({done * 100 // total}%)"


def _btn_label(number: int, name: str) -> str:
    label = f"{number}. {name}"
    return label if len(label) <= 26 else label[:25] + "…"


async def build_numberfiles_view(user_id: int, page: int, notice: Optional[str] = None):
    files = await db_list_number_files(user_id)
    pages = max(1, (len(files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
    page = max(0, min(page, pages - 1))
    user_data_store.setdefault(user_id, {})['nf_page'] = page

    keyboard = [[InlineKeyboardButton("➕ Add File", callback_data="nf_add")]]
    lines = []
    if notice:
        lines += [notice, ""]

    if not files:
        lines += [
            "📂 <b>Number Files</b>", "",
            "No files in memory yet.", "",
            "Tap ➕ Add File, or just send a .txt file to start processing it right away.",
            f"Files are kept for {NUMBER_FILE_TTL_HOURS} hours.",
        ]
        return "\n".join(lines), InlineKeyboardMarkup(keyboard)

    lines += [f"📂 <b>Number Files</b> ({len(files)} total), page {page + 1}/{pages}", ""]
    row = []
    for offset, rec in enumerate(files[page * FILES_PER_PAGE:(page + 1) * FILES_PER_PAGE]):
        number = page * FILES_PER_PAGE + offset + 1
        lines.append(
            f"{number}. <b>{html.escape(rec['name'])}</b>\n"
            f"    🔢 {rec['total']:,} • {_file_progress_text(rec)} • ⏳ {_fmt_time_left(rec['expires_at'])}"
        )
        row.append(InlineKeyboardButton(_btn_label(number, rec['name']), callback_data=f"nf_open_{rec['id']}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"nf_page_{page - 1}"))
        nav.append(InlineKeyboardButton(f"📄 {page + 1}/{pages}", callback_data="nf_noop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"nf_page_{page + 1}"))
        keyboard.append(nav)

    lines += ["", "Tap a file to resume or delete it."]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)


async def build_file_view(user_id: int, file_id: int):
    rec = await db_get_number_file(user_id, file_id)
    if not rec:
        return None
    total, done = rec["total"], rec["next_index"]
    if is_processing and active_file_id == file_id:
        resume_hint = "🔄 This file is being processed right now."
    elif 0 < done < total:
        resume_hint = f"Resume continues from number {done + 1}."
    elif done >= total:
        resume_hint = "Finished. Resume starts it over from the beginning."
    else:
        resume_hint = "Resume starts from the beginning."

    text = (
        f"📄 <b>{html.escape(rec['name'])}</b>\n\n"
        f"🔢 Numbers: {total:,}\n"
        f"📊 Progress: {_file_progress_text(rec)}\n"
        f"⏳ Expires in: {_fmt_time_left(rec['expires_at'])}\n\n"
        f"{resume_hint}"
    )
    page = user_data_store.get(user_id, {}).get('nf_page', 0)
    keyboard = [
        [InlineKeyboardButton("▶️ Resume", callback_data=f"nf_resume_{file_id}"),
         InlineKeyboardButton("🗑 Delete", callback_data=f"nf_del_{file_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"nf_page_{page}")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


@router.message(Command("numberfiles"))
async def numberfiles_command(message: Message):
    user_data_store.get(message.from_user.id, {}).pop('nf_action', None)
    text, markup = await build_numberfiles_view(message.from_user.id, 0)
    await message.answer(text, reply_markup=markup, parse_mode="HTML")


@router.callback_query(F.data.startswith("nf_"))
async def nf_callback_handler(callback: CallbackQuery):
    update, context = ptb_from_callback(callback)
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data or ""

    async def show_list(page: int, notice: Optional[str] = None):
        text, markup = await build_numberfiles_view(user_id, page, notice)
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")

    def file_id_from(prefix: str) -> Optional[int]:
        try:
            return int(action[len(prefix):])
        except ValueError:
            return None

    if action == "nf_noop":
        await query.answer()

    elif action.startswith("nf_page_"):
        await query.answer()
        try:
            page = int(action[len("nf_page_"):])
        except ValueError:
            page = 0
        context.user_data.pop('nf_action', None)
        await show_list(page)

    elif action == "nf_add":
        if user_id not in user_clients:
            await query.answer("❌ Log in first with /login. Files can only be saved while you're logged in.", show_alert=True)
            return
        await query.answer()
        context.user_data['nf_action'] = 'adding_file'
        await query.edit_message_text(
            "➕ <b>Add File</b>\n\n"
            "Send me a .txt file with your numbers (separated by commas, spaces or new lines).\n\n"
            f"It will be saved for {NUMBER_FILE_TTL_HOURS} hours. Nothing starts until you resume it.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="nf_addcancel")]]),
            parse_mode="HTML",
        )

    elif action == "nf_addcancel":
        await query.answer()
        context.user_data.pop('nf_action', None)
        await show_list(context.user_data.get('nf_page', 0))

    elif action.startswith("nf_open_"):
        await query.answer()
        fid = file_id_from("nf_open_")
        view = await build_file_view(user_id, fid) if fid is not None else None
        if not view:
            await show_list(context.user_data.get('nf_page', 0), "⚠️ That file no longer exists (deleted or expired).")
        else:
            text, markup = view
            await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")

    elif action.startswith("nf_resume_"):
        fid = file_id_from("nf_resume_")
        rec = await db_get_number_file(user_id, fid, with_numbers=True) if fid is not None else None
        if not rec:
            await query.answer()
            await show_list(context.user_data.get('nf_page', 0), "⚠️ That file no longer exists (deleted or expired).")
            return

        if is_processing:
            await query.answer("⚠️ Another batch is running. Use /cancel to stop it first.", show_alert=True)
            return

        problem = await _batch_preconditions(user_id)
        if problem:
            await query.answer(problem[0].replace("**", "").replace("\n\n", " ")[:190], show_alert=True)
            return

        start = rec["next_index"] if 0 < rec["next_index"] < rec["total"] else 0
        pending_batches[user_id] = dict(numbers=rec["numbers"], file_id=rec["id"], start_index=start,
                                        file_name=rec["name"], ts=time.time())
        await query.answer()
        text, markup = await build_otp_picker_view(user_id, 0)
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")

    elif action.startswith("nf_delyes_"):
        fid = file_id_from("nf_delyes_")
        if fid is not None and is_processing and active_file_id == fid:
            await query.answer("⚠️ This file is being processed. Use /cancel first.", show_alert=True)
            return
        await query.answer()
        rec = await db_get_number_file(user_id, fid) if fid is not None else None
        deleted = rec is not None and await db_delete_number_file(user_id, fid)
        notice = f"🗑 Deleted <b>{html.escape(rec['name'])}</b>." if deleted else "⚠️ That file no longer exists."
        await show_list(context.user_data.get('nf_page', 0), notice)

    elif action.startswith("nf_del_"):
        await query.answer()
        fid = file_id_from("nf_del_")
        rec = await db_get_number_file(user_id, fid) if fid is not None else None
        if not rec:
            await show_list(context.user_data.get('nf_page', 0), "⚠️ That file no longer exists (deleted or expired).")
            return
        await query.edit_message_text(
            f"🗑 Delete <b>{html.escape(rec['name'])}</b>?\n\n"
            "This removes the file and its saved progress. It can't be undone.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, delete", callback_data=f"nf_delyes_{fid}"),
                InlineKeyboardButton("❌ No", callback_data=f"nf_open_{fid}"),
            ]]),
            parse_mode="HTML",
        )

    else:
        await query.answer()


# ------------------------------------------------------------
# pause/resume/cancel/stats
# ------------------------------------------------------------
@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global skip_current_number
    if is_processing and current_task:
        task = current_task
        skip_current_number = True
        device_event.set()
        pause_event.set()
        task.cancel()
        # wait for the batch to finish unwinding (it releases is_processing itself) so a
        # Resume tapped right after /cancel can't overlap with the old run
        await asyncio.wait({task}, timeout=10)
        await message.answer("Batch cancelled successfully.")
    else:
        await message.answer("No active processing session to cancel.")


@router.message(Command("pause"))
async def pause_handler(message: Message):
    global is_paused
    if is_processing and not is_paused:
        is_paused = True
        pause_event.clear()
        await message.answer("Processing paused.")
    else:
        await message.answer("No active processing session to pause.")


@router.message(Command("resume"))
async def resume_handler(message: Message):
    global is_paused
    if is_processing and is_paused:
        is_paused = False
        pause_event.set()
        await message.answer("Processing resumed.")
    else:
        await message.answer("Processing is not currently paused.")


@router.message(Command("next"))
async def next_handler(message: Message):
    global skip_current_number
    if is_processing:
        skip_current_number = True
        device_event.set()
        await message.answer("Skipping current number...")
    else:
        await message.answer("No active processing session.")


@router.message(Command("stats"))
async def stats_handler(message: Message):
    if is_processing:
        await message.answer(f"Status:\nProcessing number {current_index} of {total_numbers_count}\nActive target: {active_number}")
    else:
        await message.answer("No active session running.")


async def handle_health_check(request):
    return web.Response(text="Bot is running live!")


async def handle_device_callback(request):
    global device_status, device_event
    status = request.query.get("status", "unknown")
    logger.info("Received device callback -> %s", status)

    device_status = status
    device_event.set()
    return web.Response(text="Callback received successfully")


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/healthz", handle_health_check)
    app.router.add_get("/device_callback", handle_device_callback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Web Server running on port %s (health + device callbacks)", PORT)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
async def main():
    global bot_instance
    await init_db()

    bot = Bot(token=BOT_TOKEN)
    bot_instance = bot
    await _load_saved_sessions_on_startup()
    cleanup_task = asyncio.create_task(_expired_files_cleanup_loop())

    dp = Dispatcher()
    dp.include_router(router)

    await start_web_server()
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully.")
    try:
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        if db_pool:
            await db_pool.close()
        for client in list(user_clients.values()):
            try:
                await client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())

