import asyncio
import logging
import os
import re
import json
from typing import Optional, List, Dict, Any, Tuple

import aiohttp
import asyncpg
import phonenumbers
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from telethon import TelegramClient, errors
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

router = Router()

# ------------------------------------------------------------
# DB / state
# ------------------------------------------------------------
db_pool: Optional[asyncpg.Pool] = None

target_chat_id: Optional[int] = None
user_target_chat_ids: dict[int, int] = {}

is_processing = False
is_paused = False
current_task: Optional[asyncio.Task] = None

current_index = 0
total_numbers_count = 0
active_number: Optional[str] = None

device_event = asyncio.Event()
device_status: Optional[str] = None

skip_current_number = False
pause_event = asyncio.Event()
pause_event.set()

selected_otp_group: dict[int, dict] = {}

live_success_failure_events: dict[str, asyncio.Event] = {}
live_success_failure_results: dict[str, str] = {}

user_clients: dict[int, TelegramClient] = {}
login_states: dict[int, dict] = {}

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

                CREATE TABLE IF NOT EXISTS otp_groups_bot (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    group_ids TEXT NOT NULL,
                    formats TEXT DEFAULT '[]',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    user_id BIGINT PRIMARY KEY,
                    chat_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id BIGINT PRIMARY KEY,
                    session_str TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            # Prevent duplicate names per user
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_otp_groups_bot_user_name
                ON otp_groups_bot (user_id, name)
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


# ------------------------------------------------------------
# Phone helpers
# ------------------------------------------------------------
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
# Telethon login/logout and session persistence
# ------------------------------------------------------------
async def _save_session_to_db(user_id: int, session_str: str):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_sessions (user_id, session_str)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET session_str = EXCLUDED.session_str
            """,
            user_id,
            session_str,
        )


async def _remove_saved_session_from_db(user_id: int):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)


async def _load_saved_sessions_on_startup():
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, session_str FROM user_sessions")

    for row in rows:
        uid = row["user_id"]
        session_str = row["session_str"]
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                user_clients[uid] = client
            else:
                await client.disconnect()
        except Exception:
            logger.exception("Could not restore Telethon session for user %s", uid)


async def _disconnect_user_client(user_id: int):
    client = user_clients.pop(user_id, None)
    if client:
        try:
            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client for user %s", user_id)


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
# OTP parsing and search logic (FORDET-style)
# ------------------------------------------------------------
class OTPFormatParser:
    @staticmethod
    def extract_number(text: str) -> str:
        match = re.search(r"\b\d{4,8}\b", text)
        if match:
            return match.group(0)
        return ""

    @staticmethod
    def parse_format(format_str: str) -> Dict[str, Any]:
        format_str = (format_str or "").strip()
        if not format_str:
            return {"type": "plain", "value": ""}
        tokens = []
        for part in re.split(r"(\d+|[A-Za-z]+|[^A-Za-z0-9]+)", format_str):
            if part:
                tokens.append(part)
        return {"type": "format", "tokens": tokens, "value": format_str}

    @staticmethod
    def apply_format(number: str, format_dict: Dict[str, Any]) -> str:
        if not format_dict:
            return number
        if format_dict.get("type") == "plain":
            return number
        tokens = format_dict.get("tokens", [])
        if not tokens:
            return number
        idx = 0
        out = []
        for part in tokens:
            if part.isdigit():
                if idx < len(number):
                    out.append(number[idx:idx + len(part)])
                    idx += len(part)
                else:
                    out.append(part)
            else:
                out.append(part)
        return "".join(out)

    @staticmethod
    def extract_otp(text: str) -> Optional[str]:
        text = (text or "").strip()
        if not text:
            return None
        match = re.search(r"\b\d{4,8}\b", text)
        if match:
            return match.group(0)
        return None

    @staticmethod
    def message_contains_format(message_text: str, format_str: str) -> bool:
        if not message_text or not format_str:
            return False
        return format_str in message_text or re.search(r"\d{4,8}", message_text) is not None


class AutoOTPSearchManager:
    def __init__(self):
        self.active_searches: Dict[str, Dict[str, Any]] = {}

    async def start_search(self, user_id: int, duplicate_key: str, group_ids: List[str], client: TelegramClient):
        if not group_ids:
            return None
        self.active_searches[duplicate_key] = {
            "user_id": user_id,
            "group_ids": group_ids,
            "client": client,
            "started_at": asyncio.get_running_loop().time(),
        }
        return duplicate_key

    async def cancel_search(self, duplicate_key: str):
        self.active_searches.pop(duplicate_key, None)

    async def check_message(self, user_id: int, group_id: int, message: Any) -> bool:
        txt = (getattr(message, "message", "") or "").strip()
        if not txt:
            return False
        code = OTPFormatParser.extract_otp(txt)
        return bool(code)


otp_search_manager = AutoOTPSearchManager()


async def fetch_otp_from_groups(user_id: int, group_ids: List[str], lookback: int = 30, formats: Optional[List[str]] = None) -> Optional[str]:
    client = user_clients.get(user_id)
    if not client:
        return None

    formats = formats or []
    otp_re = re.compile(r"\b(\d{4,8})\b")

    for gid in group_ids:
        if not gid:
            continue
        try:
            target = int(gid)
        except ValueError:
            target = gid

        try:
            msgs = await client.get_messages(target, limit=lookback)
            for msg in msgs:
                txt = (msg.message or "").strip()
                if not txt:
                    continue
                # prefer explicit format if provided
                if formats:
                    for fmt in formats:
                        if OTPFormatParser.message_contains_format(txt, fmt):
                            cand = OTPFormatParser.extract_otp(txt)
                            if cand:
                                return cand
                m = otp_re.search(txt)
                if m:
                    return m.group(1)
        except errors.rpcerrorlist.ChatAdminRequiredError:
            logger.warning("No access to group %s for user %s", gid, user_id)
        except Exception:
            logger.exception("Error reading OTP from group %s for user %s", gid, user_id)

    return None


# ------------------------------------------------------------
# FORDET-style category listing for getallid
# ------------------------------------------------------------
_GETALLID_PAGE_SIZE = 10


async def _dialogs_to_entries(client: TelegramClient, category: str) -> list:
    entries = []
    async for dlg in client.iter_dialogs(limit=500):
        chat = dlg.entity
        cid = getattr(chat, "id", None)
        type_name = type(chat).__name__.lower()
        title = (
            getattr(chat, "title", None)
            or getattr(chat, "first_name", None)
            or getattr(chat, "username", None)
            or "(no title)"
        )

        is_user = type_name == "user"
        is_channel = type_name == "channel"
        is_chat = type_name == "chat"
        is_bot = bool(getattr(chat, "bot", False))

        include = False
        if category == "All":
            include = True
        elif category == "Groups":
            include = is_chat or (is_channel and getattr(chat, "megagroup", False))
        elif category == "Channels":
            include = is_channel and not getattr(chat, "megagroup", False)
        elif category == "Private":
            include = is_user and not is_bot
        elif category == "Bots":
            include = is_user and is_bot

        if include and cid is not None:
            entries.append((str(title), int(cid), type_name))

    return entries


def _make_getallid_kb(entries_page: list, category: str, page: int, total_pages: int):
    kb = []
    for title, cid, type_name in entries_page:
        label = (title[:30] + "...") if len(title) > 33 else title
        kb.append([
            InlineKeyboardButton(f"Select: {label}", callback_data=f"getallid:select:{cid}"),
            InlineKeyboardButton("Info", callback_data=f"getallid:info:{cid}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"getallid:cat:{category}:{page - 1}"))
    nav.append(InlineKeyboardButton("🔙 Categories", callback_data="getallid:back"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"getallid:cat:{category}:{page + 1}"))
    if nav:
        kb.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=kb)


# ------------------------------------------------------------
# Commands: start/login/logout/setchatid/getallid/otpgroups
# ------------------------------------------------------------
@router.message(Command("start"))
async def start_handler(message: Message):
    help_text = (
        "FORDET-Style Guardian Bot\n\n"
        "Commands:\n"
        "/login - start the real login flow (phone -> code -> password)\n"
        "/logout - logout and remove saved session\n"
        "/setchatid <chat_id> - set target chat for forwarded results\n"
        "/otpgroups - configure/view OTP groups and select them with inline buttons\n"
        "/getallid - list your available groups/chats from the logged-in user account\n"
        "/pause | /resume | /stats | /cancel\n\n"
        "How it works:\n"
        "1) /login and complete the interactive login flow\n"
        "2) /otpgroups GroupName: -100123,-100456 to add a group\n"
        "3) Send numbers; the bot asks you to choose an OTP group via inline buttons\n"
        "4) If device requests a code, the bot reads the OTP from the selected groups using your logged-in user\n"
        "5) When the device says continue, the bot forwards the plain number to the target chat and waits for exact case-sensitive SUCCESS/FAILED messages."
    )
    await message.answer(help_text)


@router.message(Command("login"))
async def login_command(message: Message):
    user_id = message.from_user.id
    login_states[user_id] = {"stage": "phone"}
    await message.answer("Please send your phone number with country code, e.g. +2348012345678")


@router.message(Command("logout"))
async def logout_command(message: Message):
    user_id = message.from_user.id
    await _disconnect_user_client(user_id)
    await _remove_saved_session_from_db(user_id)
    login_states.pop(user_id, None)
    await message.answer("✅ Logged out and session removed.")


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
        await message.answer(f"✅ Target chat ID successfully set to `{chat_id}`", parse_mode="Markdown")
    except ValueError:
        await message.answer("❌ Invalid Chat ID format. Must be an integer.")


@router.message(Command("getallid"))
async def getallid_command(message: Message):
    kb = [
        [InlineKeyboardButton("All", callback_data="getallid:cat:All:0")],
        [
            InlineKeyboardButton("Groups", callback_data="getallid:cat:Groups:0"),
            InlineKeyboardButton("Channels", callback_data="getallid:cat:Channels:0"),
        ],
        [
            InlineKeyboardButton("Private", callback_data="getallid:cat:Private:0"),
            InlineKeyboardButton("Bots", callback_data="getallid:cat:Bots:0"),
        ],
    ]
    await message.answer("Select category to list chat ids:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.message(Command("otpgroups"))
async def otpgroups_command(message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)

    if len(args) > 1:
        try:
            name, gids = args[1].split(":", 1)
            name = name.strip()
            gids = gids.strip()
            if not name or not gids:
                raise ValueError
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO otp_groups_bot (user_id, name, group_ids, formats)
                    VALUES ($1, $2, $3, '[]')
                    ON CONFLICT (user_id, name)
                    DO UPDATE SET group_ids = EXCLUDED.group_ids
                    """,
                    user_id,
                    name,
                    gids,
                )
            await message.answer(f"✅ OTP Group `{name}` added successfully.", parse_mode="Markdown")
        except Exception:
            await message.answer("❌ Format error. Use: `/otpgroups GroupName: -100123,-100456`", parse_mode="Markdown")
            return

    if db_pool is None:
        await message.answer("Database not ready.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, group_ids FROM otp_groups_bot WHERE user_id = $1 ORDER BY name ASC",
            user_id,
        )

    if not rows:
        await message.answer("No OTP groups configured. Add one using:\n`/otpgroups GroupName: -100123,-100456`", parse_mode="Markdown")
        return

    page_size = 5
    page = 0
    total_pages = (len(rows) + page_size - 1) // page_size

    def render(page_index: int):
        start = page_index * page_size
        end = start + page_size
        subset = rows[start:end]
        kb = []
        for row in subset:
            name = row["name"]
            gids = row["group_ids"]
            kb.append([
                InlineKeyboardButton(f"Select {name}", callback_data=f"otp:select:{name}"),
                InlineKeyboardButton(f"Edit {name}", callback_data=f"otp:edit:{name}"),
            ])
            kb.append([
                InlineKeyboardButton(f"Rename {name}", callback_data=f"otp:rename:{name}"),
                InlineKeyboardButton(f"Delete {name}", callback_data=f"otp:delete:{name}"),
            ])
        nav = []
        if page_index > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"otp:page:{page_index - 1}"))
        nav.append(InlineKeyboardButton("➕ Add New", callback_data="otp:add"))
        if page_index < total_pages - 1:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"otp:page:{page_index + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("🗑 Delete All", callback_data="otp:delete_all")])
        return InlineKeyboardMarkup(inline_keyboard=kb)

    text = "📋 **Configured OTP Groups:**\n\n"
    for row in rows[start:end if False else 0:1]:
        pass
    text += "\n".join([f"• **{r['name']}**: `{r['group_ids']}`" for r in rows[:page_size]])
    await message.answer(text, parse_mode="Markdown", reply_markup=render(page))


# ------------------------------------------------------------
# Real login flow (phone -> code -> password)
# ------------------------------------------------------------
@router.message()
async def login_flow_messages(message: Message):
    user_id = message.from_user.id
    state = login_states.get(user_id)
    if not state:
        return

    text = (message.text or "").strip()
    if not text:
        return

    stage = state.get("stage")

    if stage == "phone":
        phone = text
        if not phone.startswith("+"):
            phone = "+" + phone

        temp_client = TelegramClient(StringSession(), API_ID, API_HASH)
        try:
            await temp_client.connect()
            try:
                await temp_client.send_code_request(phone)
            except errors.PhoneNumberInvalidError:
                await message.answer("❌ Invalid phone number. Please send a valid phone with country code, e.g. +2348012345678.")
                await temp_client.disconnect()
                login_states.pop(user_id, None)
                return
            except Exception:
                logger.exception("Failed to send code request")
                await message.answer("❌ Failed to send code request. Please try again.")
                await temp_client.disconnect()
                login_states.pop(user_id, None)
                return

            state.update({"stage": "code", "phone": phone, "temp_client": temp_client})
            await message.answer("Code sent to your Telegram. Please reply with the code you received.")
            return
        except Exception:
            logger.exception("Error starting login flow")
            await message.answer("❌ Internal error starting login.")
            login_states.pop(user_id, None)
            return

    elif stage == "code":
        code = text
        phone = state.get("phone")
        temp_client: Optional[TelegramClient] = state.get("temp_client")

        if not temp_client or not phone:
            await message.answer("❌ Login session lost. Please use /login again.")
            login_states.pop(user_id, None)
            return

        try:
            try:
                await temp_client.sign_in(phone=phone, code=code)
            except errors.SessionPasswordNeededError:
                state.update({"stage": "password", "temp_client": temp_client})
                await message.answer("Two-step verification enabled. Please send your password.")
                return
            except Exception:
                try:
                    await temp_client.sign_in(phone=phone, code=code)
                except Exception:
                    logger.exception("Failed to sign in with code")
                    await message.answer("❌ Wrong code or sign-in failed. Start again with /login.")
                    try:
                        await temp_client.disconnect()
                    except Exception:
                        pass
                    login_states.pop(user_id, None)
                    return

            session_str = StringSession.save(temp_client.session)
            await _save_session_to_db(user_id, session_str)
            user_clients[user_id] = temp_client
            login_states.pop(user_id, None)
            await message.answer("✅ Login successful and session saved.")
            return
        except Exception:
            logger.exception("Error during code sign-in")
            await message.answer("❌ Login failed. Please try /login again.")
            try:
                await temp_client.disconnect()
            except Exception:
                pass
            login_states.pop(user_id, None)
            return

    elif stage == "password":
        password = text
        temp_client: Optional[TelegramClient] = state.get("temp_client")
        phone = state.get("phone")
        if not temp_client or not phone:
            await message.answer("❌ Login session lost. Please use /login again.")
            login_states.pop(user_id, None)
            return

        try:
            await temp_client.sign_in(password=password)
            session_str = StringSession.save(temp_client.session)
            await _save_session_to_db(user_id, session_str)
            user_clients[user_id] = temp_client
            login_states.pop(user_id, None)
            await message.answer("✅ Login successful and session saved with 2FA.")
        except Exception:
            logger.exception("Password sign-in failed")
            await message.answer("❌ Password incorrect or sign-in failed. Please restart /login.")
            try:
                await temp_client.disconnect()
            except Exception:
                pass
            login_states.pop(user_id, None)
            return


# ------------------------------------------------------------
# OTP group inline buttons / FORDET-style handlers
# ------------------------------------------------------------
@router.callback_query(F.data == "otp:add")
async def otp_add_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "Add OTP Group:\nUse `/otpgroups Name: -100123,-100456`",
        parse_mode="Markdown",
    )


@router.callback_query(F.data.startswith("otp:page:"))
async def otp_page_callback(callback: CallbackQuery):
    page_text = callback.data.replace("otp:page:", "", 1)
    try:
        page = int(page_text)
    except ValueError:
        page = 0

    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, group_ids FROM otp_groups_bot WHERE user_id = $1 ORDER BY name ASC",
            user_id,
        )

    if not rows:
        await callback.message.edit_text("No OTP groups configured. Use `/otpgroups GroupName: -100123,-100456`", parse_mode="Markdown")
        return

    page_size = 5
    total_pages = (len(rows) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    subset = rows[start:end]

    kb = []
    for row in subset:
        name = row["name"]
        kb.append([
            InlineKeyboardButton(f"Select {name}", callback_data=f"otp:select:{name}"),
            InlineKeyboardButton(f"Edit {name}", callback_data=f"otp:edit:{name}"),
        ])
        kb.append([
            InlineKeyboardButton(f"Rename {name}", callback_data=f"otp:rename:{name}"),
            InlineKeyboardButton(f"Delete {name}", callback_data=f"otp:delete:{name}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"otp:page:{page - 1}"))
    nav.append(InlineKeyboardButton("➕ Add New", callback_data="otp:add"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"otp:page:{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("🗑 Delete All", callback_data="otp:delete_all")])

    text = "📋 **Configured OTP Groups:**\n\n"
    text += "\n".join([f"• **{r['name']}**: `{r['group_ids']}`" for r in subset])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("otp:select:"))
async def otp_select_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    group_name = callback.data.replace("otp:select:", "", 1)

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name, group_ids FROM otp_groups_bot WHERE user_id = $1 AND name = $2",
            user_id,
            group_name,
        )

    if row:
        selected_otp_group[user_id] = dict(row)
        await callback.message.edit_text(
            f"✅ Selected OTP Group: **{group_name}**. Starting process...",
            parse_mode="Markdown",
        )
        device_event.set()
    else:
        await callback.answer("Group not found.", show_alert=True)


@router.callback_query(F.data.startswith("otp:edit:"))
async def otp_edit_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.data.replace("otp:edit:", "", 1)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name, group_ids FROM otp_groups_bot WHERE user_id = $1 AND name = $2",
            user_id,
            name,
        )
    if not row:
        await callback.answer("Group not found.", show_alert=True)
        return
    await callback.message.edit_text(
        f"Edit OTP Group\n\n**{name}**\n`{row['group_ids']}`\n\nUse `/otpgroups {name}: -100123,-100456` to update it.",
        parse_mode="Markdown",
    )


@router.callback_query(F.data.startswith("otp:rename:"))
async def otp_rename_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.data.replace("otp:rename:", "", 1)
    await callback.message.edit_text(
        f"Rename OTP Group\nCurrent name: **{name}**\n\nUse: `/otpgroups NewName: -100123,-100456`",
        parse_mode="Markdown",
    )


@router.callback_query(F.data.startswith("otp:delete:"))
async def otp_delete_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.data.replace("otp:delete:", "", 1)

    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM otp_groups_bot WHERE user_id = $1 AND name = $2",
            user_id,
            name,
        )
    await callback.message.edit_text(f"✅ Deleted OTP group: **{name}**", parse_mode="Markdown")


@router.callback_query(F.data == "otp:delete_all")
async def otp_delete_all_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM otp_groups_bot WHERE user_id = $1", user_id)
    await callback.message.edit_text("✅ Deleted all OTP groups.", parse_mode="Markdown")


# ------------------------------------------------------------
# getallid category handlers
# ------------------------------------------------------------
@router.callback_query(F.data == "getallid:back")
async def getallid_back_callback(callback: CallbackQuery):
    kb = [
        [InlineKeyboardButton("All", callback_data="getallid:cat:All:0")],
        [
            InlineKeyboardButton("Groups", callback_data="getallid:cat:Groups:0"),
            InlineKeyboardButton("Channels", callback_data="getallid:cat:Channels:0"),
        ],
        [
            InlineKeyboardButton("Private", callback_data="getallid:cat:Private:0"),
            InlineKeyboardButton("Bots", callback_data="getallid:cat:Bots:0"),
        ],
    ]
    await callback.message.edit_text("Select category to list chat ids:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("getallid:cat:"))
async def getallid_category_callback(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer("Invalid request", show_alert=True)
        return
    category = parts[2]
    try:
        page = int(parts[3])
    except ValueError:
        page = 0

    user_id = callback.from_user.id
    client = user_clients.get(user_id)
    if not client:
        await callback.message.edit_text(
            "No logged-in user session found. Please /login to list your dialogs, or use the current chat info below."
        )
        await callback.message.reply(
            f"📍 **Current Chat Info:**\n• Chat ID: `{callback.message.chat.id}`\n• Type: `{callback.message.chat.type}`\n• Title: `{callback.message.chat.title or callback.message.chat.full_name}`",
            parse_mode="Markdown",
        )
        return

    await callback.message.edit_text(f"Loading {category} dialogs...")
    try:
        entries = await _dialogs_to_entries(client, category)
    except Exception:
        logger.exception("Failed to fetch dialogs for getallid")
        await callback.message.edit_text("Failed to fetch dialogs. Make sure your session is active and you have granted access.")
        return

    if not entries:
        await callback.message.edit_text(f"No dialogs found for category: {category}")
        return

    total = len(entries)
    total_pages = (total + _GETALLID_PAGE_SIZE - 1) // _GETALLID_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    start = page * _GETALLID_PAGE_SIZE
    end = start + _GETALLID_PAGE_SIZE
    entries_page = entries[start:end]

    text_lines = [f"Category: {category} — showing {start + 1}-{min(end, total)} of {total}\n"]
    for title, cid, type_name in entries_page:
        text_lines.append(f"• {title} — id: `{cid}` — type: `{type_name}`")
    kb = _make_getallid_kb(entries_page, category, page, total_pages)
    await callback.message.edit_text("\n".join(text_lines), parse_mode="Markdown", reply_markup=kb)


@router.callback_query(F.data.startswith("getallid:select:"))
async def getallid_select_callback(callback: CallbackQuery):
    cid_s = callback.data.replace("getallid:select:", "", 1)
    try:
        cid = int(cid_s)
    except ValueError:
        await callback.answer("Invalid chat id", show_alert=True)
        return

    user_id = callback.from_user.id
    user_target_chat_ids[user_id] = cid
    global target_chat_id
    target_chat_id = cid

    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_settings (user_id, chat_id)
                VALUES ($1, $2)
                ON CONFLICT (user_id)
                DO UPDATE SET chat_id = EXCLUDED.chat_id
                """,
                user_id,
                cid,
            )

    await callback.message.edit_text(f"✅ Target chat set to `{cid}`", parse_mode="Markdown")
    await callback.answer("Target chat saved.", show_alert=False)


@router.callback_query(F.data.startswith("getallid:info:"))
async def getallid_info_callback(callback: CallbackQuery):
    cid_s = callback.data.replace("getallid:info:", "", 1)
    try:
        cid = int(cid_s)
    except ValueError:
        await callback.answer("Invalid chat id", show_alert=True)
        return

    user_id = callback.from_user.id
    client = user_clients.get(user_id)
    info_text = f"Chat ID: `{cid}`\n"
    if client:
        try:
            entity = await client.get_entity(cid)
            title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or getattr(entity, "username", None) or ""
            info_text += f"Title: {title}\nType: {type(entity).__name__}\n"
        except Exception:
            info_text += "Could not fetch more details (maybe you don't have access).\n"

    kb_back = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton("🔙 Back", callback_data="getallid:back")]]
    )
    await callback.message.edit_text(info_text, parse_mode="Markdown", reply_markup=kb_back)
    await callback.answer()


# ------------------------------------------------------------
# Monitor chat for exact SUCCESS/FAILED messages
# ------------------------------------------------------------
@router.message()
async def monitor_live_chat_messages(message: Message):
    text = message.text or ""
    if not text:
        return

    if "SUCCESS" in text or "FAILED" in text:
        for num, ev in list(live_success_failure_events.items()):
            if num in text:
                if "SUCCESS" in text:
                    live_success_failure_results[num] = "SUCCESS"
                elif "FAILED" in text:
                    live_success_failure_results[num] = "FAILED"
                ev.set()


# ------------------------------------------------------------
# Number processing flow
# ------------------------------------------------------------
@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task, device_status, skip_current_number, current_index, total_numbers_count, active_number

    if is_processing:
        await message.answer("Another processing session is running. Use /cancel to stop it first.")
        return

    user_id = message.from_user.id
    target_chat = await _get_user_target_chat(user_id)
    if target_chat is None:
        target_chat = target_chat_id

    text = message.text
    numbers = [n.strip() for n in re.split(r"[,\s]+", text) if n.strip()]
    if not numbers:
        await message.answer("No valid numbers found in message.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, group_ids FROM otp_groups_bot WHERE user_id = $1 ORDER BY name ASC", user_id)

    if not rows:
        await message.answer("❌ No OTP groups found! Please configure one using `/otpgroups GroupName: -100123,-100456`.", parse_mode="Markdown")
        return

    keyboard_buttons = []
    for row in rows:
        keyboard_buttons.append([
            InlineKeyboardButton(text=row["name"], callback_data=f"otp:select:{row['name']}"),
        ])

    markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    device_event.clear()
    await message.answer("👇 **Please select the OTP group to use for this batch:**", reply_markup=markup, parse_mode="Markdown")

    try:
        await asyncio.wait_for(device_event.wait(), timeout=60)
    except asyncio.TimeoutError:
        await message.answer("⏳ OTP group selection timed out. Process cancelled.")
        return

    is_processing = True
    total_numbers_count = len(numbers)
    current_task = asyncio.current_task()

    try:
        status_msg = await message.answer(f"Processing {total_numbers_count} numbers...")
        for i, num in enumerate(numbers, start=1):
            current_index = i
            active_number = num
            skip_current_number = False

            await pause_event.wait()
            await status_msg.edit_text(f"Processing batch\n\nProgress: {i}/{total_numbers_count}\nTarget: {num}")
            num_msg = await message.answer(f"{num} ⏳ Checking on device...")

            device_event.clear()
            device_status = None

            success = await call_device_api(num, payload_type="check")
            if not success:
                await num_msg.edit_text(f"{num} 🔴 Error (Webhook failed)")
                await log_number_status(num, "webhook_error")
                continue

            try:
                await asyncio.wait_for(device_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                if skip_current_number:
                    await num_msg.edit_text(f"{num} ⏭️ Skipped")
                    await log_number_status(num, "skipped")
                    continue
                await num_msg.edit_text(f"{num} 🟠 Ended (Device timeout - 5 mins)")
                await log_number_status(num, "device_timeout")
                continue

            if skip_current_number:
                await num_msg.edit_text(f"{num} ⏭️ Skipped")
                await log_number_status(num, "skipped")
                continue

            result_text = (device_status or "").lower()

            if "blocked" in result_text:
                await num_msg.edit_text(f"{num} ⚫ Bad")
                await log_number_status(num, "blocked")
                continue

            if "try_again" in result_text:
                await num_msg.edit_text(f"{num} 🟡 Try Later")
                await log_number_status(num, "try_again")
                await asyncio.sleep(5)
                continue

            if "error" in result_text and "request_code" not in result_text:
                await num_msg.edit_text(f"{num} 🔴 Error Code")
                await log_number_status(num, "error")
                continue

            if "request_code" in result_text or "requestcode" in result_text or "request-code" in result_text:
                selected_group = selected_otp_group.get(user_id, {})
                await num_msg.edit_text(
                    f"{num} ⚪ Device requested code. Using group: {selected_group.get('name', 'Default')}"
                )

                group_ids_raw = (selected_group.get("group_ids") or "")
                group_ids = [g.strip() for g in group_ids_raw.split(",") if g.strip()]
                code = None

                if group_ids:
                    code = await fetch_otp_from_groups(user_id, group_ids)
                    if code:
                        await num_msg.edit_text(f"{num} ⚪ OTP fetched from group: `{code}`", parse_mode="Markdown")
                    else:
                        await num_msg.edit_text(
                            f"{num} ⚠️ Could not find OTP in the chosen group. Please ensure your logged-in account is a member and can read the group."
                        )
                else:
                    await num_msg.edit_text(f"{num} ⚠️ No group IDs configured for the selected OTP group.")

                if not code:
                    await num_msg.edit_text(f"{num} 🔴 No OTP available. Skipping.")
                    await log_number_status(num, "no_otp_found")
                    continue

                device_event.clear()
                device_status = None
                await call_device_api(num, payload_type="send_code", code=code)

                try:
                    await asyncio.wait_for(device_event.wait(), timeout=300)
                    if skip_current_number:
                        await num_msg.edit_text(f"{num} ⏭️ Skipped")
                        await log_number_status(num, "skipped")
                        continue
                except asyncio.TimeoutError:
                    if skip_current_number:
                        await num_msg.edit_text(f"{num} ⏭️ Skipped")
                        await log_number_status(num, "skipped")
                        continue
                    await num_msg.edit_text(f"{num} 🟠 Ended (Device timeout after sending code - 5 mins)")
                    await log_number_status(num, "device_timeout_after_code")
                    continue

            result_after = (device_status or "").lower()
            if "continue" not in result_after and "continued" not in result_after and "proceed" not in result_after:
                await num_msg.edit_text(f"{num} ⚠️ Device did not signal to continue. Status: `{device_status}`", parse_mode="Markdown")
                await log_number_status(num, "no_continue")
                continue

            await num_msg.edit_text(f"{num} 🟢 Continued. Forwarding internally and awaiting SUCCESS/FAILED...")

            if target_chat:
                try:
                    await message.bot.send_message(chat_id=target_chat, text=num)
                    ev = asyncio.Event()
                    live_success_failure_events[num] = ev

                    try:
                        await asyncio.wait_for(ev.wait(), timeout=300)
                        decision = live_success_failure_results.get(num, "UNKNOWN")
                        await num_msg.edit_text(f"{num} 🏁 Result Decided: {decision}")
                        await log_number_status(num, decision.lower())
                    except asyncio.TimeoutError:
                        await num_msg.edit_text(f"{num} 🟠 Live decision timeout (No SUCCESS/FAILED received)")
                        await log_number_status(num, "decision_timeout")
                    finally:
                        live_success_failure_events.pop(num, None)
                        live_success_failure_results.pop(num, None)
                except Exception:
                    logger.exception("Failed to forward to target chat %s", target_chat)
                    await num_msg.edit_text(f"{num} ❌ Error forwarding to target chat ID.")
            else:
                await num_msg.edit_text(f"{num} ⚠️ Target chat ID not set! Use /setchatid.")

        await status_msg.edit_text(f"Batch completed.\nProcessed: {total_numbers_count}/{total_numbers_count}")

    except asyncio.CancelledError:
        logger.info("Task cancelled by user request.")
    except Exception:
        logger.exception("Error during processing")
        await message.answer("An error occurred during processing.")
    finally:
        is_processing = False
        is_paused = False
        current_task = None
        active_number = None
        skip_current_number = False
        pause_event.set()


# ------------------------------------------------------------
# pause/resume/cancel/stats
# ------------------------------------------------------------
@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global is_processing, current_task, skip_current_number
    if is_processing and current_task:
        current_task.cancel()
        is_processing = False
        device_event.set()
        skip_current_number = True
        pause_event.set()
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


# ------------------------------------------------------------
# Device callback (MacroDroid)
# ------------------------------------------------------------
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
    await init_db()
    await _load_saved_sessions_on_startup()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await start_web_server()
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully.")
    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()
        for client in list(user_clients.values()):
            try:
                await client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())

