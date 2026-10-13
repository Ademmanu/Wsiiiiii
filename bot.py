import asyncio
import logging
import os
import random
import aiohttp
from aiohttp import web
import phonenumbers
import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION FROM ENVIRONMENT ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
MACRODROID_WEBHOOK_URL = os.getenv(
    "MACRODROID_WEBHOOK_URL", 
    "http://trigger.macrodroid.com/YOUR_UNIQUE_DEVICE_UUID/telegram_bot"
)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/dbname")
PORT = int(os.getenv("PORT", 10000))

router = Router()

# Global DB pool
db_pool: asyncpg.Pool | None = None

# Global state management
is_processing = False
is_paused = False
current_task: asyncio.Task | None = None

# Progress & statistics
current_index = 0
total_numbers_count = 0
active_number = None
target_chat_id: int | None = None

# Synchronization events
device_event = asyncio.Event()
device_status: str | None = None

skip_current_number = False
pause_event = asyncio.Event()
pause_event.set()

# Temporary states for user flow
user_otp_groups: dict[int, list] = {}  # user_id -> list of otp groups
selected_otp_group: dict[int, dict] = {} # user_id -> chosen group info
live_success_failure_events: dict[str, asyncio.Event] = {} # duplicate_key/number -> Event
live_success_failure_results: dict[str, str] = {} # number -> SUCCESS/FAILED

# --- DATABASE INITIALIZATION ---

async def init_db():
    """Initializes asyncpg connection pool and creates tables if they don't exist."""
    global db_pool
    dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL
    
    try:
        db_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10)
        logger.info("Successfully connected to PostgreSQL pool.")
        
        async with db_pool.acquire() as conn:
            await conn.execute("""
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
                    group_ids TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bot_settings (
                    user_id BIGINT PRIMARY KEY,
                    chat_id BIGINT
                );
            """)
        logger.info("Database schema verified.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL: {e}")

async def log_number_status(phone_number: str, status: str, details: str = None):
    """Helper to log execution status for each phone number into Postgres."""
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO number_logs (phone_number, status, details) VALUES ($1, $2, $3)",
                phone_number, status, details
            )
    except Exception as e:
        logger.error(f"Failed to write log to PostgreSQL for {phone_number}: {e}")

# --- HELPER FUNCTIONS ---

def parse_phone_number(raw_number: str):
    """Splits raw number into (country_code, national_number)."""
    cleaned = raw_number.strip()
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned
        
    try:
        parsed = phonenumbers.parse(cleaned, None)
        if phonenumbers.is_valid_number(parsed):
            return str(parsed.country_code), str(parsed.national_number)
    except Exception as e:
        logger.warning(f"Failed to parse {raw_number} using phonenumbers: {e}")
        
    if cleaned.startswith("+234") or cleaned.startswith("+233"):
        return cleaned[1:4], cleaned[4:]
    elif cleaned.startswith("+44"):
        return cleaned[1:3], cleaned[3:]
    
    return cleaned[1:4], cleaned[4:]

async def call_device_api(raw_number: str, payload_type: str = "check", code: str | None = None):
    """Triggers MacroDroid webhook to start phone automation."""
    country_code, national_number = parse_phone_number(raw_number)
    
    url = (
        f"{MACRODROID_WEBHOOK_URL}"
        f"?country_code={country_code}&national_number={national_number}&type={payload_type}"
    )
    if code:
        url += f"&code={code}"

    logger.info(f"Triggering MacroDroid -> +{country_code} {national_number} [{payload_type}]")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                logger.info(f"MacroDroid webhook triggered, status: {response.status}")
                return True
    except Exception as e:
        logger.error(f"Failed to reach MacroDroid webhook: {e}")
        return False

# --- COMMAND HANDLERS ---

@router.message(Command("start"))
async def start_handler(message: Message):
    help_text = (
        "Telegram Guardian Bot\n\n"
        "Commands:\n"
        "/setchatid <chat_id> - Set target chat for forwarding results\n"
        "/otpgroups - Configure/view OTP groups\n"
        "/getallid - List accessible chat IDs\n"
        "/pause | /resume | /stats | /cancel"
    )
    await message.answer(help_text)

@router.message(Command("setchatid"))
async def setchatid_handler(message: Message):
    global target_chat_id
    args = message.text.split()
    if len(args) < 2:
        await message.answer(f"Current target chat id: `{target_chat_id}`\nUsage: `/setchatid -100xxxxxxxxxx`", parse_mode="Markdown")
        return
    try:
        target_chat_id = int(args[1])
        if db_pool:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO bot_settings (user_id, chat_id) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET chat_id = $2",
                    message.from_user.id, target_chat_id
                )
        await message.answer(f"✅ Target chat ID successfully set to `{target_chat_id}`", parse_mode="Markdown")
    except ValueError:
        await message.answer("❌ Invalid Chat ID format. Must be an integer.")

@router.message(Command("getallid"))
async def getallid_handler(message: Message):
    await message.answer(
        f"📍 **Current Chat Info:**\n"
        f"• Chat ID: `{message.chat.id}`\n"
        f"• Type: `{message.chat.type}`\n"
        f"• Title: `{message.chat.title or message.chat.full_name}`",
        parse_mode="Markdown"
    )

@router.message(Command("otpgroups"))
async def otpgroups_handler(message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    
    if len(args) > 1:
        try:
            name, gids = args[1].split(":", 1)
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO otp_groups_bot (user_id, name, group_ids) VALUES ($1, $2, $3)",
                    user_id, name.strip(), gids.strip()
                )
            await message.answer(f"✅ OTP Group `{name.strip()}` added successfully.")
        except Exception:
            await message.answer("❌ Format error. Use: `/otpgroups GroupName: -100123,-100456`", parse_mode="Markdown")
            return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, group_ids FROM otp_groups_bot WHERE user_id = $1", user_id)
    
    if not rows:
        await message.answer("No OTP groups configured. Add one using:\n`/otpgroups GroupName: -100123,-100456`", parse_mode="Markdown")
        return
        
    text = "📋 **Configured OTP Groups:**\n\n"
    for r in rows:
        text += f"• **{r['name']}**: `{r['group_ids']}`\n"
    await message.answer(text, parse_mode="Markdown")

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
        await message.answer("No active running session to pause.")

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

# --- INLINE BUTTON CALLBACK FOR OTP GROUP SELECTION ---
@router.callback_query(F.data.startswith("sel_otp_"))
async def otp_selection_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    group_name = callback.data.replace("sel_otp_", "")
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT name, group_ids FROM otp_groups_bot WHERE user_id = $1 AND name = $2", user_id, group_name)
    
    if row:
        selected_otp_group[user_id] = dict(row)
        await callback.message.edit_text(f"✅ Selected OTP Group: **{group_name}**. Starting process...", parse_mode="Markdown")
        device_event.set()
    else:
        await callback.answer("Group not found!", show_alert=True)

# --- LIVE MONITORING CHAT LISTENER FOR SUCCESS/FAILED ---
@router.message()
async def monitor_live_chat_messages(message: Message):
    text = message.text or ""
    if "SUCCESS" in text or "FAILED" in text:
        status_val = "SUCCESS" if "SUCCESS" in text else "FAILED"
        for num, ev in list(live_success_failure_events.items()):
            if num in text:
                live_success_failure_results[num] = status_val
                ev.set()

# --- NUMBER PROCESSOR ---

@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task, device_event, device_status, skip_current_number, current_index, total_numbers_count, active_number, target_chat_id

    if is_processing:
        await message.answer("Another processing session is running. Use /cancel to stop it first.")
        return

    user_id = message.from_user.id
    
    if not target_chat_id and db_pool:
        async with db_pool.acquire() as conn:
            res = await conn.fetchval("SELECT chat_id FROM bot_settings WHERE user_id = $1", user_id)
            if res:
                target_chat_id = res

    text = message.text
    numbers = [n.strip() for n in text.replace(",", "\n").split() if n.strip()]

    if not numbers:
        await message.answer("No valid numbers found in message.")
        return

    async with db_pool.acquire() as conn:
        groups = await conn.fetch("SELECT name FROM otp_groups_bot WHERE user_id = $1", user_id)

    if not groups:
        await message.answer("❌ No OTP groups found! Please configure at least one group using `/otpgroups Name: -100xxx` before sending numbers.", parse_mode="Markdown")
        return

    keyboard_buttons = [[InlineKeyboardButton(text=g["name"], callback_data=f"sel_otp_{g['name']}")] for g in groups]
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

            res = (device_status or "").lower()

            if "blocked" in res:
                await num_msg.edit_text(f"{num} ⚫ Bad")
                await log_number_status(num, "blocked")
                continue

            elif "try_again" in res:
                await num_msg.edit_text(f"{num} 🟡 Try Later")
                await log_number_status(num, "try_again")
                await asyncio.sleep(5)
                continue

            elif "error" in res:
                await num_msg.edit_text(f"{num} 🔴 Error Code")
                await log_number_status(num, "error")
                continue

            elif "request_code" in res:
                selected_group = selected_otp_group.get(user_id, {})
                await num_msg.edit_text(f"{num} ⚪ Requesting code via group: {selected_group.get('name', 'Default')}")
                
                code = f"{random.randint(0, 999999):06d}"
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

            if skip_current_number:
                await num_msg.edit_text(f"{num} ⏭️ Skipped")
                await log_number_status(num, "skipped")
                continue

            await num_msg.edit_text(f"{num} 🟢 Continued. Forwarding internally and awaiting SUCCESS/FAILED...")
            
            if target_chat_id:
                try:
                    forwarded_msg = await message.bot.send_message(chat_id=target_chat_id, text=num)
                    
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
                        
                except Exception as ex:
                    logger.error(f"Failed to forward to target chat ID {target_chat_id}: {ex}")
                    await num_msg.edit_text(f"{num} ❌ Error forwarding to target chat ID.")
            else:
                await num_msg.edit_text(f"{num} ⚠️ Target chat ID not set! Use /setchatid.")

        await status_msg.edit_text(f"Batch completed.\nProcessed: {total_numbers_count}/{total_numbers_count}")

    except asyncio.CancelledError:
        logger.info("Task was cancelled by user request.")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        await message.answer(f"An error occurred during processing: {str(e)}")
    finally:
        is_processing = False
        is_paused = False
        current_task = None
        active_number = None
        skip_current_number = False
        pause_event.set()

# --- WEB SERVER (RENDER HEALTH + DEVICE CALLBACKS) ---
async def handle_health_check(request):
    return web.Response(text="Bot is running live!")

async def handle_device_callback(request):
    global device_status, device_event
    status = request.query.get("status", "unknown")
    logger.info(f"Received device callback -> status: {status}")
    
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
    logger.info(f"Web Server running on port {PORT} (Handling health checks & device callbacks).")

async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await start_web_server()
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully. Polling for messages...")
    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
