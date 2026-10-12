import asyncio
import logging
import os
import random
import aiohttp
from aiohttp import web
import phonenumbers
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION FROM ENVIRONMENT ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
MACRODROID_WEBHOOK_URL = os.getenv(
    "MACRODROID_WEBHOOK_URL", 
    "http://trigger.macrodroid.com/YOUR_UNIQUE_DEVICE_UUID/telegram_bot"
)
PORT = int(os.getenv("PORT", 10000))

router = Router()

# Global state management
is_processing = False
is_paused = False
current_task: asyncio.Task | None = None

# Progress & statistics
current_index = 0
total_numbers_count = 0
active_number = None

# Synchronization events
device_event = asyncio.Event()
device_status: str | None = None

user_intervention_event = asyncio.Event()
user_intervention_result: str | None = None

skip_event = asyncio.Event()
pause_event = asyncio.Event()
pause_event.set()  # Initially set so it's not blocked

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
        "🤖 **Telegram Guardian Bot (MacroDroid Bridge)**\n\n"
        "Send a list of phone numbers (comma or space separated) to begin automated processing.\n\n"
        "🛠 **Available Commands:**\n"
        "• `/success` - Mark current number as Success 🟢\n"
        "• `/failed` - Mark current number as Failed 🟠\n"
        "• `/next` - Skip current number and move to the next ⏭️\n"
        "• `/pause` - Temporarily pause processing queue ⏸️\n"
        "• `/resume` - Resume paused processing queue ▶️\n"
        "• `/stats` - Check current batch progress 📊\n"
        "• `/cancel` - Stop current batch completely ❌"
    )
    await message.reply(help_text, parse_mode="Markdown")

@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global is_processing, current_task
    if is_processing and current_task:
        current_task.cancel()
        is_processing = False
        device_event.set()
        user_intervention_event.set()
        skip_event.set()
        pause_event.set()
        await message.reply("❌ Processing cancelled successfully.")
    else:
        await message.reply("No active processing session to cancel.")

@router.message(Command("pause"))
async def pause_handler(message: Message):
    global is_paused
    if is_processing and not is_paused:
        is_paused = True
        pause_event.clear()
        await message.reply("⏸️ Processing paused. Send `/resume` when ready.")
    else:
        await message.reply("No active running session to pause.")

@router.message(Command("resume"))
async def resume_handler(message: Message):
    global is_paused
    if is_processing and is_paused:
        is_paused = False
        pause_event.set()
        await message.reply("▶️ Processing resumed.")
    else:
        await message.reply("Processing is not currently paused.")

@router.message(Command("next"))
async def next_handler(message: Message):
    if is_processing:
        skip_event.set()
        device_event.set()
        user_intervention_event.set()
        await message.reply("⏭️ Skipping current number...")
    else:
        await message.reply("No active processing session.")

@router.message(Command("stats"))
async def stats_handler(message: Message):
    if is_processing:
        await message.reply(f"📊 **Current Status:**\nProcessing number {current_index} of {total_numbers_count}\nActive target: `{active_number}`", parse_mode="Markdown")
    else:
        await message.reply("No active session running.")

@router.message(Command("success"))
async def success_handler(message: Message):
    global user_intervention_result
    if is_processing and not user_intervention_event.is_set():
        user_intervention_result = "success"
        user_intervention_event.set()
    else:
        await message.reply("No pending step requiring /success.")

@router.message(Command("failed"))
async def failed_handler(message: Message):
    global user_intervention_result
    if is_processing and not user_intervention_event.is_set():
        user_intervention_result = "failed"
        user_intervention_event.set()
    else:
        await message.reply("No pending step requiring /failed.")

# --- NUMBER PROCESSOR ---

@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task, device_event, device_status, user_intervention_event, user_intervention_result, skip_event, current_index, total_numbers_count, active_number

    if is_processing:
        await message.reply("⚠️ Another processing session is already running. Use /cancel to stop it first.")
        return

    text = message.text
    numbers = [n.strip() for n in text.replace(",", "\n").split() if n.strip()]

    if not numbers:
        await message.reply("No valid numbers found in your message.")
        return

    is_processing = True
    total_numbers_count = len(numbers)
    current_task = asyncio.current_task()

    try:
        status_msg = await message.answer(f"Processing {total_numbers_count} numbers...")

        for i, num in enumerate(numbers, start=1):
            current_index = i
            active_number = num

            # Check pause state
            await pause_event.wait()

            # Check if skipped before starting number
            if skip_event.is_set():
                break

            await status_msg.edit_text(f"Processing numbers\n\nProgress: {i}/{total_numbers_count} ({num})")
            num_msg = await message.answer(f"{num} ⏳ Checking on device...")

            device_event.clear()
            device_status = None
            skip_event.clear()

            success = await call_device_api(num, payload_type="check")
            if not success:
                await num_msg.edit_text(f"{num} 🔴 Error (Webhook failed)")
                continue

            # Wait for device callback (up to 3 mins) or skip/cancel
            try:
                done, pending = await asyncio.wait(
                    [asyncio.create_task(device_event.wait()), asyncio.create_task(skip_event.wait())],
                    timeout=180,
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                    
                if skip_event.is_set():
                    await num_msg.edit_text(f"{num} ⏭️ Skipped")
                    continue
            except asyncio.TimeoutError:
                await num_msg.edit_text(f"{num} 🟠 Ended (Device timeout)")
                continue

            res = (device_status or "").lower()

            if "blocked" in res:
                await num_msg.edit_text(f"{num} ⚫ Bad")
                continue

            elif "try_again" in res:
                await num_msg.edit_text(f"{num} 🟡 Try Later")
                await asyncio.sleep(5)
                continue

            elif "error" in res:
                await num_msg.edit_text(f"{num} 🔴 Error Code")
                continue

            elif "request_code" in res:
                await num_msg.edit_text(f"{num} ⚪ Code")
                
                code = f"{random.randint(0, 999999):06d}"
                device_event.clear()
                device_status = None

                await call_device_api(num, payload_type="send_code", code=code)

                try:
                    await asyncio.wait_for(device_event.wait(), timeout=180)
                    res = (device_status or "").lower()
                    if "blocked" in res:
                        await num_msg.edit_text(f"{num} ⚫ Bad")
                        continue
                except asyncio.TimeoutError:
                    pass

            # Edit message for manual intervention window
            await num_msg.edit_text(f"{num} 🟤 Waiting for your decision (/success, /failed, or /next)")
            
            user_intervention_event.clear()
            skip_event.clear()
            user_intervention_result = None
            
            try:
                done, pending = await asyncio.wait(
                    [asyncio.create_task(user_intervention_event.wait()), asyncio.create_task(skip_event.wait())],
                    timeout=180,
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()

                if skip_event.is_set():
                    await num_msg.edit_text(f"{num} ⏭️ Skipped")
                    continue
                
                if user_intervention_result == "success":
                    await num_msg.edit_text(f"{num} 🟢 Success")
                elif user_intervention_result == "failed":
                    await num_msg.edit_text(f"{num} 🟠 Failed")
            except asyncio.TimeoutError:
                await num_msg.edit_text(f"{num} 🟠 Ended (Time expired)")

        await status_msg.edit_text(f"Processing Completed\n\nProcessed {total_numbers_count} numbers")

    except asyncio.CancelledError:
        logger.info("Task was cancelled by user request.")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        await message.answer(f"❌ An error occurred during processing: {str(e)}")
    finally:
        is_processing = False
        is_paused = False
        current_task = None
        active_number = None
        pause_event.set()

# --- WEB SERVER (RENDER HEALTH + DEVICE CALLBACKS) ---
async def handle_health_check(request):
    return web.Response(text="Bot is running live!")

async def handle_device_callback(request):
    """Incoming webhook endpoint for MacroDroid to report status back to the bot."""
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
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await start_web_server()
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully. Polling for messages...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
