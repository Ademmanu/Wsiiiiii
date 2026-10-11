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
current_task: asyncio.Task | None = None

# Synchronization events for device callbacks and human intervention
device_event = asyncio.Event()
device_status: str | None = None

user_intervention_event = asyncio.Event()
user_intervention_result: str | None = None

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

@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global is_processing, current_task
    if is_processing and current_task:
        current_task.cancel()
        is_processing = False
        device_event.set()
        user_intervention_event.set()
        await message.reply("❌ Processing cancelled successfully.")
    else:
        await message.reply("No active processing session to cancel.")

@router.message(Command("success"))
async def success_handler(message: Message):
    global user_intervention_result
    if is_processing and not user_intervention_event.is_set():
        user_intervention_result = "success"
        user_intervention_event.set()
        await message.reply("✅ Marked as Success.")
    else:
        await message.reply("No pending step requiring /success.")

@router.message(Command("failed"))
async def failed_handler(message: Message):
    global user_intervention_result
    if is_processing and not user_intervention_event.is_set():
        user_intervention_result = "failed"
        user_intervention_event.set()
        await message.reply("❌ Marked as Failed.")
    else:
        await message.reply("No pending step requiring /success.")

@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task, device_event, device_status, user_intervention_event, user_intervention_result

    if is_processing:
        await message.reply("⚠️ Another processing session is already running. Use /cancel to stop it first.")
        return

    text = message.text
    numbers = [n.strip() for n in text.replace(",", "\n").split() if n.strip()]

    if not numbers:
        await message.reply("No valid numbers found in your message.")
        return

    is_processing = True
    current_task = asyncio.current_task()

    try:
        total = len(numbers)
        status_msg = await message.answer(f"Processing {total} numbers")

        for i, num in enumerate(numbers, start=1):
            await status_msg.edit_text(f"Processing {total} numbers\n\n{i}/{total}")

            # Reset device event for this iteration
            device_event.clear()
            device_status = None

            # 1. Trigger MacroDroid to check number
            success = await call_device_api(num, payload_type="check")
            if not success:
                await message.answer(f"{num} 🔴 Error (Webhook failed)")
                continue

            await message.answer(f"{num} ⏳ Checking on device...")

            # 2. Wait up to 3 minutes (180 seconds) for the device to call back
            try:
                await asyncio.wait_for(device_event.wait(), timeout=180)
            except asyncio.TimeoutError:
                await message.answer(f"{num} 🟠 Ended (Device timeout)")
                continue

            res = (device_status or "").lower()

            if "blocked" in res:
                await message.answer(f"{num} ⚫ Bad")
                continue

            elif "try_again" in res:
                await message.answer(f"{num} 🟡 in 5s")
                await asyncio.sleep(5)
                continue

            elif "error" in res:
                await message.answer(f"{num} 🔴 Error Code")
                continue

            elif "request_code" in res:
                await message.answer(f"{num} ⚪ Code")
                
                code = f"{random.randint(0, 999999):06d}"
                device_event.clear()
                device_status = None

                # Send code to MacroDroid
                await call_device_api(num, payload_type="send_code", code=code)

                # Wait up to 3 minutes for device confirmation of code submission
                try:
                    await asyncio.wait_for(device_event.wait(), timeout=180)
                    res = (device_status or "").lower()
                    if "blocked" in res:
                        await message.answer(f"{num} ⚫ Bad")
                        continue
                except asyncio.TimeoutError:
                    pass  # Proceed to manual window if device doesn't send a final response

            # 3. Enter 3-minute manual intervention window (/success or /failed)
            await message.answer(f"{num} 🟤 Waiting for your decision (/success or /failed)")
            
            user_intervention_event.clear()
            user_intervention_result = None
            
            try:
                await asyncio.wait_for(user_intervention_event.wait(), timeout=180)
                
                if user_intervention_result == "success":
                    await message.answer(f"{num} 🟢 Success")
                elif user_intervention_result == "failed":
                    await message.answer(f"{num} 🟠 Failed")
            except asyncio.TimeoutError:
                await message.answer(f"{num} 🟠 Ended (Time expired)")

        await message.answer(f"Processing Completed\n\n{total} number processed")
        await status_msg.edit_text(f"Processed {total} numbers\n\n{total}/{total}")

    except asyncio.CancelledError:
        logger.info("Task was cancelled by user request.")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        await message.answer(f"❌ An error occurred during processing: {str(e)}")
    finally:
        is_processing = False
        current_task = None

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

    # Start the web server concurrently
    await start_web_server()
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully. Polling for messages...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
                
