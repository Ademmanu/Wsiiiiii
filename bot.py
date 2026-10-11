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
# Render injects PORT dynamically (defaults to 10000)
PORT = int(os.getenv("PORT", 10000))

router = Router()

# Global state management
is_processing = False
current_task: asyncio.Task | None = None

# Intervention state for human /success or /failed commands
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
    """Sends country_code and national_number to MacroDroid."""
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
                logger.info(f"MacroDroid webhook status: {response.status}")
    except Exception as e:
        logger.error(f"Failed to reach MacroDroid webhook: {e}")

    await asyncio.sleep(2)
    return random.choice(["blocked", "try_again_5s", "request_code"])

@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global is_processing, current_task
    if is_processing and current_task:
        current_task.cancel()
        is_processing = False
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
        await message.reply("No pending step requiring /failed.")

@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task, user_intervention_event, user_intervention_result

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

            device_res = await call_device_api(num, payload_type="check")

            if device_res == "blocked":
                await message.answer(f"{num} ⚫ Bad")
                continue

            elif "try_again" in device_res:
                await message.answer(f"{num} 🟡 in 5s")
                await asyncio.sleep(5)
                continue

            elif device_res == "request_code":
                await message.answer(f"{num} ⚪ Code")
                
                code = f"{random.randint(0, 999999):06d}"
                await call_device_api(num, payload_type="send_code", code=code)

                wait_start = asyncio.get_event_loop().time()
                timeout = 180
                code_handled = False

                while (asyncio.get_event_loop().time() - wait_start) < timeout:
                    await asyncio.sleep(3)
                    
                    sub_res = random.choice(["continue", "error", "blocked", "none"])

                    if sub_res == "blocked":
                        await message.answer(f"{num} ⚫ Bad")
                        code_handled = True
                        break
                    
                    elif sub_res == "continue":
                        await message.answer(f"{num} 🟤 Waiting")
                        
                        elapsed = asyncio.get_event_loop().time() - wait_start
                        remaining_time = max(0.1, timeout - elapsed)
                        
                        user_intervention_event.clear()
                        user_intervention_result = None
                        
                        try:
                            await asyncio.wait_for(user_intervention_event.wait(), timeout=remaining_time)
                            
                            if user_intervention_result == "success":
                                await message.answer(f"{num} 🟢 Success")
                            elif user_intervention_result == "failed":
                                await message.answer(f"{num} 🟠 Failed")
                                
                        except asyncio.TimeoutError:
                            await message.answer(f"{num} 🟠 Ended")
                            
                        code_handled = True
                        break

                    elif sub_res == "error":
                        await message.answer(f"{num} 🔴 Error Code")
                        continue

                if not code_handled:
                    await message.answer(f"{num} 🟠 Ended")

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

# --- DUMMY WEB SERVER FOR RENDER HEALTH CHECKS ---
async def handle_health_check(request):
    return web.Response(text="Bot is running live!")

async def start_dummy_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/healthz", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Dummy HTTP Server running on port {PORT} for Render health scanner.")

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    # Start dummy web server concurrently with polling
    await start_dummy_server()

    # Clear lingering updates before starting to avoid conflicts
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Bot started successfully. Polling for messages...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
