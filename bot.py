import asyncio
import logging
import random
import aiohttp
import phonenumbers
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN_HERE"
# Replace with your actual MacroDroid Webhook URL (or local device IP / Ngrok tunnel)
MACRODROID_WEBHOOK_URL = "http://trigger.macrodroid.com/YOUR_UNIQUE_DEVICE_UUID/telegram_bot"
SECONDARY_BOT_WEBHOOK_URL = "https://your-secondary-bot-webhook-url.com/api"

router = Router()

# Global state to ensure only one processing session runs at a time
is_processing = False
current_task: asyncio.Task | None = None

def parse_phone_number(raw_number: str):
    """
    Takes a raw number string, adds a '+' if missing, and splits 
    it into (country_code, national_number) using phonenumbers[span_1](start_span)[span_1](end_span).
    """
    cleaned = raw_number.strip()
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned
        
    try:
        parsed = phonenumbers.parse(cleaned, None)
        if phonenumbers.is_valid_number(parsed):
            country_code = str(parsed.country_code)
            national_number = str(parsed.national_number)
            return country_code, national_number
    except Exception as e:
        logger.warning(f"Failed to parse {raw_number} using phonenumbers: {e}")
        
    # Fallback heuristic if standard parsing fails
    if cleaned.startswith("+234") or cleaned.startswith("+233"):
        return cleaned[1:4], cleaned[4:]
    elif cleaned.startswith("+44"):
        return cleaned[1:3], cleaned[3:]
    
    return cleaned[1:4], cleaned[4:]

async def call_device_api(raw_number: str, payload_type: str = "check", code: str | None = None):
    """
    Separates the country code from the number and sends them via GET query parameters to MacroDroid.
    """
    country_code, national_number = parse_phone_number(raw_number)
    
    url = (
        f"{MACRODROID_WEBHOOK_URL}"
        f"?country_code={country_code}&national_number={national_number}&type={payload_type}"
    )
    if code:
        url += f"&code={code}"

    logger.info(f"Triggering MacroDroid -> Country Code: +{country_code}, Number: {national_number}, Type: {payload_type}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                logger.info(f"MacroDroid webhook triggered, status: {response.status}")
                # If MacroDroid returns a response body (like 'blocked', 'try_again', etc.), you can parse it here.
                # For now, we fallback to a simulated response or placeholder check.
    except Exception as e:
        logger.error(f"Failed to reach MacroDroid webhook: {e}")

    # Simulated delay for device action execution
    await asyncio.sleep(2)
    
    # Mock return for testing logic flow (Replace with actual response from MacroDroid if returned)
    return random.choice(["blocked", "try_again_5s", "request_code"])

async def call_secondary_bot_api(number: str, code: str):
    """Sends number and code to the secondary bot API/webhook."""
    logger.info(f"Sending to secondary bot: Number={number}, Code={code}")
    await asyncio.sleep(1)
    return random.choice(["success", "failed"])

@router.message(Command("cancel"))
async def cancel_handler(message: Message):
    global is_processing, current_task
    if is_processing and current_task:
        current_task.cancel()
        is_processing = False
        await message.reply("❌ Processing cancelled successfully.")
    else:
        await message.reply("No active processing session to cancel.")

@router.message(F.text & ~F.text.startswith("/"))
async def handle_numbers(message: Message):
    global is_processing, current_task

    if is_processing:
        await message.reply("⚠️ Another processing session is already running. Use /cancel to stop it first.")
        return

    text = message.text
    # Split numbers by newline, comma, or spaces
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
            # Update live tracking on the initial message
            await status_msg.edit_text(f"Processing {total} numbers\n\n{i}/{total}")

            # 1. Send number to device via MacroDroid Webhook
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
                
                # Generate random 6-digit code
                code = f"{random.randint(0, 999999):06d}"
                
                # Send code back to device
                await call_device_api(num, payload_type="send_code", code=code)

                # Start 3-minute wait timer (180 seconds)
                wait_start = asyncio.get_event_loop().time()
                timeout = 180
                code_handled = False

                while (asyncio.get_event_loop().time() - wait_start) < timeout:
                    await asyncio.sleep(5) # Poll interval
                    
                    sub_res = random.choice(["continue", "error", "blocked", "none"])

                    if sub_res == "blocked":
                        await message.answer(f"{num} ⚫ Bad")
                        code_handled = True
                        break
                    
                    elif sub_res == "continue":
                        await message.answer(f"{num} 🟤 Waiting")
                        
                        secondary_res = await call_secondary_bot_api(num, code)
                        
                        if secondary_res == "success":
                            await message.answer(f"{num} 🟢 Success")
                        else:
                            await message.answer(f"{num} 🟠 Failed")
                        
                        code_handled = True
                        break

                    elif sub_res == "error":
                        await message.answer(f"{num} 🔴 Error Code")
                        continue

                if not code_handled:
                    # Timeout reached
                    await message.answer(f"{num} 🟠 Ended")

        # Completion summary
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

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    logger.info("Bot started successfully. Polling for messages...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())