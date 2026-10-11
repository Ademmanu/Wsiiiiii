import asyncio
import logging
import os
import random
import aiohttp
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

router = Router()

# Global state management
is_processing = False
current_task: asyncio.Task | None = None

# Intervention state for human /success or /failed commands
user_intervention_event = asyncio.Event()
user_intervention_result: str | None = None  # Stores 'success' or 'failed'

def parse_phone_number(raw_number: str):
    """
    Splits raw number into (country_code, national_number).
    """
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
    """
    Sends country_code and national_number separately to MacroDroid via GET query params.
    """
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
    except Exception as e:
        logger.error(f"Failed to reach MacroDroid webhook: {e}")

    await asyncio.sleep(2)
    # Simulated response from device (Replace with actual device callback logic)
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
                await call_device_api(num, payload_type="send_code", code=code)

                # Start overall 3-minute wait timer (180 seconds)
                wait_start = asyncio.get_event_loop().time()
                timeout = 180
                code_handled = False

                while (asyncio.get_event_loop().time() - wait_start) < timeout:
                    await asyncio.sleep(3)
                    
                    # Simulated check for device feedback
                    sub_res = random.choice(["continue", "error", "blocked", "none"])

                    if sub_res == "blocked":
                        await message.answer(f"{num} ⚫ Bad")
                        code_handled = True
                        break
                    
                    elif sub_res == "continue":
                        await message.answer(f"{num} 🟤 Waiting")
                        
                        # Calculate remaining timeout window for human intervention
                        elapsed = asyncio.get_event_loop().time() - wait_start
                        remaining_time = max(0.1, timeout - elapsed)
                        
                        # Reset event & result listener
                        user_intervention_event.clear()
                        user_intervention_result = None
                        
                        try:
                            # Wait for user to send /success or /failed before time runs out
                            await asyncio.wait_for(user_intervention_event.wait(), timeout=remaining_time)
                            
                            if user_intervention_result == "success":
                                await message.answer(f"{num} 🟢 Success")
                            elif user_intervention_result == "failed":
                                await message.answer(f"{num} 🟠 Failed")
                                
                        except asyncio.TimeoutError:
                            # User didn't reply in time
                            await message.answer(f"{num} 🟠 Ended")
                            
                        code_handled = True
                        break

                    elif sub_res == "error":
                        await message.answer(f"{num} 🔴 Error Code")
                        continue

                if not code_handled:
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
