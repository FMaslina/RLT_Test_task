import asyncio
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from dotenv import load_dotenv

from aggregation_algorithm import aggregate

load_dotenv()

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()


@dp.message(Command("start"))
async def start_command(message: types.Message):
    await message.answer("Приветствую! Отправьте мне JSON, чтобы получить данные")


@dp.message(F.text)
async def send_aggregated_data(message: types.Message):
    aggregated_data = await aggregate(message.text)
    if aggregated_data == 0:
        await message.reply("Некорректный ввод")
    else:
        await message.reply(str(aggregated_data))

dp.message.register(send_aggregated_data, F.text)


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
