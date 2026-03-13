# -*- coding: utf-8 -*-
"""
=============================================================
  DERDO BOT  |  95 Don Chechnya  |  DERD  |  DOED
  ДердоBOT   |  95 Дон Чечня    |  APT BURGER  |  ДЕРД  |  ДОЕД
                    by BerushaGMD
=============================================================
"""

import asyncio
import logging
import random
import re
import os
from collections import defaultdict
from datetime import datetime

import sqlite3
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BotCommand

# ------------------------------------------------------------------------------
#  КОНФИГ
# ------------------------------------------------------------------------------

BOT_TOKEN   = os.getenv("BOT_TOKEN", "ВАШ_ТОКЕН_ЗДЕСЬ")
DB_PATH     = "derdo_messages.db"
MAX_MESSAGES = 5000
RANDOM_REPLY_CHANCE = 0.10   # 10 %

# ------------------------------------------------------------------------------
#  ЛОГГЕР
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ДердоBOT")

# ------------------------------------------------------------------------------
#  БАЗА ДАННЫХ
# ------------------------------------------------------------------------------

def _init_db_sync() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            text       TEXT    NOT NULL,
            created_at TEXT    NOT NULL
        )
    """)
    con.commit()
    con.close()

async def init_db() -> None:
    await asyncio.to_thread(_init_db_sync)
    logger.info("БД инициализирована -> %s", DB_PATH)


def _save_message_sync(user_id: int, text: str) -> None:
    con = sqlite3.connect(DB_PATH)
    (count,) = con.execute("SELECT COUNT(*) FROM messages").fetchone()
    if count >= MAX_MESSAGES:
        con.execute("DELETE FROM messages WHERE id = (SELECT MIN(id) FROM messages)")
        logger.debug("FIFO: удалено старейшее сообщение (было %d)", count)
    con.execute(
        "INSERT INTO messages (user_id, text, created_at) VALUES (?, ?, ?)",
        (user_id, text, datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()

async def save_message(user_id: int, text: str) -> None:
    await asyncio.to_thread(_save_message_sync, user_id, text)


def _get_all_texts_sync() -> list:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT text FROM messages").fetchall()
    con.close()
    return [row[0] for row in rows]

async def get_all_texts() -> list:
    return await asyncio.to_thread(_get_all_texts_sync)


def _get_random_message_sync():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT text FROM messages ORDER BY RANDOM() LIMIT 1").fetchone()
    con.close()
    return row[0] if row else None

async def get_random_message():
    return await asyncio.to_thread(_get_random_message_sync)


def _count_messages_sync() -> int:
    con = sqlite3.connect(DB_PATH)
    (n,) = con.execute("SELECT COUNT(*) FROM messages").fetchone()
    con.close()
    return n

async def count_messages() -> int:
    return await asyncio.to_thread(_count_messages_sync)

# ------------------------------------------------------------------------------
#  МАРКОВСКИЕ ЦЕПИ
# ------------------------------------------------------------------------------

def tokenize(text: str) -> list[str]:
    """Разбивает текст на слова, игнорируя пунктуацию."""
    return re.findall(r"[А-Яа-яЁёA-Za-z0-9]+", text)


def build_markov(texts: list[str], order: int = 2) -> dict[tuple, list[str]]:
    """Строит марковскую цепь из списка текстов (order-граммы)."""
    chain: dict[tuple, list[str]] = defaultdict(list)
    for text in texts:
        words = tokenize(text)
        if len(words) < order + 1:
            continue
        for i in range(len(words) - order):
            key = tuple(words[i : i + order])
            chain[key].append(words[i + order])
    return chain


def generate_markov(chain: dict[tuple, list[str]],
                    seed: list[str] | None = None,
                    max_words: int = 25,
                    order: int = 2) -> str:
    """
    Генерирует фразу по марковской цепи.
    seed - начальные слова (для /continue); если None - случайный старт.
    """
    if not chain:
        return ""

    keys = list(chain.keys())

    if seed and len(seed) >= order:
        # Ищем ключ, совпадающий с концом seed
        start_key = tuple(seed[-order:])
        if start_key not in chain:
            # Ищем похожие ключи (первое слово совпадает)
            candidates = [k for k in keys if k[0].lower() == seed[-order].lower()]
            start_key = random.choice(candidates) if candidates else random.choice(keys)
    else:
        start_key = random.choice(keys)

    result: list[str] = list(start_key)

    for _ in range(max_words):
        key = tuple(result[-order:])
        if key not in chain:
            break
        next_word = random.choice(chain[key])
        result.append(next_word)

    return " ".join(result)

# ------------------------------------------------------------------------------
#  БОТ
# ------------------------------------------------------------------------------

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

BRAND = "🔥 *95 Дон Чечня*"


# -- /start -----------------------------------------------------------------

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    text = (
        "🕯️ *ДердоBOT* активирован ле!\n\n"
        "Читаю все сообщения, запоминаю, говорю.\n\n"
        "⚡⚡ *Команды:*\n"
        "  `/generate` - сгенерировать какую-то случайную фразу\n"
        "  `/continue <текст>` - продолжить твою фразу про валеру\n"
        "  `/stats` - статистика БД"
    )
    await msg.answer(text, parse_mode="Markdown")


# -- /stats -----------------------------------------------------------------

@dp.message(Command("stats"))
async def cmd_stats(msg: Message) -> None:
    n = await count_messages()
    await msg.answer(
        f"📊 *Статистика ДердоBOTа*\n\n"
        f"💾 Сообщений от дебилов: *{n}* / {MAX_MESSAGES}\n"
        f"🧠 Заполнено мозга: *{n / MAX_MESSAGES * 100:.1f}%*",
        parse_mode="Markdown",
    )


# -- /generate --------------------------------------------------------------

@dp.message(Command("generate"))
async def cmd_generate(msg: Message) -> None:
    texts = await get_all_texts()

    if len(texts) < 5:
        await msg.answer(
            "⚠️ лее маловато ещо данных для генерации. Пишите больше гении",
            parse_mode="Markdown",
        )
        return

    chain = build_markov(texts)

    if not chain:
        # Фолбэк: случайная склейка слов
        all_words = [w for t in texts for w in tokenize(t)]
        random.shuffle(all_words)
        phrase = " ".join(all_words[:random.randint(5, 15)])
    else:
        phrase = generate_markov(chain)

    if not phrase:
        phrase = "...95"

    await msg.answer(
        f"_{phrase}_",
        parse_mode="Markdown",
    )


# -- /continue --------------------------------------------------------------

@dp.message(Command("continue"))
async def cmd_continue(msg: Message) -> None:
    # Извлекаем аргумент после /continue
    raw = msg.text or ""
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "✏️ Использование: `/continue <твой текст>`\n"
            "Пример: `/continue 95 дон чечня это`",
            parse_mode="Markdown",
        )
        return

    seed_text = parts[1].strip()
    seed_words = tokenize(seed_text)

    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer(
            "⚠️ лее еще мало данных, дай боту накопить больше высеров с чата",
            parse_mode="Markdown",
        )
        return

    chain = build_markov(texts)
    continuation = generate_markov(chain, seed=seed_words)

    if not continuation:
        continuation = seed_text + " ...95"

    await msg.answer(
        f"_{continuation}_",
        parse_mode="Markdown",
    )


# -- Все остальные текстовые сообщения --------------------------------------

@dp.message(F.text)
async def handle_text(msg: Message) -> None:
    text = msg.text
    user_id = msg.from_user.id if msg.from_user else 0

    # 1. Сохраняем в БД (лимит: максимум 40 символов)
    if len(text) > 40:
        logger.debug("Пропущено (длина %d > 40): %.40s", len(text), text)
        return
    await save_message(user_id, text)
    logger.info("Сохранено от user_id=%d: %s", user_id, text)

    # 2. С вероятностью 10% - отвечаем случайным сообщением из БД
    if random.random() < RANDOM_REPLY_CHANCE:
        random_msg = await get_random_message()
        if random_msg:
            await msg.reply(
                f"_{random_msg}_",
                parse_mode="Markdown",
            )
            logger.info("Случайный ответ отправлен (10%% шанс)")


# ------------------------------------------------------------------------------
#  ЗАПУСК
# ------------------------------------------------------------------------------

async def main() -> None:
    logger.info("=" * 60)
    logger.info("  ДердоBOT запускается - 95 Дон Чечня | ДЭРД | ДЕРД | ДОЕД")
    logger.info("  by BerushaGMD")
    logger.info("=" * 60)

    await init_db()

    # Регистрируем команды - Telegram покажет подсказки при наборе "/"
    await bot.set_my_commands([
        BotCommand(command="start",    description="Запустить бота / список команд"),
        BotCommand(command="generate", description="Сгенерировать случайную фразу"),
        BotCommand(command="continue", description="Продолжить фразу: /continue <текст>"),
        BotCommand(command="stats",    description="Статистика базы данных"),
    ])
    logger.info("Команды зарегистрированы в Telegram")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
