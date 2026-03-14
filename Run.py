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
from datetime import datetime, timezone

import sqlite3
import subprocess
import sys

# Авто-установка aiogram если не установлен
try:
    import aiogram
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiogram==3.13.1"])

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BotCommand

# ------------------------------------------------------------------------------
#  КОНФИГ
# ------------------------------------------------------------------------------

BOT_TOKEN            = os.getenv("BOT_TOKEN", "8605622348:AAG03XrlvdcLGJBiHzbet5bXHyQr_4CV54E")
DB_PATH              = "derdo_messages.db"
MAX_MESSAGES         = 360000
RANDOM_REPLY_CHANCE  = 0.10          # 10 %
FARM_COOLDOWN_SEC    = 30 * 60       # 30 минут

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
    # Таблица сообщений
    con.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            text       TEXT    NOT NULL,
            created_at TEXT    NOT NULL
        )
    """)
    # Таблица Доедиков
    con.execute("""
        CREATE TABLE IF NOT EXISTS doediki (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT    NOT NULL DEFAULT '',
            balance    INTEGER NOT NULL DEFAULT 0,
            last_farm  TEXT    NOT NULL DEFAULT ''
        )
    """)
    con.commit()
    con.close()

async def init_db() -> None:
    await asyncio.to_thread(_init_db_sync)
    logger.info("БД инициализирована -> %s", DB_PATH)


# ── messages ────────────────────────────────────────────────────────────────

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


# ── doediki ─────────────────────────────────────────────────────────────────

def _farm_sync(user_id: int, username: str) -> dict:
    """
    Пытается сфармить Доедики.
    Возвращает dict:
        ok        bool  - успех / кулдаун
        earned    int   - сколько заработано (0 если кулдаун)
        balance   int   - новый баланс
        remaining int   - секунд до следующего фарма (если кулдаун)
        jackpot   bool  - был ли джекпот
    """
    con = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc)

    row = con.execute(
        "SELECT balance, last_farm FROM doediki WHERE user_id = ?", (user_id,)
    ).fetchone()

    if row:
        balance, last_farm_str = row
        if last_farm_str:
            last_farm = datetime.fromisoformat(last_farm_str)
            if last_farm.tzinfo is None:
                last_farm = last_farm.replace(tzinfo=timezone.utc)
            elapsed = (now - last_farm).total_seconds()
            if elapsed < FARM_COOLDOWN_SEC:
                con.close()
                return {
                    "ok": False,
                    "earned": 0,
                    "balance": balance,
                    "remaining": int(FARM_COOLDOWN_SEC - elapsed),
                    "jackpot": False,
                }
    else:
        balance = 0

    # Джекпот 5% -> 50-100 Доедиков, обычно 1-15
    jackpot = random.random() < 0.05
    earned  = random.randint(50, 100) if jackpot else random.randint(1, 15)
    balance += earned

    con.execute("""
        INSERT INTO doediki (user_id, username, balance, last_farm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username  = excluded.username,
            balance   = excluded.balance,
            last_farm = excluded.last_farm
    """, (user_id, username, balance, now.isoformat()))
    con.commit()
    con.close()

    return {"ok": True, "earned": earned, "balance": balance, "remaining": 0, "jackpot": jackpot}

async def farm(user_id: int, username: str) -> dict:
    return await asyncio.to_thread(_farm_sync, user_id, username)


def _get_balance_sync(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT balance FROM doediki WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else 0

async def get_balance(user_id: int) -> int:
    return await asyncio.to_thread(_get_balance_sync, user_id)


def _get_leaderboard_sync(limit: int = 10) -> list:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT username, balance FROM doediki ORDER BY balance DESC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    return rows

async def get_leaderboard(limit: int = 10) -> list:
    return await asyncio.to_thread(_get_leaderboard_sync, limit)

# ------------------------------------------------------------------------------
#  МАРКОВСКИЕ ЦЕПИ
# ------------------------------------------------------------------------------

def tokenize(text: str) -> list:
    return re.findall(r"[А-Яа-яЁёA-Za-z0-9]+", text)


def build_markov(texts: list, order: int = 2) -> dict:
    chain = defaultdict(list)
    for text in texts:
        words = tokenize(text)
        if len(words) < order + 1:
            continue
        for i in range(len(words) - order):
            key = tuple(words[i : i + order])
            chain[key].append(words[i + order])
    return chain


def generate_markov_free(chain: dict, max_words: int = 25, order: int = 2) -> str:
    """Свободная генерация для /generate."""
    if not chain:
        return ""
    result = list(random.choice(list(chain.keys())))
    for _ in range(max_words):
        key = tuple(result[-order:])
        if key not in chain:
            break
        result.append(random.choice(chain[key]))
    return " ".join(result)


def generate_continuation(chain: dict, seed: list, max_words: int = 20, order: int = 2) -> str:
    """
    Возвращает ТОЛЬКО новые слова-продолжение (без seed).
    Используется в /continue для раздельного форматирования.
    """
    if not chain or len(seed) < order:
        return ""

    keys = list(chain.keys())
    start_key = tuple(seed[-order:])

    if start_key not in chain:
        candidates = [k for k in keys if k[0].lower() == seed[-1].lower()]
        start_key = random.choice(candidates) if candidates else random.choice(keys)

    working = list(start_key)
    for _ in range(max_words):
        key = tuple(working[-order:])
        if key not in chain:
            break
        working.append(random.choice(chain[key]))

    new_words = working[order:]
    return " ".join(new_words)


# ------------------------------------------------------------------------------
#  БОТ
# ------------------------------------------------------------------------------

bot = Bot(token="8605622348:AAG03XrlvdcLGJBiHzbet5bXHyQr_4CV54E")
dp  = Dispatcher()


# ── /start ──────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    text = (
        "⚡⚡ *ДердоBOT* активирован!\n\n"
        "Читаю все высеры, думаю, высераю.\n\n"
        "🕯️ *Команды:*\n"
        "  /generate - сгенерировать случайный высер\n"
        "  /continue - продолжить твою фразу\n"
        "  /farm - фармить в рабство Доедиков 🟦 (кд 30 мин)\n"
        "  /doediki - мой баланс Доедиков\n"
        "  /topdoed - топ фармеров\n"
        "  /stats - статистика мозга"
    )
    await msg.answer(text, parse_mode="Markdown")


# ── /stats ───────────────────────────────────────────────────────────────────

@dp.message(Command("stats"))
async def cmd_stats(msg: Message) -> None:
    n = await count_messages()
    await msg.answer(
        f"📊 *Статистика ДердоBOTа*\n\n"
        f"💾 Сообщений в мозге: *{n}* / {MAX_MESSAGES}\n"
        f"😎 Заполнено: *{n / MAX_MESSAGES * 100:.1f}%*",
        parse_mode="Markdown",
    )


# ── /generate ────────────────────────────────────────────────────────────────

@dp.message(Command("generate"))
async def cmd_generate(msg: Message) -> None:
    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ леее маловато данных для высеров!!!")
        return

    chain = build_markov(texts)
    if not chain:
        all_words = [w for t in texts for w in tokenize(t)]
        random.shuffle(all_words)
        phrase = " ".join(all_words[:random.randint(5, 15)])
    else:
        phrase = generate_markov_free(chain)

    await msg.answer(f"_{phrase or '...95'}_", parse_mode="Markdown")


# ── /continue ────────────────────────────────────────────────────────────────

@dp.message(Command("continue"))
async def cmd_continue(msg: Message) -> None:
    raw   = msg.text or ""
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "✏️ Использование: `/continue <твой текст>`\n"
            "Пример: `/continue 95 дон чечня это`",
            parse_mode="Markdown",
        )
        return

    seed_text  = parts[1].strip()
    seed_words = tokenize(seed_text)

    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ Ещё мало высеров брат")
        return

    chain        = build_markov(texts)
    continuation = generate_continuation(chain, seed_words) if chain else ""

    if not continuation:
        continuation = "...95."

    # Форматирование: "фраза" + "продолжение"
    # seed - жирный, продолжение - курсив, между ними "+"
    await msg.answer(
        f"*{seed_text}* + _{continuation}_",
        parse_mode="Markdown",
    )


# ── /farm ────────────────────────────────────────────────────────────────────

@dp.message(Command("farm"))
async def cmd_farm(msg: Message) -> None:
    user     = msg.from_user
    user_id  = user.id if user else 0
    username = (user.username or user.full_name or str(user_id)) if user else str(user_id)

    result = await farm(user_id, username)

    if not result["ok"]:
        mins = result["remaining"] // 60
        secs = result["remaining"] % 60
        await msg.answer(
            f"⏳ Подожди ле! До следующего фарма: *{mins}м {secs}с*\n\n"
            f"Доедиков в рабстве: *{result['balance']}* 🟦",
            parse_mode="Markdown",
        )
        return

    jackpot_line = "⚡ *ДЖЕКПОТ ДОЕДИКОВ 95!!!*\n\n" if result["jackpot"] else ""
    await msg.answer(
        f"{jackpot_line}"
        f"Собрано Доедиков: *{result['earned']}* 🟦\n"
        f"Доедиков в рабстве: *{result['balance']}* 🟦",
        parse_mode="Markdown",
    )
    logger.info("Фарм: user_id=%d earned=%d jackpot=%s", user_id, result["earned"], result["jackpot"])


# ── /doediki ─────────────────────────────────────────────────────────────────

@dp.message(Command("doediki"))
async def cmd_doediki(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    balance = await get_balance(user_id)
    name    = (msg.from_user.username or msg.from_user.full_name) if msg.from_user else "Анон"
    await msg.answer(
        f"💼 *{name}*, твой баланс:\n*{balance}* 🟦 Доедиков",
        parse_mode="Markdown",
    )


# ── /topdoed ─────────────────────────────────────────────────────────────────

@dp.message(Command("topdoed"))
async def cmd_topdoed(msg: Message) -> None:
    rows = await get_leaderboard(10)
    if not rows:
        await msg.answer(
            "📭 Никто ещё не фармил Доедики. Начни первым - /farm",
            parse_mode="Markdown",
        )
        return

    medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
    lines  = ["🏆 *Топ фармеров Доедиков* 🟦\n"]
    for i, (username, balance) in enumerate(rows):
        lines.append(f"{medals[i]} *{username}* - {balance} 🟦")

    await msg.answer("\n".join(lines), parse_mode="Markdown")


# ── Все остальные текстовые сообщения ────────────────────────────────────────

@dp.message(F.text)
async def handle_text(msg: Message) -> None:
    text    = msg.text
    user_id = msg.from_user.id if msg.from_user else 0

    if len(text) > 40:
        logger.debug("Пропущено (длина %d > 40): %.40s", len(text), text)
        return

    await save_message(user_id, text)
    logger.info("Сохранено от user_id=%d: %s", user_id, text)

    if random.random() < RANDOM_REPLY_CHANCE:
        random_msg = await get_random_message()
        if random_msg:
            await msg.reply(f"_{random_msg}_", parse_mode="Markdown")
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

    await bot.set_my_commands([
        BotCommand(command="start",    description="Запустить бота / список команд"),
        BotCommand(command="generate", description="Сгенерировать случайную фразу"),
        BotCommand(command="continue", description="Продолжить фразу: /continue <текст>"),
        BotCommand(command="farm",     description="Сфармить Доедики 🟦 (кд 30 мин)"),
        BotCommand(command="doediki",  description="Мой баланс Доедиков"),
        BotCommand(command="topdoed",  description="Топ фармеров Доедиков"),
        BotCommand(command="stats",    description="Статистика базы данных"),
    ])
    logger.info("Команды зарегистрированы в Telegram")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ==============================================================================
#  ПРЕДЛОЖЕНИЯ ПО ФУНКЦИЯМ (НЕ ДОБАВЛЕНЫ)
# ==============================================================================
#
#  1. /top - топ самых частых слов в БД
#     Counter(tokenize(text) for text in texts), вывести топ-10 слов.
#
#  2. /who <слово> - кто чаще всего пишет это слово
#     GROUP BY user_id WHERE text LIKE '%слово%'.
#
#  3. /duel @user1 @user2 - битва стилей
#     Две отдельные цепи, одна фраза от каждого, голосование InlineKeyboard.
#
#  4. /forget - удалить свои сообщения из БД
#     DELETE FROM messages WHERE user_id = ?
#
#  5. Авто-постинг /generate раз в N часов
#     asyncio.create_task + sleep(N*3600), постит в чат по chat_id.
#
#  6. Динамический order для марков
#     order=3 если len(texts)>500 иначе order=2 - фразы связнее.
#
#  7. Магазин Доедиков /shop
#     Трать 🟦 на роли/стикеры/фичи. Таблица items, команда /buy <item_id>.
#
#  8. /daily - ежедневный бонус, отдельный кулдаун 24ч.
#
# ==============================================================================    return [row[0] for row in rows]

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


# ── doediki ─────────────────────────────────────────────────────────────────

def _farm_sync(user_id: int, username: str) -> dict:
    """
    Пытается сфармить Доедики.
    Возвращает dict:
        ok        bool  - успех / кулдаун
        earned    int   - сколько заработано (0 если кулдаун)
        balance   int   - новый баланс
        remaining int   - секунд до следующего фарма (если кулдаун)
        jackpot   bool  - был ли джекпот
    """
    con = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc)

    row = con.execute(
        "SELECT balance, last_farm FROM doediki WHERE user_id = ?", (user_id,)
    ).fetchone()

    if row:
        balance, last_farm_str = row
        if last_farm_str:
            last_farm = datetime.fromisoformat(last_farm_str)
            if last_farm.tzinfo is None:
                last_farm = last_farm.replace(tzinfo=timezone.utc)
            elapsed = (now - last_farm).total_seconds()
            if elapsed < FARM_COOLDOWN_SEC:
                con.close()
                return {
                    "ok": False,
                    "earned": 0,
                    "balance": balance,
                    "remaining": int(FARM_COOLDOWN_SEC - elapsed),
                    "jackpot": False,
                }
    else:
        balance = 0

    # Джекпот 5% -> 50-100 Доедиков, обычно 1-15
    jackpot = random.random() < 0.05
    earned  = random.randint(50, 100) if jackpot else random.randint(1, 15)
    balance += earned

    con.execute("""
        INSERT INTO doediki (user_id, username, balance, last_farm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username  = excluded.username,
            balance   = excluded.balance,
            last_farm = excluded.last_farm
    """, (user_id, username, balance, now.isoformat()))
    con.commit()
    con.close()

    return {"ok": True, "earned": earned, "balance": balance, "remaining": 0, "jackpot": jackpot}

async def farm(user_id: int, username: str) -> dict:
    return await asyncio.to_thread(_farm_sync, user_id, username)


def _get_balance_sync(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT balance FROM doediki WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else 0

async def get_balance(user_id: int) -> int:
    return await asyncio.to_thread(_get_balance_sync, user_id)


def _get_leaderboard_sync(limit: int = 10) -> list:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT username, balance FROM doediki ORDER BY balance DESC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    return rows

async def get_leaderboard(limit: int = 10) -> list:
    return await asyncio.to_thread(_get_leaderboard_sync, limit)


# ------------------------------------------------------------------------------
#  МАРКОВСКИЕ ЦЕПИ
# ------------------------------------------------------------------------------

def tokenize(text: str) -> list:
    return re.findall(r"[А-Яа-яЁёA-Za-z0-9]+", text)


def build_markov(texts: list, order: int = 2) -> dict:
    chain = defaultdict(list)
    for text in texts:
        words = tokenize(text)
        if len(words) < order + 1:
            continue
        for i in range(len(words) - order):
            key = tuple(words[i : i + order])
            chain[key].append(words[i + order])
    return chain


def generate_markov_free(chain: dict, max_words: int = 25, order: int = 2) -> str:
    """Свободная генерация для /generate."""
    if not chain:
        return ""
    result = list(random.choice(list(chain.keys())))
    for _ in range(max_words):
        key = tuple(result[-order:])
        if key not in chain:
            break
        result.append(random.choice(chain[key]))
    return " ".join(result)


def generate_continuation(chain: dict, seed: list, max_words: int = 20, order: int = 2) -> str:
    """
    Возвращает ТОЛЬКО новые слова-продолжение (без seed).
    Используется в /continue для раздельного форматирования.
    """
    if not chain or len(seed) < order:
        return ""

    keys = list(chain.keys())
    start_key = tuple(seed[-order:])

    if start_key not in chain:
        candidates = [k for k in keys if k[0].lower() == seed[-1].lower()]
        start_key = random.choice(candidates) if candidates else random.choice(keys)

    working = list(start_key)
    for _ in range(max_words):
        key = tuple(working[-order:])
        if key not in chain:
            break
        working.append(random.choice(chain[key]))

    new_words = working[order:]
    return " ".join(new_words)


# ------------------------------------------------------------------------------
#  БОТ
# ------------------------------------------------------------------------------

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ──────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    text = (
        "⚡⚡ *ДердоBOT* активирован!\n\n"
        "Читаю все высеры, думаю, высераю\\.\n\n"
        "🕯️ *Команды:*\n"
        "  /generate - сгенерировать случайный высер\n"
        "  /continue - продолжить твою фразу\n"
        "  /farm - фармить Доедиков 🟦 (кд 30 мин)\n"
        "  /doediki - мой баланс Доедиков\n"
        "  /topdoed - топ фармеров\n"
        "  /stats - статистика мозга"
    )
    await msg.answer(text, parse_mode="Markdown")


# ── /stats ───────────────────────────────────────────────────────────────────

@dp.message(Command("stats"))
async def cmd_stats(msg: Message) -> None:
    n = await count_messages()
    await msg.answer(
        f"📊 *Статистика ДердоBOTа*\n\n"
        f"💾 Сообщений в мозге: *{n}* / {MAX_MESSAGES}\n"
        f"😎 Заполнено: *{n / MAX_MESSAGES * 100:.1f}%*",
        parse_mode="Markdown",
    )


# ── /generate ────────────────────────────────────────────────────────────────

@dp.message(Command("generate"))
async def cmd_generate(msg: Message) -> None:
    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ леее маловато данных для высеров!!!")
        return

    chain = build_markov(texts)
    if not chain:
        all_words = [w for t in texts for w in tokenize(t)]
        random.shuffle(all_words)
        phrase = " ".join(all_words[:random.randint(5, 15)])
    else:
        phrase = generate_markov_free(chain)

    await msg.answer(f"_{phrase or '...95'}_", parse_mode="Markdown")


# ── /continue ────────────────────────────────────────────────────────────────

@dp.message(Command("continue"))
async def cmd_continue(msg: Message) -> None:
    raw   = msg.text or ""
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "✏️ Использование: `/continue <твой текст>`\n"
            "Пример: `/continue 95 дон чечня это`",
            parse_mode="Markdown",
        )
        return

    seed_text  = parts[1].strip()
    seed_words = tokenize(seed_text)

    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ Ещё мало высеров брат")
        return

    chain        = build_markov(texts)
    continuation = generate_continuation(chain, seed_words) if chain else ""

    if not continuation:
        continuation = "...95."

    # Форматирование: "фраза" + "продолжение"
    # seed - жирный, продолжение - курсив, между ними "+"
    await msg.answer(
        f"*{seed_text}* + _{continuation}_",
        parse_mode="Markdown",
    )


# ── /farm ────────────────────────────────────────────────────────────────────

@dp.message(Command("farm"))
async def cmd_farm(msg: Message) -> None:
    user     = msg.from_user
    user_id  = user.id if user else 0
    username = (user.username or user.full_name or str(user_id)) if user else str(user_id)

    result = await farm(user_id, username)

    if not result["ok"]:
        mins = result["remaining"] // 60
        secs = result["remaining"] % 60
        await msg.answer(
            f"⏳ Подожди ле! До следующего фарма: *{mins}м {secs}с*\n\n"
            f"Доедиков в рабстве: *{result['balance']}* 🟦",
            parse_mode="Markdown",
        )
        return

    jackpot_line = "🎰 *ДЖЕКПОТ ДОЕДИКОВ!!!*\n\n" if result["jackpot"] else ""
    await msg.answer(
        f"{jackpot_line}"
        f"Собрано Доедиков: *{result['earned']}* 🟦\n"
        f"Доедиков в рабстве: *{result['balance']}* 🟦",
        parse_mode="Markdown",
    )
    logger.info("Фарм: user_id=%d earned=%d jackpot=%s", user_id, result["earned"], result["jackpot"])


# ── /doediki ─────────────────────────────────────────────────────────────────

@dp.message(Command("doediki"))
async def cmd_doediki(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    balance = await get_balance(user_id)
    name    = (msg.from_user.username or msg.from_user.full_name) if msg.from_user else "Анон"
    await msg.answer(
        f"💼 *{name}*, твой баланс:\n*{balance}* 🟦 Доедиков",
        parse_mode="Markdown",
    )


# ── /topdoed ─────────────────────────────────────────────────────────────────

@dp.message(Command("topdoed"))
async def cmd_topdoed(msg: Message) -> None:
    rows = await get_leaderboard(10)
    if not rows:
        await msg.answer(
            "📭 Никто ещё не фармил Доедики. Начни первым - /farm",
            parse_mode="Markdown",
        )
        return

    medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
    lines  = ["🏆 *Топ фармеров Доедиков* 🟦\n"]
    for i, (username, balance) in enumerate(rows):
        lines.append(f"{medals[i]} *{username}* - {balance} 🟦")

    await msg.answer("\n".join(lines), parse_mode="Markdown")


# ── Все остальные текстовые сообщения ────────────────────────────────────────

@dp.message(F.text)
async def handle_text(msg: Message) -> None:
    text    = msg.text
    user_id = msg.from_user.id if msg.from_user else 0

    if len(text) > 40:
        logger.debug("Пропущено (длина %d > 40): %.40s", len(text), text)
        return

    await save_message(user_id, text)
    logger.info("Сохранено от user_id=%d: %s", user_id, text)

    if random.random() < RANDOM_REPLY_CHANCE:
        random_msg = await get_random_message()
        if random_msg:
            await msg.reply(f"_{random_msg}_", parse_mode="Markdown")
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

    await bot.set_my_commands([
        BotCommand(command="start",    description="Запустить бота / список команд"),
        BotCommand(command="generate", description="Сгенерировать случайную фразу"),
        BotCommand(command="continue", description="Продолжить фразу: /continue <текст>"),
        BotCommand(command="farm",     description="Сфармить Доедики 🟦 (кд 30 мин)"),
        BotCommand(command="doediki",  description="Мой баланс Доедиков"),
        BotCommand(command="topdoed",  description="Топ фармеров Доедиков"),
        BotCommand(command="stats",    description="Статистика базы данных"),
    ])
    logger.info("Команды зарегистрированы в Telegram")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ==============================================================================
#  ПРЕДЛОЖЕНИЯ ПО ФУНКЦИЯМ (НЕ ДОБАВЛЕНЫ)
# ==============================================================================
#
#  1. /top - топ самых частых слов в БД
#     Counter(tokenize(text) for text in texts), вывести топ-10 слов.
#
#  2. /who <слово> - кто чаще всего пишет это слово
#     GROUP BY user_id WHERE text LIKE '%слово%'.
#
#  3. /duel @user1 @user2 - битва стилей
#     Две отдельные цепи, одна фраза от каждого, голосование InlineKeyboard.
#
#  4. /forget - удалить свои сообщения из БД
#     DELETE FROM messages WHERE user_id = ?
#
#  5. Авто-постинг /generate раз в N часов
#     asyncio.create_task + sleep(N*3600), постит в чат по chat_id.
#
#  6. Динамический order для марков
#     order=3 если len(texts)>500 иначе order=2 - фразы связнее.
#
#  7. Магазин Доедиков /shop
#     Трать 🟦 на роли/стикеры/фичи. Таблица items, команда /buy <item_id>.
#
#  8. /daily - ежедневный бонус, отдельный кулдаун 24ч.
#
# ==============================================================================async def get_all_texts() -> list:
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


# ── doediki ─────────────────────────────────────────────────────────────────

def _farm_sync(user_id: int, username: str) -> dict:
    """
    Пытается сфармить Доедики.
    Возвращает dict:
        ok        bool  - успех / кулдаун
        earned    int   - сколько заработано (0 если кулдаун)
        balance   int   - новый баланс
        remaining int   - секунд до следующего фарма (если кулдаун)
        jackpot   bool  - был ли джекпот
    """
    con = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc)

    row = con.execute(
        "SELECT balance, last_farm FROM doediki WHERE user_id = ?", (user_id,)
    ).fetchone()

    if row:
        balance, last_farm_str = row
        if last_farm_str:
            last_farm = datetime.fromisoformat(last_farm_str)
            if last_farm.tzinfo is None:
                last_farm = last_farm.replace(tzinfo=timezone.utc)
            elapsed = (now - last_farm).total_seconds()
            if elapsed < FARM_COOLDOWN_SEC:
                con.close()
                return {
                    "ok": False,
                    "earned": 0,
                    "balance": balance,
                    "remaining": int(FARM_COOLDOWN_SEC - elapsed),
                    "jackpot": False,
                }
    else:
        balance = 0

    # Джекпот 5% -> 50-100 Доедиков, обычно 1-15
    jackpot = random.random() < 0.05
    earned  = random.randint(50, 100) if jackpot else random.randint(1, 15)
    balance += earned

    con.execute("""
        INSERT INTO doediki (user_id, username, balance, last_farm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username  = excluded.username,
            balance   = excluded.balance,
            last_farm = excluded.last_farm
    """, (user_id, username, balance, now.isoformat()))
    con.commit()
    con.close()

    return {"ok": True, "earned": earned, "balance": balance, "remaining": 0, "jackpot": jackpot}

async def farm(user_id: int, username: str) -> dict:
    return await asyncio.to_thread(_farm_sync, user_id, username)


def _get_balance_sync(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT balance FROM doediki WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else 0

async def get_balance(user_id: int) -> int:
    return await asyncio.to_thread(_get_balance_sync, user_id)


def _get_leaderboard_sync(limit: int = 10) -> list:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT username, balance FROM doediki ORDER BY balance DESC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    return rows

async def get_leaderboard(limit: int = 10) -> list:
    return await asyncio.to_thread(_get_leaderboard_sync, limit)


# ------------------------------------------------------------------------------
#  МАРКОВСКИЕ ЦЕПИ
# ------------------------------------------------------------------------------

def tokenize(text: str) -> list:
    return re.findall(r"[А-Яа-яЁёA-Za-z0-9]+", text)


def build_markov(texts: list, order: int = 2) -> dict:
    chain = defaultdict(list)
    for text in texts:
        words = tokenize(text)
        if len(words) < order + 1:
            continue
        for i in range(len(words) - order):
            key = tuple(words[i : i + order])
            chain[key].append(words[i + order])
    return chain


def generate_markov_free(chain: dict, max_words: int = 25, order: int = 2) -> str:
    """Свободная генерация для /generate."""
    if not chain:
        return ""
    result = list(random.choice(list(chain.keys())))
    for _ in range(max_words):
        key = tuple(result[-order:])
        if key not in chain:
            break
        result.append(random.choice(chain[key]))
    return " ".join(result)


def generate_continuation(chain: dict, seed: list, max_words: int = 20, order: int = 2) -> str:
    """
    Возвращает ТОЛЬКО новые слова-продолжение (без seed).
    Используется в /continue для раздельного форматирования.
    """
    if not chain or len(seed) < order:
        return ""

    keys = list(chain.keys())
    start_key = tuple(seed[-order:])

    if start_key not in chain:
        candidates = [k for k in keys if k[0].lower() == seed[-1].lower()]
        start_key = random.choice(candidates) if candidates else random.choice(keys)

    working = list(start_key)
    for _ in range(max_words):
        key = tuple(working[-order:])
        if key not in chain:
            break
        working.append(random.choice(chain[key]))

    new_words = working[order:]
    return " ".join(new_words)


# ------------------------------------------------------------------------------
#  ЭКРАНИРОВАНИЕ MarkdownV2
# ------------------------------------------------------------------------------

def escape_mdv2(text: str) -> str:
    """Экранирует спецсимволы для MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(r"([" + re.escape(special) + r"])", r"\\\1", text)


# ------------------------------------------------------------------------------
#  БОТ
# ------------------------------------------------------------------------------

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ──────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    text = (
        "⚡⚡ *ДердоBOT* активирован!\n\n"
        "Читаю все высеры, думаю, высераю\\.\n\n"
        "🕯️ *Команды:*\n"
        "  `/generate` - сгенерировать случайный высер\n"
        "  `/continue <текст>` - продолжить твою фразу\n"
        "  `/farm` - фармить Доедиков 🟦 _\\(кд 30 мин\\)_\n"
        "  `/doediki` - мой баланс Доедиков\n"
        "  `/topdoed` - топ фармеров\n"
        "  `/stats` - статистика мозга"
    )
    await msg.answer(text, parse_mode="MarkdownV2")


# ── /stats ───────────────────────────────────────────────────────────────────

@dp.message(Command("stats"))
async def cmd_stats(msg: Message) -> None:
    n = await count_messages()
    await msg.answer(
        f"📊 *Статистика ДердоBOTа*\n\n"
        f"💾 Сообщений в мозге: *{n}* / {MAX_MESSAGES}\n"
        f"😎 Заполнено: *{n / MAX_MESSAGES * 100:.1f}%*",
        parse_mode="Markdown",
    )


# ── /generate ────────────────────────────────────────────────────────────────

@dp.message(Command("generate"))
async def cmd_generate(msg: Message) -> None:
    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ леее маловато данных для высеров!!!")
        return

    chain = build_markov(texts)
    if not chain:
        all_words = [w for t in texts for w in tokenize(t)]
        random.shuffle(all_words)
        phrase = " ".join(all_words[:random.randint(5, 15)])
    else:
        phrase = generate_markov_free(chain)

    await msg.answer(f"_{phrase or '...95'}_", parse_mode="Markdown")


# ── /continue ────────────────────────────────────────────────────────────────

@dp.message(Command("continue"))
async def cmd_continue(msg: Message) -> None:
    raw   = msg.text or ""
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "✏️ Использование: `/continue <твой текст>`\n"
            "Пример: `/continue 95 дон чечня это`",
            parse_mode="Markdown",
        )
        return

    seed_text  = parts[1].strip()
    seed_words = tokenize(seed_text)

    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ Ещё мало высеров брат")
        return

    chain        = build_markov(texts)
    continuation = generate_continuation(chain, seed_words) if chain else ""

    if not continuation:
        continuation = "...95."

    # Форматирование: "фраза" + "продолжение"
    # seed - жирный, продолжение - курсив, между ними "+"
    seed_esc = escape_mdv2(seed_text)
    cont_esc = escape_mdv2(continuation)
    await msg.answer(
        f"*{seed_esc}* \\+ _{cont_esc}_",
        parse_mode="MarkdownV2",
    )


# ── /farm ────────────────────────────────────────────────────────────────────

@dp.message(Command("farm"))
async def cmd_farm(msg: Message) -> None:
    user     = msg.from_user
    user_id  = user.id if user else 0
    username = (user.username or user.full_name or str(user_id)) if user else str(user_id)

    result = await farm(user_id, username)

    if not result["ok"]:
        mins = result["remaining"] // 60
        secs = result["remaining"] % 60
        await msg.answer(
            f"💥 Подожди ле! До следующего фарма: *{mins}м {secs}с*\n\n"
            f"Доедиков в рабстве: *{result['balance']}* 🟦",
            parse_mode="Markdown",
        )
        return

    jackpot_line = "⚡ *ДЖЕКПОТ, ВЫ НАШЛИ МНОГО ДОЕДИКОВ!!!*\n\n" if result["jackpot"] else ""
    await msg.answer(
        f"{jackpot_line}"
        f"Собрано Доедиков: *{result['earned']}* 🟦\n"
        f"Доедиков в рабстве: *{result['balance']}* 🟦",
        parse_mode="Markdown",
    )
    logger.info("Фарм: user_id=%d earned=%d jackpot=%s", user_id, result["earned"], result["jackpot"])


# ── /doediki ─────────────────────────────────────────────────────────────────

@dp.message(Command("doediki"))
async def cmd_doediki(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    balance = await get_balance(user_id)
    name    = (msg.from_user.username or msg.from_user.full_name) if msg.from_user else "Анон"
    await msg.answer(
        f"💼 *{name}*, твой баланс:\n*{balance}* 🟦 Доедиков",
        parse_mode="Markdown",
    )


# ── /topdoed ─────────────────────────────────────────────────────────────────

@dp.message(Command("topdoed"))
async def cmd_topdoed(msg: Message) -> None:
    rows = await get_leaderboard(10)
    if not rows:
        await msg.answer(
            "📭 Никто ещё не фармил Доедики\\. Начни первым - `/farm`",
            parse_mode="MarkdownV2",
        )
        return

    medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
    lines  = ["🏆 *Топ фармеров Доедиков* 🟦\n"]
    for i, (username, balance) in enumerate(rows):
        lines.append(f"{medals[i]} *{username}* - {balance} 🟦")

    await msg.answer("\n".join(lines), parse_mode="Markdown")


# ── Все остальные текстовые сообщения ────────────────────────────────────────

@dp.message(F.text)
async def handle_text(msg: Message) -> None:
    text    = msg.text
    user_id = msg.from_user.id if msg.from_user else 0

    if len(text) > 40:
        logger.debug("Пропущено (длина %d > 40): %.40s", len(text), text)
        return

    await save_message(user_id, text)
    logger.info("Сохранено от user_id=%d: %s", user_id, text)

    if random.random() < RANDOM_REPLY_CHANCE:
        random_msg = await get_random_message()
        if random_msg:
            await msg.reply(f"_{random_msg}_", parse_mode="Markdown")
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

    await bot.set_my_commands([
        BotCommand(command="start",    description="Запустить бота / список команд"),
        BotCommand(command="generate", description="Сгенерировать случайную фразу"),
        BotCommand(command="continue", description="Продолжить фразу: /continue <текст>"),
        BotCommand(command="farm",     description="Сфармить Доедики 🟦 (кд 30 мин)"),
        BotCommand(command="doediki",  description="Мой баланс Доедиков"),
        BotCommand(command="topdoed",  description="Топ фармеров Доедиков"),
        BotCommand(command="stats",    description="Статистика базы данных"),
    ])
    logger.info("Команды зарегистрированы в Telegram")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ==============================================================================
#  ПРЕДЛОЖЕНИЯ ПО ФУНКЦИЯМ (НЕ ДОБАВЛЕНЫ)
# ==============================================================================
#
#  1. /top - топ самых частых слов в БД
#     Counter(tokenize(text) for text in texts), вывести топ-10 слов.
#
#  2. /who <слово> - кто чаще всего пишет это слово
#     GROUP BY user_id WHERE text LIKE '%слово%'.
#
#  3. /duel @user1 @user2 - битва стилей
#     Две отдельные цепи, одна фраза от каждого, голосование InlineKeyboard.
#
#  4. /forget - удалить свои сообщения из БД
#     DELETE FROM messages WHERE user_id = ?
#
#  5. Авто-постинг /generate раз в N часов
#     asyncio.create_task + sleep(N*3600), постит в чат по chat_id.
#
#  6. Динамический order для марков
#     order=3 если len(texts)>500 иначе order=2 - фразы связнее.
#
#  7. Магазин Доедиков /shop
#     Трать 🟦 на роли/стикеры/фичи. Таблица items, команда /buy <item_id>.
#
#  8. /daily - ежедневный бонус, отдельный кулдаун 24ч.
#
# ==============================================================================async def get_all_texts() -> list:
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


# ── doediki ─────────────────────────────────────────────────────────────────

def _farm_sync(user_id: int, username: str) -> dict:
    """
    Пытается сфармить Доедики.
    Возвращает dict:
        ok        bool  — успех / кулдаун
        earned    int   — сколько заработано (0 если кулдаун)
        balance   int   — новый баланс
        remaining int   — секунд до следующего фарма (если кулдаун)
        jackpot   bool  — был ли джекпот
    """
    con = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc)

    row = con.execute(
        "SELECT balance, last_farm FROM doediki WHERE user_id = ?", (user_id,)
    ).fetchone()

    if row:
        balance, last_farm_str = row
        if last_farm_str:
            last_farm = datetime.fromisoformat(last_farm_str)
            if last_farm.tzinfo is None:
                last_farm = last_farm.replace(tzinfo=timezone.utc)
            elapsed = (now - last_farm).total_seconds()
            if elapsed < FARM_COOLDOWN_SEC:
                con.close()
                return {
                    "ok": False,
                    "earned": 0,
                    "balance": balance,
                    "remaining": int(FARM_COOLDOWN_SEC - elapsed),
                    "jackpot": False,
                }
    else:
        balance = 0

    # Джекпот 5% -> 50-100 Доедиков, обычно 1-15
    jackpot = random.random() < 0.05
    earned  = random.randint(50, 100) if jackpot else random.randint(1, 15)
    balance += earned

    con.execute("""
        INSERT INTO doediki (user_id, username, balance, last_farm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username  = excluded.username,
            balance   = excluded.balance,
            last_farm = excluded.last_farm
    """, (user_id, username, balance, now.isoformat()))
    con.commit()
    con.close()

    return {"ok": True, "earned": earned, "balance": balance, "remaining": 0, "jackpot": jackpot}

async def farm(user_id: int, username: str) -> dict:
    return await asyncio.to_thread(_farm_sync, user_id, username)


def _get_balance_sync(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT balance FROM doediki WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else 0

async def get_balance(user_id: int) -> int:
    return await asyncio.to_thread(_get_balance_sync, user_id)


def _get_leaderboard_sync(limit: int = 10) -> list:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT username, balance FROM doediki ORDER BY balance DESC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    return rows

async def get_leaderboard(limit: int = 10) -> list:
    return await asyncio.to_thread(_get_leaderboard_sync, limit)


# ------------------------------------------------------------------------------
#  МАРКОВСКИЕ ЦЕПИ
# ------------------------------------------------------------------------------

def tokenize(text: str) -> list:
    return re.findall(r"[А-Яа-яЁёA-Za-z0-9]+", text)


def build_markov(texts: list, order: int = 2) -> dict:
    chain = defaultdict(list)
    for text in texts:
        words = tokenize(text)
        if len(words) < order + 1:
            continue
        for i in range(len(words) - order):
            key = tuple(words[i : i + order])
            chain[key].append(words[i + order])
    return chain


def generate_markov_free(chain: dict, max_words: int = 25, order: int = 2) -> str:
    """Свободная генерация для /generate."""
    if not chain:
        return ""
    result = list(random.choice(list(chain.keys())))
    for _ in range(max_words):
        key = tuple(result[-order:])
        if key not in chain:
            break
        result.append(random.choice(chain[key]))
    return " ".join(result)


def generate_continuation(chain: dict, seed: list, max_words: int = 20, order: int = 2) -> str:
    """
    Возвращает ТОЛЬКО новые слова-продолжение (без seed).
    Используется в /continue для раздельного форматирования.
    """
    if not chain or len(seed) < order:
        return ""

    keys = list(chain.keys())
    start_key = tuple(seed[-order:])

    if start_key not in chain:
        candidates = [k for k in keys if k[0].lower() == seed[-1].lower()]
        start_key = random.choice(candidates) if candidates else random.choice(keys)

    working = list(start_key)
    for _ in range(max_words):
        key = tuple(working[-order:])
        if key not in chain:
            break
        working.append(random.choice(chain[key]))

    new_words = working[order:]
    return " ".join(new_words)


# ------------------------------------------------------------------------------
#  ЭКРАНИРОВАНИЕ MarkdownV2
# ------------------------------------------------------------------------------

def escape_mdv2(text: str) -> str:
    """Экранирует спецсимволы для MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(r"([" + re.escape(special) + r"])", r"\\\1", text)


# ------------------------------------------------------------------------------
#  БОТ
# ------------------------------------------------------------------------------

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ──────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    text = (
        "⚡⚡ *ДердоBOT* активирован!\n\n"
        "Читаю все высеры, думаю, высераю "V2"\\.\n\n"
        "🕯️ *Команды:*\n"
        "  `/generate` - сгенерировать случайный высер\n"
        "  `/continue <текст>` - продолжить твою фразу\n"
        "  `/farm` - сфармить Доедики 🟦 _\\(кд 30 мин\\)_\n"
        "  `/doediki` - мой баланс Доедиков\n"
        "  `/topdoed` - топ фармеров\n"
        "  `/stats` - статистика мозга"
    )
    await msg.answer(text, parse_mode="MarkdownV2")


# ── /stats ───────────────────────────────────────────────────────────────────

@dp.message(Command("stats"))
async def cmd_stats(msg: Message) -> None:
    n = await count_messages()
    await msg.answer(
        f"📊 *Статистика ДердоBOTа*\n\n"
        f"💾 Сообщений в мозге: *{n}* / {MAX_MESSAGES}\n"
        f"😎 Заполнено: *{n / MAX_MESSAGES * 100:.1f}%*",
        parse_mode="Markdown",
    )


# ── /generate ────────────────────────────────────────────────────────────────

@dp.message(Command("generate"))
async def cmd_generate(msg: Message) -> None:
    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ леее маловато данных для высеров!!!")
        return

    chain = build_markov(texts)
    if not chain:
        all_words = [w for t in texts for w in tokenize(t)]
        random.shuffle(all_words)
        phrase = " ".join(all_words[:random.randint(5, 15)])
    else:
        phrase = generate_markov_free(chain)

    await msg.answer(f"_{phrase or '...95'}_", parse_mode="Markdown")


# ── /continue ────────────────────────────────────────────────────────────────

@dp.message(Command("continue"))
async def cmd_continue(msg: Message) -> None:
    raw   = msg.text or ""
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await msg.answer(
            "✏️ Использование: `/continue <твой текст>`\n"
            "Пример: `/continue 95 дон чечня это`",
            parse_mode="Markdown",
        )
        return

    seed_text  = parts[1].strip()
    seed_words = tokenize(seed_text)

    texts = await get_all_texts()
    if len(texts) < 5:
        await msg.answer("⚠️ Ещё мало высеров брат")
        return

    chain        = build_markov(texts)
    continuation = generate_continuation(chain, seed_words) if chain else ""

    if not continuation:
        continuation = "...95."

    # Форматирование: "фраза" + "продолжение"
    # seed — жирный, продолжение — курсив, между ними "+"
    seed_esc = escape_mdv2(seed_text)
    cont_esc = escape_mdv2(continuation)
    await msg.answer(
        f"*{seed_esc}* \\+ _{cont_esc}_",
        parse_mode="MarkdownV2",
    )


# ── /farm ────────────────────────────────────────────────────────────────────

@dp.message(Command("farm"))
async def cmd_farm(msg: Message) -> None:
    user     = msg.from_user
    user_id  = user.id if user else 0
    username = (user.username or user.full_name or str(user_id)) if user else str(user_id)

    result = await farm(user_id, username)

    if not result["ok"]:
        mins = result["remaining"] // 60
        secs = result["remaining"] % 60
        await msg.answer(
            f"⏳ Подожди бля! До следующего фарма: *{mins}м {secs}с*\n\n"
            f"Доедиков в рабстве: *{result['balance']}* 🟦",
            parse_mode="Markdown",
        )
        return

    jackpot_line = "⚡ *ДЖЕКПОТ ПО ПОИМКЕ!!!*\n\n" if result["jackpot"] else ""
    await msg.answer(
        f"{jackpot_line}"
        f"Собрано Доедиков: *{result['earned']}* 🟦\n"
        f"Доедиков в рабстве: *{result['balance']}* 🟦",
        parse_mode="Markdown",
    )
    logger.info("Фарм: user_id=%d earned=%d jackpot=%s", user_id, result["earned"], result["jackpot"])


# ── /doediki ─────────────────────────────────────────────────────────────────

@dp.message(Command("doediki"))
async def cmd_doediki(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    balance = await get_balance(user_id)
    name    = (msg.from_user.username or msg.from_user.full_name) if msg.from_user else "Анон"
    await msg.answer(
        f"💼 *{name}*, твой баланс:\n*{balance}* 🟦 Доедиков",
        parse_mode="Markdown",
    )


# ── /topdoed ─────────────────────────────────────────────────────────────────

@dp.message(Command("topdoed"))
async def cmd_topdoed(msg: Message) -> None:
    rows = await get_leaderboard(10)
    if not rows:
        await msg.answer(
            "📭 Никто ещё не фармил Доедики\\. Начни первым — `/farm`",
            parse_mode="MarkdownV2",
        )
        return

    medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
    lines  = ["🏆 *Топ фармеров Доедиков* 🟦\n"]
    for i, (username, balance) in enumerate(rows):
        lines.append(f"{medals[i]} *{username}* — {balance} 🟦")

    await msg.answer("\n".join(lines), parse_mode="Markdown")


# ── Все остальные текстовые сообщения ────────────────────────────────────────

@dp.message(F.text)
async def handle_text(msg: Message) -> None:
    text    = msg.text
    user_id = msg.from_user.id if msg.from_user else 0

    if len(text) > 40:
        logger.debug("Пропущено (длина %d > 40): %.40s", len(text), text)
        return

    await save_message(user_id, text)
    logger.info("Сохранено от user_id=%d: %s", user_id, text)

    if random.random() < RANDOM_REPLY_CHANCE:
        random_msg = await get_random_message()
        if random_msg:
            await msg.reply(f"_{random_msg}_", parse_mode="Markdown")
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

    await bot.set_my_commands([
        BotCommand(command="start",    description="Запустить бота / список команд"),
        BotCommand(command="generate", description="Сгенерировать случайную фразу"),
        BotCommand(command="continue", description="Продолжить фразу: /continue <текст>"),
        BotCommand(command="farm",     description="Сфармить Доедики 🟦 (кд 30 мин)"),
        BotCommand(command="doediki",  description="Мой баланс Доедиков"),
        BotCommand(command="topdoed",  description="Топ фармеров Доедиков"),
        BotCommand(command="stats",    description="Статистика базы данных"),
    ])
    logger.info("Команды зарегистрированы в Telegram")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ==============================================================================
#  ПРЕДЛОЖЕНИЯ ПО ФУНКЦИЯМ (НЕ ДОБАВЛЕНЫ)
# ==============================================================================
#
#  1. /top — топ самых частых слов в БД
#     Counter(tokenize(text) for text in texts), вывести топ-10 слов.
#
#  2. /who <слово> — кто чаще всего пишет это слово
#     GROUP BY user_id WHERE text LIKE '%слово%'.
#
#  3. /duel @user1 @user2 — битва стилей
#     Две отдельные цепи, одна фраза от каждого, голосование InlineKeyboard.
#
#  4. /forget — удалить свои сообщения из БД
#     DELETE FROM messages WHERE user_id = ?
#
#  5. Авто-постинг /generate раз в N часов
#     asyncio.create_task + sleep(N*3600), постит в чат по chat_id.
#
#  6. Динамический order для марков
#     order=3 если len(texts)>500 иначе order=2 — фразы связнее.
#
#  7. Магазин Доедиков /shop
#     Трать 🟦 на роли/стикеры/фичи. Таблица items, команда /buy <item_id>.
#
#  8. /daily — ежедневный бонус, отдельный кулдаун 24ч.
#
# ==============================================================================
