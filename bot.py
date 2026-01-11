import os
import uuid
import base64
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Iterable, List

import aiohttp
import asyncpg

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

from peremen import (
    BOT_TOKEN,
    ADMIN_ID,
    YOO_SHOP_ID,
    YOO_SECRET,
    YOO_MODE,
    YOO_RETURN_URL,
    PRIVATE_CHANNEL_ID,
    PRICE_RUB,
    CURRENCY,
    DESCRIPTION,
    SUPPORT_TEXT,
    DATABASE_URL,   # <-- добавь в peremen.py
)

# ---------------- PostgreSQL (asyncpg) ----------------
pool: Optional[asyncpg.Pool] = None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def db_init() -> None:
    assert pool is not None
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                is_subscribed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                user_id BIGINT PRIMARY KEY,
                payment_id TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            """
        )


async def upsert_user(user_id: int) -> None:
    assert pool is not None
    now = now_utc()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, is_subscribed, created_at, updated_at)
            VALUES ($1, FALSE, $2, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET updated_at = EXCLUDED.updated_at;
            """,
            user_id,
            now,
        )


async def set_subscribed(user_id: int, subscribed: bool) -> None:
    assert pool is not None
    now = now_utc()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, is_subscribed, created_at, updated_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (user_id) DO UPDATE
            SET is_subscribed = EXCLUDED.is_subscribed,
                updated_at = EXCLUDED.updated_at;
            """,
            user_id,
            subscribed,
            now,
        )


async def get_subscribed(user_id: int) -> bool:
    assert pool is not None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT is_subscribed FROM users WHERE user_id=$1",
            user_id,
        )
    return bool(row and row["is_subscribed"])


async def remove_user(user_id: int) -> None:
    assert pool is not None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM users WHERE user_id=$1", user_id)
            await conn.execute("DELETE FROM payments WHERE user_id=$1", user_id)


async def get_subscribers() -> List[int]:
    assert pool is not None
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users WHERE is_subscribed=TRUE")
    return [int(r["user_id"]) for r in rows]


async def count_users() -> tuple[int, int]:
    assert pool is not None
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        subs = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_subscribed=TRUE")
    return int(total), int(subs)


async def save_last_payment(user_id: int, payment_id: str) -> None:
    assert pool is not None
    now = now_utc()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO payments (user_id, payment_id, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
            SET payment_id = EXCLUDED.payment_id,
                updated_at = EXCLUDED.updated_at;
            """,
            user_id,
            payment_id,
            now,
        )


async def get_last_payment(user_id: int) -> Optional[str]:
    assert pool is not None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payment_id FROM payments WHERE user_id=$1",
            user_id,
        )
    return str(row["payment_id"]) if row else None


# ---------------- YooKassa ----------------
def yookassa_auth_header() -> str:
    token = base64.b64encode(f"{YOO_SHOP_ID}:{YOO_SECRET}".encode()).decode()
    return f"Basic {token}"


async def yk_create_payment(user_id: int) -> Dict[str, Any]:
    idempotence_key = str(uuid.uuid4())
    method = "bank_card" if YOO_MODE.upper() == "TEST" else "sbp"

    payload = {
        "amount": {"value": f"{PRICE_RUB}.00", "currency": CURRENCY},
        "capture": True,
        "description": DESCRIPTION,
        "confirmation": {"type": "redirect", "return_url": YOO_RETURN_URL},
        "payment_method_data": {"type": method},
        "metadata": {"tg_user_id": str(user_id)},
    }

    headers = {
        "Authorization": yookassa_auth_header(),
        "Content-Type": "application/json",
        "Idempotence-Key": idempotence_key,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.yookassa.ru/v3/payments",
            headers=headers,
            data=json.dumps(payload),
            timeout=20,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"YooKassa error {resp.status}: {text}")
            return json.loads(text)


async def yk_get_payment(payment_id: str) -> Dict[str, Any]:
    headers = {"Authorization": yookassa_auth_header()}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://api.yookassa.ru/v3/payments/{payment_id}",
            headers=headers,
            timeout=20,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"YooKassa error {resp.status}: {text}")
            return json.loads(text)


async def issue_invite_link(bot: Bot) -> str:
    invite = await bot.create_chat_invite_link(
        chat_id=PRIVATE_CHANNEL_ID,
        member_limit=1,
        creates_join_request=False,
    )
    return invite.invite_link


# ---------------- UI ----------------
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def menu_kb(subscribed: bool) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Оплатить", callback_data="pay")
    kb.button(text="✅ Проверить оплату", callback_data="check")
    kb.button(text="🔗 Получить доступ", callback_data="access")
    kb.button(
        text="🔕 Отписаться" if subscribed else "🔔 Подписаться на рассылку",
        callback_data="toggle_sub",
    )
    kb.button(text="🆘 Поддержка", callback_data="support")
    kb.adjust(1)
    return kb


dp = Dispatcher()


# ---------------- Команды ----------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)

    sub = await get_subscribed(user_id)
    await message.answer(
        "👋 Привет!\n\n"
        f"Это бот участия в челлендже.\n"
        f"💰 Стоимость: {PRICE_RUB} ₽\n\n"
        "Как пройти:\n"
        "1) Нажми «Оплатить»\n"
        "2) Оплати по ссылке\n"
        "3) Нажми «Проверить оплату»\n"
        "4) Нажми «Получить доступ»\n\n"
        "Хочешь получать анонсы/старты — включи рассылку 🔔",
        reply_markup=menu_kb(sub).as_markup(),
    )


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)
    await message.answer("Меню 👇", reply_markup=menu_kb(await get_subscribed(user_id)).as_markup())


@dp.message(Command("whoami"))
async def cmd_whoami(message: Message):
    await message.answer(f"Твой user_id: {message.from_user.id}")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    total, subs = await count_users()
    await message.answer(f"📊 База:\nВсего пользователей: {total}\nПодписчиков на рассылку: {subs}")


# ---------------- Callback кнопки ----------------
@dp.callback_query(F.data == "toggle_sub")
async def cb_toggle_sub(call: CallbackQuery):
    user_id = call.from_user.id
    await upsert_user(user_id)

    current = await get_subscribed(user_id)
    await set_subscribed(user_id, not current)

    await call.answer("Готово ✅")
    await call.message.answer(
        "🔔 Подписка включена." if not current else "🔕 Подписка выключена.",
        reply_markup=menu_kb(not current).as_markup(),
    )


@dp.callback_query(F.data == "support")
async def cb_support(call: CallbackQuery):
    await call.answer()
    await call.message.answer(SUPPORT_TEXT)


@dp.callback_query(F.data == "pay")
async def cb_pay(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id
    await upsert_user(user_id)
    await call.answer()

    try:
        payment = await yk_create_payment(user_id)
    except Exception as e:
        await call.message.answer(f"❌ Не смог создать платёж.\nПричина: {e}")
        return

    payment_id = payment["id"]
    await save_last_payment(user_id, payment_id)

    confirmation_url = payment["confirmation"]["confirmation_url"]
    await call.message.answer(
        "💳 Платёж создан!\n\n"
        "1) Оплати по ссылке:\n"
        f"{confirmation_url}\n\n"
        "2) Потом нажми «Проверить оплату» ✅\n"
        "3) И «Получить доступ» 🔗\n\n"
        f"🧾 Payment ID: {payment_id}",
        reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
    )


@dp.callback_query(F.data == "check")
async def cb_check(call: CallbackQuery):
    user_id = call.from_user.id
    await upsert_user(user_id)
    await call.answer()

    payment_id = await get_last_payment(user_id)
    if not payment_id:
        await call.message.answer("❗ Сначала создай платёж: нажми «Оплатить».")
        return

    try:
        payment = await yk_get_payment(payment_id)
    except Exception as e:
        await call.message.answer(f"❌ Ошибка при проверке платежа:\n{e}")
        return

    status = payment.get("status", "unknown")
    if status == "succeeded":
        await call.message.answer(
            "✅ Оплата подтверждена!\nНажми «Получить доступ» 🔗",
            reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
        )
    elif status in ("pending", "waiting_for_capture"):
        await call.message.answer(
            "⏳ Платёж пока обрабатывается.\n"
            "Подожди 10–30 секунд и нажми «Проверить оплату» ещё раз.",
            reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
        )
    else:
        await call.message.answer(
            f"⚠️ Статус платежа: {status}\n"
            "Если оплата не прошла — создай новый платёж кнопкой «Оплатить».",
            reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
        )


@dp.callback_query(F.data == "access")
async def cb_access(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id
    await upsert_user(user_id)
    await call.answer()

    payment_id = await get_last_payment(user_id)
    if not payment_id:
        await call.message.answer("❗ Сначала нажми «Оплатить».")
        return

    try:
        payment = await yk_get_payment(payment_id)
    except Exception as e:
        await call.message.answer(f"❌ Не смог проверить платёж:\n{e}")
        return

    status = payment.get("status")
    meta = payment.get("metadata") or {}

    if str(meta.get("tg_user_id", "")) not in ("", str(user_id)):
        await call.message.answer("❌ Этот платёж не твой. Создай новый платёж через «Оплатить».")
        return

    if status != "succeeded":
        await call.message.answer(
            f"⛔ Оплата ещё не подтверждена (status: {status}).\nНажми «Проверить оплату» ✅",
            reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
        )
        return

    try:
        link = await issue_invite_link(bot)
    except Exception as e:
        await call.message.answer(
            "✅ Оплата подтверждена, но я не могу выдать ссылку в канал.\n"
            f"Причина: {e}\n\n"
            "Проверь:\n"
            "• бот админ канала\n"
            "• есть право создавать пригласительные ссылки",
        )
        return

    await call.message.answer(
        "🎉 Доступ выдан!\n"
        f"Вот ссылка в закрытый канал:\n{link}",
        reply_markup=menu_kb(await get_subscribed(user_id)).as_markup(),
    )


# ---------------- Рассылки ----------------
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование:\n/broadcast Текст рассылки")
        return

    text = parts[1].strip()
    subscribers = await get_subscribers()

    if not subscribers:
        await message.answer("Подписчиков на рассылку пока нет (никто не нажал 🔔).")
        return

    await message.answer(f"📣 Старт рассылки: {len(subscribers)} подписчиков...")

    ok = blocked = failed = 0
    for uid in subscribers:
        try:
            await bot.send_message(uid, text)
            ok += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            blocked += 1
            await remove_user(uid)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await bot.send_message(uid, text)
                ok += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    await message.answer(
        "✅ Рассылка завершена.\n"
        f"Доставлено: {ok}\n"
        f"Заблокировали бота/нет доступа: {blocked}\n"
        f"Ошибки: {failed}"
    )


@dp.message(Command("broadcast_here"))
async def cmd_broadcast_here(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return

    if not message.reply_to_message:
        await message.answer("Использование: ответь на сообщение командой /broadcast_here")
        return

    subscribers = await get_subscribers()
    if not subscribers:
        await message.answer("Подписчиков на рассылку пока нет (никто не нажал 🔔).")
        return

    await message.answer(f"📎 Рассылка копией сообщения: {len(subscribers)} подписчиков...")

    ok = blocked = failed = 0
    src = message.reply_to_message

    for uid in subscribers:
        try:
            await src.copy_to(chat_id=uid)
            ok += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            blocked += 1
            await remove_user(uid)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await src.copy_to(chat_id=uid)
                ok += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    await message.answer(
        "✅ Рассылка завершена.\n"
        f"Доставлено: {ok}\n"
        f"Заблокировали бота/нет доступа: {blocked}\n"
        f"Ошибки: {failed}"
    )


# ---------------- Main ----------------
async def main():
    global pool

    if not BOT_TOKEN or "PASTE_" in BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в peremen.py")
    if not YOO_SHOP_ID or "PASTE_" in YOO_SHOP_ID or not YOO_SECRET or "PASTE_" in YOO_SECRET:
        raise RuntimeError("YOO_SHOP_ID/YOO_SECRET не заданы в peremen.py")
    if ADMIN_ID == 123456789:
        raise RuntimeError("Укажи ADMIN_ID в peremen.py (узнай через /whoami)")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан. Добавь в Railway Variables или в peremen.py")

    # Создаём пул Postgres
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    await db_init()

    bot = Bot(BOT_TOKEN)

    # чтобы polling не конфликтовал с webhook
    await bot.delete_webhook(drop_pending_updates=True)

    # Не слушаем channel_post, чтобы не ломаться от канала
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

    # Аккуратно закрываем пул
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
