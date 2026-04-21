"""
💰 Telegram бот для учёта расходов — Расширенная версия
=========================================================
Функции:
  - Добавление расходов и доходов
  - 15 категорий расходов + 8 категорий доходов
  - Месячный/годовой/всё время бюджет и лимиты по категориям
  - Статистика: по месяцам, по категориям, по дням недели
  - Экспорт в CSV
  - Поиск по комментарию
  - Несколько валют (₽, $, €)
  - Регулярные (повторяющиеся) расходы
  - Цели накоплений
  - Напоминания о внесении расходов
  - Топ трат
  - Сравнение месяцев
  - Многопользовательский режим (у каждого своя база)

Требования:
  pip install python-telegram-bot==20.7

Запуск:
  python expense_bot.py
"""

import logging
import sqlite3
import csv
import io
import os
from datetime import datetime, date, timedelta
from calendar import monthrange
from collections import defaultdict
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, ConversationHandler,
    filters, JobQueue
)

# ─── Настройки ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_FILE   = "expenses.db"
DEFAULT_CURRENCY = "₽"

# ─── Состояния диалогов ─────────────────────────────────────────────────────
(
    MAIN_MENU,
    ADD_AMOUNT, ADD_CATEGORY, ADD_COMMENT, ADD_DATE,
    ADD_INCOME_AMOUNT, ADD_INCOME_CATEGORY, ADD_INCOME_COMMENT,
    SET_BUDGET_AMOUNT, SET_BUDGET_CATEGORY,
    SET_LIMIT_CATEGORY, SET_LIMIT_AMOUNT,
    SEARCH_QUERY,
    SET_GOAL_NAME, SET_GOAL_AMOUNT, SET_GOAL_CONTRIB,
    ADD_RECURRING_AMOUNT, ADD_RECURRING_CATEGORY, ADD_RECURRING_FREQ,
    SET_CURRENCY,
    STATS_MENU, EXPORT_MENU, BUDGET_MENU, GOALS_MENU, RECURRING_MENU,
) = range(25)

# ─── Категории ──────────────────────────────────────────────────────────────
EXPENSE_CATEGORIES = [
    "🍔 Еда и рестораны",
    "🛒 Продукты",
    "🚌 Транспорт",
    "🚗 Авто",
    "🏠 Жильё и ЖКХ",
    "👕 Одежда",
    "💊 Здоровье",
    "🎮 Развлечения",
    "📱 Связь и интернет",
    "📚 Образование",
    "🐾 Питомцы",
    "🎁 Подарки",
    "✈️ Путешествия",
    "💼 Работа и бизнес",
    "❓ Другое",
]

INCOME_CATEGORIES = [
    "💼 Зарплата",
    "🤝 Фриланс",
    "📈 Инвестиции",
    "🎁 Подарок",
    "🏦 Кэшбэк",
    "🏠 Аренда",
    "🎰 Прочий доход",
    "💰 Возврат долга",
]

CURRENCIES = {"₽": "RUB", "$": "USD", "€": "EUR"}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ════════════════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS expenses (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            amount      REAL    NOT NULL,
            category    TEXT    NOT NULL,
            comment     TEXT    DEFAULT '',
            currency    TEXT    DEFAULT '₽',
            tx_type     TEXT    DEFAULT 'expense',
            tx_date     TEXT    NOT NULL,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS budgets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            month       TEXT    NOT NULL,
            amount      REAL    NOT NULL,
            UNIQUE(user_id, month)
        );

        CREATE TABLE IF NOT EXISTS limits (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            category    TEXT    NOT NULL,
            amount      REAL    NOT NULL,
            UNIQUE(user_id, category)
        );

        CREATE TABLE IF NOT EXISTS goals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            name        TEXT    NOT NULL,
            target      REAL    NOT NULL,
            saved       REAL    DEFAULT 0,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS recurring (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            amount      REAL    NOT NULL,
            category    TEXT    NOT NULL,
            comment     TEXT    DEFAULT '',
            frequency   TEXT    NOT NULL,
            next_date   TEXT    NOT NULL,
            active      INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS settings (
            user_id     INTEGER PRIMARY KEY,
            currency    TEXT    DEFAULT '₽',
            remind_hour INTEGER DEFAULT -1
        );
        """)
        conn.commit()


# ── Расходы / Доходы ────────────────────────────────────────────────────────

def add_transaction(user_id, amount, category, comment, currency, tx_type, tx_date=None):
    if tx_date is None:
        tx_date = date.today().isoformat()
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO expenses (user_id,amount,category,comment,currency,tx_type,tx_date) VALUES (?,?,?,?,?,?,?)",
            (user_id, amount, category, comment, currency, tx_type, tx_date)
        )
        conn.commit()
        return cur.lastrowid


def get_month_expenses(user_id, year=None, month=None):
    now = datetime.now()
    year  = year  or now.year
    month = month or now.month
    first = date(year, month, 1).isoformat()
    last  = date(year, month, monthrange(year, month)[1]).isoformat()
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM expenses WHERE user_id=? AND tx_type='expense' AND tx_date BETWEEN ? AND ? ORDER BY tx_date DESC",
            (user_id, first, last)
        ).fetchall()


def get_month_income(user_id, year=None, month=None):
    now = datetime.now()
    year  = year  or now.year
    month = month or now.month
    first = date(year, month, 1).isoformat()
    last  = date(year, month, monthrange(year, month)[1]).isoformat()
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM expenses WHERE user_id=? AND tx_type='income' AND tx_date BETWEEN ? AND ? ORDER BY tx_date DESC",
            (user_id, first, last)
        ).fetchall()


def get_total(rows):
    return sum(r["amount"] for r in rows)


def get_category_stats(user_id, year=None, month=None):
    rows = get_month_expenses(user_id, year, month)
    stats = defaultdict(float)
    for r in rows:
        stats[r["category"]] += r["amount"]
    return dict(sorted(stats.items(), key=lambda x: -x[1]))


def get_last_transactions(user_id, limit=15):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM expenses WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()


def delete_transaction(user_id, tx_id):
    with get_db() as conn:
        conn.execute("DELETE FROM expenses WHERE id=? AND user_id=?", (tx_id, user_id))
        conn.commit()


def delete_last_expense(user_id):
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM expenses WHERE user_id=? AND tx_type='expense' ORDER BY id DESC LIMIT 1",
            (user_id,)
        ).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM expenses WHERE id=?", (row["id"],))
        conn.commit()
    return True


def search_transactions(user_id, query):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM expenses WHERE user_id=? AND (comment LIKE ? OR category LIKE ?) ORDER BY tx_date DESC LIMIT 30",
            (user_id, f"%{query}%", f"%{query}%")
        ).fetchall()


def get_yearly_summary(user_id, year=None):
    year = year or datetime.now().year
    result = {}
    for m in range(1, 13):
        exp = get_total(get_month_expenses(user_id, year, m))
        inc = get_total(get_month_income(user_id, year, m))
        result[m] = {"expense": exp, "income": inc}
    return result


def get_weekday_stats(user_id):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT tx_date, amount FROM expenses WHERE user_id=? AND tx_type='expense' ORDER BY tx_date",
            (user_id,)
        ).fetchall()
    wd = defaultdict(float)
    cnt = defaultdict(int)
    for r in rows:
        d = date.fromisoformat(r["tx_date"])
        wd[d.weekday()] += r["amount"]
        cnt[d.weekday()] += 1
    return wd, cnt


def get_top_expenses(user_id, limit=5, year=None, month=None):
    now = datetime.now()
    year  = year  or now.year
    month = month or now.month
    first = date(year, month, 1).isoformat()
    last  = date(year, month, monthrange(year, month)[1]).isoformat()
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM expenses WHERE user_id=? AND tx_type='expense' AND tx_date BETWEEN ? AND ? ORDER BY amount DESC LIMIT ?",
            (user_id, first, last, limit)
        ).fetchall()


# ── Бюджет ──────────────────────────────────────────────────────────────────

def set_budget(user_id, amount, month=None):
    month = month or datetime.now().strftime("%Y-%m")
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO budgets (user_id, month, amount) VALUES (?,?,?)",
            (user_id, month, amount)
        )
        conn.commit()


def get_budget(user_id, month=None):
    month = month or datetime.now().strftime("%Y-%m")
    with get_db() as conn:
        row = conn.execute(
            "SELECT amount FROM budgets WHERE user_id=? AND month=?",
            (user_id, month)
        ).fetchone()
    return row["amount"] if row else None


# ── Лимиты ──────────────────────────────────────────────────────────────────

def set_limit(user_id, category, amount):
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO limits (user_id, category, amount) VALUES (?,?,?)",
            (user_id, category, amount)
        )
        conn.commit()


def get_limits(user_id):
    with get_db() as conn:
        rows = conn.execute("SELECT category, amount FROM limits WHERE user_id=?", (user_id,)).fetchall()
    return {r["category"]: r["amount"] for r in rows}


def delete_limit(user_id, category):
    with get_db() as conn:
        conn.execute("DELETE FROM limits WHERE user_id=? AND category=?", (user_id, category))
        conn.commit()


# ── Цели ────────────────────────────────────────────────────────────────────

def add_goal(user_id, name, target):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO goals (user_id, name, target) VALUES (?,?,?)",
            (user_id, name, target)
        )
        conn.commit()
        return cur.lastrowid


def get_goals(user_id):
    with get_db() as conn:
        return conn.execute("SELECT * FROM goals WHERE user_id=? ORDER BY id", (user_id,)).fetchall()


def contribute_goal(user_id, goal_id, amount):
    with get_db() as conn:
        conn.execute(
            "UPDATE goals SET saved = saved + ? WHERE id=? AND user_id=?",
            (amount, goal_id, user_id)
        )
        conn.commit()


def delete_goal(user_id, goal_id):
    with get_db() as conn:
        conn.execute("DELETE FROM goals WHERE id=? AND user_id=?", (goal_id, user_id))
        conn.commit()


# ── Регулярные платежи ───────────────────────────────────────────────────────

def add_recurring(user_id, amount, category, comment, frequency):
    next_date = date.today().isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO recurring (user_id,amount,category,comment,frequency,next_date) VALUES (?,?,?,?,?,?)",
            (user_id, amount, category, comment, frequency, next_date)
        )
        conn.commit()


def get_recurring(user_id):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM recurring WHERE user_id=? AND active=1 ORDER BY next_date",
            (user_id,)
        ).fetchall()


def delete_recurring(user_id, rec_id):
    with get_db() as conn:
        conn.execute("UPDATE recurring SET active=0 WHERE id=? AND user_id=?", (rec_id, user_id))
        conn.commit()


def apply_recurring(user_id, rec_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM recurring WHERE id=? AND user_id=?", (rec_id, user_id)).fetchone()
        if not row:
            return False
        cur = get_user_currency(user_id)
        add_transaction(user_id, row["amount"], row["category"], row["comment"], cur, "expense")
        freq = row["frequency"]
        nd = date.today()
        if freq == "daily":   nd += timedelta(days=1)
        elif freq == "weekly": nd += timedelta(weeks=1)
        elif freq == "monthly":
            m = nd.month % 12 + 1
            y = nd.year + (1 if nd.month == 12 else 0)
            nd = nd.replace(year=y, month=m)
        conn.execute("UPDATE recurring SET next_date=? WHERE id=?", (nd.isoformat(), rec_id))
        conn.commit()
    return True


# ── Настройки ───────────────────────────────────────────────────────────────

def get_user_currency(user_id):
    with get_db() as conn:
        row = conn.execute("SELECT currency FROM settings WHERE user_id=?", (user_id,)).fetchone()
    return row["currency"] if row else DEFAULT_CURRENCY


def set_user_currency(user_id, currency):
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (user_id, currency) VALUES (?,?)",
            (user_id, currency)
        )
        conn.commit()


# ── Экспорт CSV ─────────────────────────────────────────────────────────────

def export_csv(user_id, tx_type=None, year=None, month=None):
    now = datetime.now()
    with get_db() as conn:
        if tx_type and year and month:
            first = date(year, month, 1).isoformat()
            last  = date(year, month, monthrange(year, month)[1]).isoformat()
            rows = conn.execute(
                "SELECT * FROM expenses WHERE user_id=? AND tx_type=? AND tx_date BETWEEN ? AND ? ORDER BY tx_date",
                (user_id, tx_type, first, last)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM expenses WHERE user_id=? ORDER BY tx_date",
                (user_id,)
            ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Дата", "Тип", "Сумма", "Валюта", "Категория", "Комментарий"])
    for r in rows:
        writer.writerow([r["id"], r["tx_date"], r["tx_type"], r["amount"], r["currency"], r["category"], r["comment"]])
    output.seek(0)
    return output.getvalue().encode("utf-8-sig")


# ════════════════════════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ════════════════════════════════════════════════════════════════════════════

def main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("➕ Расход"),       KeyboardButton("💚 Доход")],
        [KeyboardButton("📊 Статистика"),   KeyboardButton("📋 История")],
        [KeyboardButton("💰 Бюджет"),       KeyboardButton("🎯 Цели")],
        [KeyboardButton("🔄 Регулярные"),   KeyboardButton("⚙️ Настройки")],
        [KeyboardButton("🔍 Поиск"),        KeyboardButton("📤 Экспорт")],
    ], resize_keyboard=True)


def cancel_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("❌ Отмена")]], resize_keyboard=True)


def skip_cancel_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("⏩ Пропустить"), KeyboardButton("❌ Отмена")]], resize_keyboard=True)


def make_grid_keyboard(items, cols=3, extra=None):
    rows = []
    for i in range(0, len(items), cols):
        rows.append([KeyboardButton(c) for c in items[i:i+cols]])
    if extra:
        rows.append([KeyboardButton(e) for e in extra])
    rows.append([KeyboardButton("❌ Отмена")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def stats_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📅 Этот месяц"),    KeyboardButton("📅 Прошлый месяц")],
        [KeyboardButton("📆 Год"),           KeyboardButton("📆 Сравнить месяцы")],
        [KeyboardButton("🗓 По дням недели"), KeyboardButton("🏆 Топ трат")],
        [KeyboardButton("💚 Доходы"),         KeyboardButton("⚖️ Баланс")],
        [KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)


def budget_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("💵 Установить бюджет"),  KeyboardButton("📊 Статус бюджета")],
        [KeyboardButton("🔒 Лимит категории"),    KeyboardButton("📋 Все лимиты")],
        [KeyboardButton("🗑 Удалить лимит"),       KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)


def goals_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🎯 Новая цель"),     KeyboardButton("📋 Мои цели")],
        [KeyboardButton("💸 Пополнить цель"), KeyboardButton("🗑 Удалить цель")],
        [KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)


def recurring_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("➕ Добавить регулярный"), KeyboardButton("📋 Мои регулярные")],
        [KeyboardButton("✅ Внести сейчас"),        KeyboardButton("🗑 Удалить регулярный")],
        [KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)


def settings_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("💱 Изменить валюту"),  KeyboardButton("📊 Моя статистика")],
        [KeyboardButton("🗑 Удалить данные"),    KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)


# ════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ════════════════════════════════════════════════════════════════════════════

def fmt(amount, cur=None):
    cur = cur or DEFAULT_CURRENCY
    return f"{amount:,.2f} {cur}"


def progress_bar(current, total, width=20):
    if total <= 0:
        return "░" * width
    filled = min(int(current / total * width), width)
    return "█" * filled + "░" * (width - filled)


def month_name_ru(month_num):
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь",
             "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month_num - 1]


def weekday_name_ru(wd):
    names = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    return names[wd]


async def go_main(update, text="Главное меню 👇"):
    await update.message.reply_text(text, reply_markup=main_keyboard())
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ХЭНДЛЕРЫ — ДОБАВЛЕНИЕ РАСХОДА
# ════════════════════════════════════════════════════════════════════════════

async def add_expense_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["tx_type"] = "expense"
    cur = get_user_currency(update.effective_user.id)
    await update.message.reply_text(
        f"💵 Введи сумму расхода ({cur}):",
        reply_markup=cancel_keyboard()
    )
    return ADD_AMOUNT


async def add_income_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["tx_type"] = "income"
    cur = get_user_currency(update.effective_user.id)
    await update.message.reply_text(
        f"💚 Введи сумму дохода ({cur}):",
        reply_markup=cancel_keyboard()
    )
    return ADD_AMOUNT


async def handle_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace(",", ".")
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число, например: *1500* или *299.99*", parse_mode="Markdown")
        return ADD_AMOUNT

    context.user_data["amount"] = amount
    tx_type = context.user_data.get("tx_type", "expense")
    cats = EXPENSE_CATEGORIES if tx_type == "expense" else INCOME_CATEGORIES
    await update.message.reply_text(
        f"✅ Сумма: *{amount:,.2f}*\n\nВыбери категорию:",
        parse_mode="Markdown",
        reply_markup=make_grid_keyboard(cats, cols=2)
    )
    return ADD_CATEGORY


async def handle_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cat = update.message.text.strip()
    tx_type = context.user_data.get("tx_type", "expense")
    cats = EXPENSE_CATEGORIES if tx_type == "expense" else INCOME_CATEGORIES
    if cat not in cats:
        await update.message.reply_text("❗ Выбери категорию из списка 👇")
        return ADD_CATEGORY
    context.user_data["category"] = cat
    await update.message.reply_text(
        "💬 Комментарий (необязательно):",
        reply_markup=skip_cancel_keyboard()
    )
    return ADD_COMMENT


async def handle_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    context.user_data["comment"] = "" if text == "⏩ Пропустить" else text
    await update.message.reply_text(
        "📅 Укажи дату (ДД.ММ.ГГГГ) или пропусти для сегодня:",
        reply_markup=skip_cancel_keyboard()
    )
    return ADD_DATE


async def handle_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user_id  = update.effective_user.id
    tx_type  = context.user_data.get("tx_type", "expense")
    amount   = context.user_data["amount"]
    category = context.user_data["category"]
    comment  = context.user_data.get("comment", "")
    currency = get_user_currency(user_id)

    if text == "⏩ Пропустить":
        tx_date = date.today().isoformat()
    else:
        try:
            tx_date = datetime.strptime(text, "%d.%m.%Y").date().isoformat()
        except ValueError:
            await update.message.reply_text("❗ Неверный формат даты. Используй ДД.ММ.ГГГГ или пропусти.")
            return ADD_DATE

    add_transaction(user_id, amount, category, comment, currency, tx_type, tx_date)

    # Проверяем лимиты
    warning = ""
    if tx_type == "expense":
        limits = get_limits(user_id)
        if category in limits:
            month_stats = get_category_stats(user_id)
            spent = month_stats.get(category, 0)
            lim   = limits[category]
            if spent > lim:
                warning = f"\n\n⚠️ Превышен лимит по *{category}*!\nПотрачено: *{fmt(spent, currency)}* из *{fmt(lim, currency)}*"
            elif spent > lim * 0.8:
                warning = f"\n\n⚠️ Осталось менее 20% лимита по *{category}*!"

    total = get_total(get_month_expenses(user_id))
    budget = get_budget(user_id)
    budget_warn = ""
    if budget and tx_type == "expense":
        pct = total / budget * 100
        if pct > 100:
            budget_warn = f"\n\n🔴 Бюджет превышен! ({pct:.0f}%)"
        elif pct > 80:
            budget_warn = f"\n\n🟡 Использовано {pct:.0f}% месячного бюджета"

    emoji = "✅" if tx_type == "expense" else "💚"
    type_word = "Расход" if tx_type == "expense" else "Доход"
    msg = (
        f"{emoji} {type_word} записан!\n\n"
        f"💵 Сумма:     *{fmt(amount, currency)}*\n"
        f"🏷 Категория: *{category}*\n"
        f"📅 Дата:      *{tx_date}*\n"
    )
    if comment:
        msg += f"💬 Комментарий: {comment}\n"
    msg += f"\n📊 Расходы за месяц: *{fmt(total, currency)}*"
    msg += warning + budget_warn

    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=main_keyboard())
    context.user_data.clear()
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  СТАТИСТИКА
# ════════════════════════════════════════════════════════════════════════════

async def stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 Выбери тип статистики:", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_this_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    now = datetime.now()
    stats = get_category_stats(uid)
    total = get_total(get_month_expenses(uid))
    budget = get_budget(uid)
    income_total = get_total(get_month_income(uid))

    if not stats:
        await update.message.reply_text("📊 Нет расходов за текущий месяц.", reply_markup=stats_keyboard())
        return STATS_MENU

    lines = [f"📊 *{month_name_ru(now.month)} {now.year}*\n"]
    for cat, amt in stats.items():
        pct = amt / total * 100 if total > 0 else 0
        bar = progress_bar(amt, total)
        lines.append(f"{cat}\n`{bar}` {pct:.1f}%  —  *{fmt(amt, cur)}*\n")

    lines.append(f"──────────────────")
    lines.append(f"💸 Расходы: *{fmt(total, cur)}*")
    lines.append(f"💚 Доходы:  *{fmt(income_total, cur)}*")
    lines.append(f"⚖️ Баланс:  *{fmt(income_total - total, cur)}*")

    if budget:
        pct = total / budget * 100
        bar = progress_bar(total, budget)
        lines.append(f"\n💰 Бюджет: `{bar}` {pct:.0f}%")
        lines.append(f"   {fmt(total, cur)} / {fmt(budget, cur)}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_last_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    now = datetime.now()
    if now.month == 1:
        y, m = now.year - 1, 12
    else:
        y, m = now.year, now.month - 1

    stats = get_category_stats(uid, y, m)
    total = get_total(get_month_expenses(uid, y, m))

    if not stats:
        await update.message.reply_text(f"📊 Нет расходов за {month_name_ru(m)} {y}.", reply_markup=stats_keyboard())
        return STATS_MENU

    lines = [f"📊 *{month_name_ru(m)} {y}*\n"]
    for cat, amt in stats.items():
        pct = amt / total * 100 if total > 0 else 0
        bar = progress_bar(amt, total)
        lines.append(f"{cat}\n`{bar}` {pct:.1f}%  —  *{fmt(amt, cur)}*\n")
    lines.append(f"──────────────────\n💸 Итого: *{fmt(total, cur)}*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    year = datetime.now().year
    summary = get_yearly_summary(uid, year)

    lines = [f"📆 *Статистика за {year} год*\n"]
    annual_exp = annual_inc = 0
    for m in range(1, 13):
        exp = summary[m]["expense"]
        inc = summary[m]["income"]
        annual_exp += exp
        annual_inc += inc
        if exp > 0 or inc > 0:
            bar = progress_bar(exp, max(d["expense"] for d in summary.values()) or 1, width=12)
            lines.append(f"{month_name_ru(m)[:3]:>3} `{bar}` {fmt(exp, cur)}")

    lines.append(f"\n──────────────────")
    lines.append(f"💸 Расходы за год: *{fmt(annual_exp, cur)}*")
    lines.append(f"💚 Доходы за год:  *{fmt(annual_inc, cur)}*")
    lines.append(f"⚖️ Баланс:         *{fmt(annual_inc - annual_exp, cur)}*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_compare(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    now = datetime.now()
    this_m = get_total(get_month_expenses(uid))
    if now.month == 1:
        prev_total = get_total(get_month_expenses(uid, now.year - 1, 12))
        prev_name  = f"{month_name_ru(12)} {now.year-1}"
    else:
        prev_total = get_total(get_month_expenses(uid, now.year, now.month - 1))
        prev_name  = month_name_ru(now.month - 1)

    diff = this_m - prev_total
    arrow = "📈" if diff > 0 else "📉"
    pct  = abs(diff / prev_total * 100) if prev_total > 0 else 0

    msg = (
        f"📆 *Сравнение месяцев*\n\n"
        f"📅 {month_name_ru(now.month)}: *{fmt(this_m, cur)}*\n"
        f"📅 {prev_name}:    *{fmt(prev_total, cur)}*\n\n"
        f"{arrow} Разница: *{fmt(abs(diff), cur)}* ({pct:.1f}%)\n"
    )
    if diff > 0:
        msg += "Расходы выросли по сравнению с прошлым месяцем."
    elif diff < 0:
        msg += "Расходы снизились по сравнению с прошлым месяцем. 🎉"
    else:
        msg += "Расходы одинаковые."

    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_weekday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    wd, cnt = get_weekday_stats(uid)

    if not wd:
        await update.message.reply_text("Нет данных для статистики по дням.", reply_markup=stats_keyboard())
        return STATS_MENU

    max_val = max(wd.values()) if wd else 1
    lines   = ["🗓 *Расходы по дням недели*\n"]
    for d in range(7):
        amt = wd.get(d, 0)
        avg = amt / cnt[d] if cnt[d] else 0
        bar = progress_bar(amt, max_val, width=15)
        lines.append(f"{weekday_name_ru(d)} `{bar}` ср. *{fmt(avg, cur)}*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cur  = get_user_currency(uid)
    rows = get_top_expenses(uid, 10)

    if not rows:
        await update.message.reply_text("Нет расходов за этот месяц.", reply_markup=stats_keyboard())
        return STATS_MENU

    lines = ["🏆 *Топ-10 трат за месяц*\n"]
    for i, r in enumerate(rows, 1):
        cmt = f" — {r['comment']}" if r["comment"] else ""
        lines.append(f"{i}. *{fmt(r['amount'], cur)}* | {r['category']}{cmt}\n   📅 {r['tx_date']}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_income(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    now = datetime.now()
    rows = get_month_income(uid)
    total = get_total(rows)

    if not rows:
        await update.message.reply_text("💚 Нет доходов за текущий месяц.", reply_markup=stats_keyboard())
        return STATS_MENU

    stats = defaultdict(float)
    for r in rows:
        stats[r["category"]] += r["amount"]
    stats = dict(sorted(stats.items(), key=lambda x: -x[1]))

    lines = [f"💚 *Доходы за {month_name_ru(now.month)} {now.year}*\n"]
    for cat, amt in stats.items():
        pct = amt / total * 100
        lines.append(f"{cat}: *{fmt(amt, cur)}* ({pct:.1f}%)")
    lines.append(f"\n💰 Итого: *{fmt(total, cur)}*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


async def stats_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    now = datetime.now()
    exp = get_total(get_month_expenses(uid))
    inc = get_total(get_month_income(uid))
    bal = inc - exp

    summary = get_yearly_summary(uid)
    year_exp = sum(v["expense"] for v in summary.values())
    year_inc = sum(v["income"]  for v in summary.values())

    emoji = "💹" if bal >= 0 else "📉"
    msg = (
        f"{emoji} *Баланс*\n\n"
        f"*{month_name_ru(now.month)} {now.year}:*\n"
        f"  💚 Доходы:  {fmt(inc, cur)}\n"
        f"  💸 Расходы: {fmt(exp, cur)}\n"
        f"  ⚖️ Баланс:  *{fmt(bal, cur)}*\n\n"
        f"*{now.year} год:*\n"
        f"  💚 Доходы:  {fmt(year_inc, cur)}\n"
        f"  💸 Расходы: {fmt(year_exp, cur)}\n"
        f"  ⚖️ Баланс:  *{fmt(year_inc - year_exp, cur)}*"
    )
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=stats_keyboard())
    return STATS_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ИСТОРИЯ
# ════════════════════════════════════════════════════════════════════════════

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cur  = get_user_currency(uid)
    rows = get_last_transactions(uid, 15)

    if not rows:
        return await go_main(update, "📋 История пуста.")

    lines = ["📋 *Последние 15 транзакций:*\n"]
    for r in rows:
        icon = "💸" if r["tx_type"] == "expense" else "💚"
        cmt  = f" | {r['comment']}" if r["comment"] else ""
        lines.append(f"{icon} {r['tx_date']}  *{fmt(r['amount'], cur)}*\n   {r['category']}{cmt}")

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑 Удалить последний расход", callback_data="del_last")
    ]])
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=kb)
    await update.message.reply_text("Главное меню 👇", reply_markup=main_keyboard())
    return MAIN_MENU


async def del_last_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if delete_last_expense(uid):
        await query.edit_message_reply_markup(None)
        await query.message.reply_text("↩️ Последний расход удалён.", reply_markup=main_keyboard())
    else:
        await query.answer("Нет расходов для удаления.", show_alert=True)
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ПОИСК
# ════════════════════════════════════════════════════════════════════════════

async def search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Введи поисковый запрос (часть категории или комментария):",
                                    reply_markup=cancel_keyboard())
    return SEARCH_QUERY


async def search_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    cur   = get_user_currency(uid)
    query = update.message.text.strip()
    rows  = search_transactions(uid, query)

    if not rows:
        await update.message.reply_text(f"🔍 По запросу «{query}» ничего не найдено.", reply_markup=main_keyboard())
        return MAIN_MENU

    lines = [f"🔍 *Результаты поиска «{query}»* ({len(rows)} записей)\n"]
    for r in rows:
        icon = "💸" if r["tx_type"] == "expense" else "💚"
        cmt  = f" | {r['comment']}" if r["comment"] else ""
        lines.append(f"{icon} {r['tx_date']}  *{fmt(r['amount'], cur)}*\n   {r['category']}{cmt}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=main_keyboard())
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  БЮДЖЕТ И ЛИМИТЫ
# ════════════════════════════════════════════════════════════════════════════

async def budget_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💰 Управление бюджетом:", reply_markup=budget_keyboard())
    return BUDGET_MENU


async def budget_set_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💵 Введи сумму месячного бюджета:", reply_markup=cancel_keyboard())
    return SET_BUDGET_AMOUNT


async def budget_set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число.")
        return SET_BUDGET_AMOUNT
    uid = update.effective_user.id
    set_budget(uid, amount)
    cur = get_user_currency(uid)
    await update.message.reply_text(f"✅ Бюджет установлен: *{fmt(amount, cur)}*",
                                     parse_mode="Markdown", reply_markup=budget_keyboard())
    return BUDGET_MENU


async def budget_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    cur    = get_user_currency(uid)
    budget = get_budget(uid)
    total  = get_total(get_month_expenses(uid))

    if not budget:
        await update.message.reply_text("❗ Бюджет не установлен. Нажми *Установить бюджет*.",
                                         parse_mode="Markdown", reply_markup=budget_keyboard())
        return BUDGET_MENU

    remaining = budget - total
    pct = total / budget * 100
    bar = progress_bar(total, budget)
    status = "🔴 Превышен!" if remaining < 0 else ("🟡 Осторожно" if pct > 80 else "🟢 В норме")

    msg = (
        f"💰 *Бюджет на {month_name_ru(datetime.now().month)}*\n\n"
        f"Лимит:     *{fmt(budget, cur)}*\n"
        f"Потрачено: *{fmt(total, cur)}*\n"
        f"Осталось:  *{fmt(remaining, cur)}*\n\n"
        f"`{bar}` {pct:.0f}%\n{status}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=budget_keyboard())
    return BUDGET_MENU


async def limit_set_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔒 Выбери категорию для лимита:",
                                     reply_markup=make_grid_keyboard(EXPENSE_CATEGORIES, cols=2))
    return SET_LIMIT_CATEGORY


async def limit_set_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cat = update.message.text.strip()
    if cat not in EXPENSE_CATEGORIES:
        await update.message.reply_text("❗ Выбери из списка.")
        return SET_LIMIT_CATEGORY
    context.user_data["limit_cat"] = cat
    await update.message.reply_text(f"💵 Введи лимит для *{cat}*:", parse_mode="Markdown",
                                     reply_markup=cancel_keyboard())
    return SET_LIMIT_AMOUNT


async def limit_set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число.")
        return SET_LIMIT_AMOUNT
    uid = update.effective_user.id
    cat = context.user_data["limit_cat"]
    set_limit(uid, cat, amount)
    cur = get_user_currency(uid)
    await update.message.reply_text(f"✅ Лимит для *{cat}*: *{fmt(amount, cur)}*",
                                     parse_mode="Markdown", reply_markup=budget_keyboard())
    return BUDGET_MENU


async def limits_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    cur    = get_user_currency(uid)
    limits = get_limits(uid)
    stats  = get_category_stats(uid)

    if not limits:
        await update.message.reply_text("📋 Лимиты не установлены.", reply_markup=budget_keyboard())
        return BUDGET_MENU

    lines = ["📋 *Лимиты по категориям*\n"]
    for cat, lim in limits.items():
        spent = stats.get(cat, 0)
        pct   = spent / lim * 100
        bar   = progress_bar(spent, lim, width=15)
        status = "🔴" if pct > 100 else ("🟡" if pct > 80 else "🟢")
        lines.append(f"{status} {cat}\n   `{bar}` {pct:.0f}%  {fmt(spent, cur)} / {fmt(lim, cur)}\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=budget_keyboard())
    return BUDGET_MENU


async def limit_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    limits = get_limits(uid)
    if not limits:
        await update.message.reply_text("❗ Нет установленных лимитов.", reply_markup=budget_keyboard())
        return BUDGET_MENU
    cats = list(limits.keys())
    kb   = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🗑 {c}", callback_data=f"dellim:{c}")]
        for c in cats
    ])
    await update.message.reply_text("Выбери лимит для удаления:", reply_markup=kb)
    return BUDGET_MENU


async def limit_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    cat = query.data.split(":", 1)[1]
    delete_limit(uid, cat)
    await query.edit_message_text(f"✅ Лимит для *{cat}* удалён.", parse_mode="Markdown")
    return BUDGET_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ЦЕЛИ
# ════════════════════════════════════════════════════════════════════════════

async def goals_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎯 Управление целями:", reply_markup=goals_keyboard())
    return GOALS_MENU


async def goal_new_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎯 Введи название цели (например: *Отпуск*, *Ноутбук*):",
                                     parse_mode="Markdown", reply_markup=cancel_keyboard())
    return SET_GOAL_NAME


async def goal_set_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["goal_name"] = update.message.text.strip()
    await update.message.reply_text("💵 Введи целевую сумму:", reply_markup=cancel_keyboard())
    return SET_GOAL_AMOUNT


async def goal_set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число.")
        return SET_GOAL_AMOUNT
    uid  = update.effective_user.id
    name = context.user_data["goal_name"]
    add_goal(uid, name, amount)
    cur = get_user_currency(uid)
    await update.message.reply_text(f"✅ Цель *{name}* создана! Сумма: *{fmt(amount, cur)}*",
                                     parse_mode="Markdown", reply_markup=goals_keyboard())
    return GOALS_MENU


async def goals_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    cur   = get_user_currency(uid)
    goals = get_goals(uid)

    if not goals:
        await update.message.reply_text("🎯 Нет активных целей.", reply_markup=goals_keyboard())
        return GOALS_MENU

    lines = ["🎯 *Мои цели*\n"]
    for g in goals:
        pct = g["saved"] / g["target"] * 100 if g["target"] > 0 else 0
        bar = progress_bar(g["saved"], g["target"])
        left = g["target"] - g["saved"]
        status = "✅" if pct >= 100 else "🔄"
        lines.append(
            f"{status} *{g['name']}*\n"
            f"`{bar}` {pct:.1f}%\n"
            f"   Накоплено: *{fmt(g['saved'], cur)}* / *{fmt(g['target'], cur)}*\n"
            f"   Осталось: *{fmt(max(0, left), cur)}*\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=goals_keyboard())
    return GOALS_MENU


async def goal_contrib_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    goals = get_goals(uid)
    if not goals:
        await update.message.reply_text("🎯 Нет активных целей.", reply_markup=goals_keyboard())
        return GOALS_MENU
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎯 {g['name']} ({g['saved']:.0f}/{g['target']:.0f})", callback_data=f"contrib:{g['id']}")]
        for g in goals
    ])
    await update.message.reply_text("Выбери цель для пополнения:", reply_markup=kb)
    return SET_GOAL_CONTRIB


async def goal_contrib_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    goal_id = int(query.data.split(":")[1])
    context.user_data["contrib_goal"] = goal_id
    await query.edit_message_text("💵 Введи сумму пополнения:")
    return SET_GOAL_CONTRIB


async def goal_contrib_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число.")
        return SET_GOAL_CONTRIB
    uid     = update.effective_user.id
    cur     = get_user_currency(uid)
    goal_id = context.user_data.get("contrib_goal")
    if goal_id:
        contribute_goal(uid, goal_id, amount)
    await update.message.reply_text(f"✅ Пополнено на *{fmt(amount, cur)}*!",
                                     parse_mode="Markdown", reply_markup=goals_keyboard())
    return GOALS_MENU


async def goal_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    goals = get_goals(uid)
    if not goals:
        await update.message.reply_text("🎯 Нет целей для удаления.", reply_markup=goals_keyboard())
        return GOALS_MENU
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🗑 {g['name']}", callback_data=f"delgoal:{g['id']}")]
        for g in goals
    ])
    await update.message.reply_text("Выбери цель для удаления:", reply_markup=kb)
    return GOALS_MENU


async def goal_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    uid     = query.from_user.id
    goal_id = int(query.data.split(":")[1])
    delete_goal(uid, goal_id)
    await query.edit_message_text("✅ Цель удалена.")
    return GOALS_MENU


# ════════════════════════════════════════════════════════════════════════════
#  РЕГУЛЯРНЫЕ ПЛАТЕЖИ
# ════════════════════════════════════════════════════════════════════════════

async def recurring_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Регулярные платежи:", reply_markup=recurring_keyboard())
    return RECURRING_MENU


async def recurring_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💵 Введи сумму регулярного платежа:", reply_markup=cancel_keyboard())
    return ADD_RECURRING_AMOUNT


async def recurring_add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❗ Введи корректное число.")
        return ADD_RECURRING_AMOUNT
    context.user_data["rec_amount"] = amount
    await update.message.reply_text("🏷 Выбери категорию:",
                                     reply_markup=make_grid_keyboard(EXPENSE_CATEGORIES, cols=2))
    return ADD_RECURRING_CATEGORY


async def recurring_add_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cat = update.message.text.strip()
    if cat not in EXPENSE_CATEGORIES:
        await update.message.reply_text("❗ Выбери из списка.")
        return ADD_RECURRING_CATEGORY
    context.user_data["rec_cat"] = cat
    kb = ReplyKeyboardMarkup([
        [KeyboardButton("📅 Ежедневно"), KeyboardButton("📅 Еженедельно")],
        [KeyboardButton("📅 Ежемесячно"), KeyboardButton("❌ Отмена")],
    ], resize_keyboard=True)
    await update.message.reply_text("⏱ Выбери частоту:", reply_markup=kb)
    return ADD_RECURRING_FREQ


async def recurring_add_freq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    freq_map = {
        "📅 Ежедневно":   "daily",
        "📅 Еженедельно": "weekly",
        "📅 Ежемесячно":  "monthly",
    }
    if text not in freq_map:
        await update.message.reply_text("❗ Выбери частоту из кнопок.")
        return ADD_RECURRING_FREQ

    uid    = update.effective_user.id
    amount = context.user_data["rec_amount"]
    cat    = context.user_data["rec_cat"]
    freq   = freq_map[text]
    add_recurring(uid, amount, cat, "", freq)
    cur = get_user_currency(uid)

    await update.message.reply_text(
        f"✅ Регулярный платёж добавлен!\n\n"
        f"💵 Сумма:     *{fmt(amount, cur)}*\n"
        f"🏷 Категория: *{cat}*\n"
        f"⏱ Частота:   *{text}*",
        parse_mode="Markdown", reply_markup=recurring_keyboard()
    )
    return RECURRING_MENU


async def recurring_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cur  = get_user_currency(uid)
    rows = get_recurring(uid)

    if not rows:
        await update.message.reply_text("🔄 Нет регулярных платежей.", reply_markup=recurring_keyboard())
        return RECURRING_MENU

    freq_names = {"daily": "Ежедневно", "weekly": "Еженедельно", "monthly": "Ежемесячно"}
    lines = ["🔄 *Регулярные платежи*\n"]
    for r in rows:
        lines.append(
            f"• *{fmt(r['amount'], cur)}* | {r['category']}\n"
            f"  ⏱ {freq_names.get(r['frequency'], r['frequency'])} | след. {r['next_date']}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=recurring_keyboard())
    return RECURRING_MENU


async def recurring_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cur  = get_user_currency(uid)
    rows = get_recurring(uid)

    if not rows:
        await update.message.reply_text("🔄 Нет регулярных платежей.", reply_markup=recurring_keyboard())
        return RECURRING_MENU

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"✅ {r['category']} — {fmt(r['amount'], cur)}",
            callback_data=f"apprec:{r['id']}"
        )] for r in rows
    ])
    await update.message.reply_text("Выбери платёж для внесения:", reply_markup=kb)
    return RECURRING_MENU


async def recurring_apply_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    uid    = query.from_user.id
    rec_id = int(query.data.split(":")[1])
    if apply_recurring(uid, rec_id):
        await query.edit_message_text("✅ Платёж внесён и дата обновлена!")
    else:
        await query.edit_message_text("❗ Ошибка внесения платежа.")
    return RECURRING_MENU


async def recurring_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cur  = get_user_currency(uid)
    rows = get_recurring(uid)

    if not rows:
        await update.message.reply_text("🔄 Нет регулярных платежей.", reply_markup=recurring_keyboard())
        return RECURRING_MENU

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"🗑 {r['category']} — {fmt(r['amount'], cur)}",
            callback_data=f"delrec:{r['id']}"
        )] for r in rows
    ])
    await update.message.reply_text("Выбери платёж для удаления:", reply_markup=kb)
    return RECURRING_MENU


async def recurring_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    uid    = query.from_user.id
    rec_id = int(query.data.split(":")[1])
    delete_recurring(uid, rec_id)
    await query.edit_message_text("✅ Регулярный платёж удалён.")
    return RECURRING_MENU


# ════════════════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ════════════════════════════════════════════════════════════════════════════

async def settings_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    await update.message.reply_text(
        f"⚙️ *Настройки*\n\nВалюта: *{cur}*",
        parse_mode="Markdown", reply_markup=settings_keyboard()
    )
    return STATS_MENU


async def currency_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = ReplyKeyboardMarkup(
        [[KeyboardButton(c) for c in CURRENCIES.keys()], [KeyboardButton("❌ Отмена")]],
        resize_keyboard=True
    )
    await update.message.reply_text("💱 Выбери валюту:", reply_markup=kb)
    return SET_CURRENCY


async def currency_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur = update.message.text.strip()
    if cur not in CURRENCIES:
        await update.message.reply_text("❗ Выбери валюту из кнопок.")
        return SET_CURRENCY
    set_user_currency(update.effective_user.id, cur)
    await update.message.reply_text(f"✅ Валюта изменена на *{cur}*", parse_mode="Markdown",
                                     reply_markup=main_keyboard())
    return MAIN_MENU


async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_currency(uid)
    with get_db() as conn:
        total_count  = conn.execute("SELECT COUNT(*) as c FROM expenses WHERE user_id=?", (uid,)).fetchone()["c"]
        exp_count    = conn.execute("SELECT COUNT(*) as c FROM expenses WHERE user_id=? AND tx_type='expense'", (uid,)).fetchone()["c"]
        inc_count    = conn.execute("SELECT COUNT(*) as c FROM expenses WHERE user_id=? AND tx_type='income'", (uid,)).fetchone()["c"]
        first_row    = conn.execute("SELECT MIN(tx_date) as d FROM expenses WHERE user_id=?", (uid,)).fetchone()
        total_spent  = conn.execute("SELECT SUM(amount) as s FROM expenses WHERE user_id=? AND tx_type='expense'", (uid,)).fetchone()["s"] or 0
        total_earned = conn.execute("SELECT SUM(amount) as s FROM expenses WHERE user_id=? AND tx_type='income'", (uid,)).fetchone()["s"] or 0

    first_date = first_row["d"] if first_row and first_row["d"] else "—"
    msg = (
        f"📊 *Общая статистика*\n\n"
        f"📅 Первая запись: *{first_date}*\n"
        f"📝 Всего записей: *{total_count}*\n"
        f"   💸 Расходов: *{exp_count}*\n"
        f"   💚 Доходов:  *{inc_count}*\n\n"
        f"💸 Всего потрачено:  *{fmt(total_spent, cur)}*\n"
        f"💚 Всего заработано: *{fmt(total_earned, cur)}*\n"
        f"⚖️ Итоговый баланс:  *{fmt(total_earned - total_spent, cur)}*"
    )
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=settings_keyboard())
    return STATS_MENU


async def delete_all_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("⚠️ Да, удалить всё", callback_data="confirm_delete_all"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete_all"),
    ]])
    await update.message.reply_text(
        "⚠️ *Удалить все данные?*\n\nЭто действие нельзя отменить!",
        parse_mode="Markdown", reply_markup=kb
    )
    return STATS_MENU


async def delete_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if query.data == "confirm_delete_all":
        with get_db() as conn:
            conn.execute("DELETE FROM expenses  WHERE user_id=?", (uid,))
            conn.execute("DELETE FROM budgets   WHERE user_id=?", (uid,))
            conn.execute("DELETE FROM limits    WHERE user_id=?", (uid,))
            conn.execute("DELETE FROM goals     WHERE user_id=?", (uid,))
            conn.execute("DELETE FROM recurring WHERE user_id=?", (uid,))
            conn.execute("DELETE FROM settings  WHERE user_id=?", (uid,))
            conn.commit()
        await query.edit_message_text("✅ Все данные удалены.")
    else:
        await query.edit_message_text("❌ Удаление отменено.")
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ
# ════════════════════════════════════════════════════════════════════════════

async def export_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = ReplyKeyboardMarkup([
        [KeyboardButton("📤 Всё за этот месяц"), KeyboardButton("📤 Все данные")],
        [KeyboardButton("📤 Только расходы"),    KeyboardButton("📤 Только доходы")],
        [KeyboardButton("◀️ Назад")],
    ], resize_keyboard=True)
    await update.message.reply_text("📤 Выбери что экспортировать:", reply_markup=kb)
    return EXPORT_MENU


async def export_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text
    now  = datetime.now()

    if text == "📤 Всё за этот месяц":
        data     = export_csv(uid, None, now.year, now.month)
        filename = f"expenses_{now.strftime('%Y_%m')}.csv"
    elif text == "📤 Все данные":
        data     = export_csv(uid)
        filename = "all_expenses.csv"
    elif text == "📤 Только расходы":
        data     = export_csv(uid, "expense", now.year, now.month)
        filename = f"expenses_only_{now.strftime('%Y_%m')}.csv"
    elif text == "📤 Только доходы":
        data     = export_csv(uid, "income", now.year, now.month)
        filename = f"income_only_{now.strftime('%Y_%m')}.csv"
    else:
        return await go_main(update)

    await update.message.reply_document(
        document=io.BytesIO(data),
        filename=filename,
        caption=f"📊 Экспорт данных — {filename}"
    )
    await update.message.reply_text("Главное меню 👇", reply_markup=main_keyboard())
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ОБЩИЕ ХЭНДЛЕРЫ
# ════════════════════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"👋 Привет, {name}!\n\n"
        f"Я помогу отслеживать твои *расходы и доходы*.\n\n"
        f"*Возможности:*\n"
        f"• Добавление расходов и доходов\n"
        f"• 15 категорий расходов + 8 доходов\n"
        f"• Статистика по месяцам и категориям\n"
        f"• Месячный бюджет и лимиты\n"
        f"• Цели накоплений\n"
        f"• Регулярные платежи\n"
        f"• Экспорт в CSV\n"
        f"• Поиск по истории\n"
        f"• Поддержка ₽, $ и €",
        parse_mode="Markdown",
        reply_markup=main_keyboard()
    )
    return MAIN_MENU


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    return await go_main(update, "❌ Отменено. Главное меню:")


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await go_main(update)


async def unknown_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используй кнопки меню 👇", reply_markup=main_keyboard())
    return MAIN_MENU


# ════════════════════════════════════════════════════════════════════════════
#  ЗАПУСК
# ════════════════════════════════════════════════════════════════════════════

def build_conv_handler():
    cancel_filter  = filters.Regex("^❌ Отмена$")
    back_filter    = filters.Regex("^◀️ Назад$")

    return ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.Regex("^➕ Расход$"),            add_expense_start),
                MessageHandler(filters.Regex("^💚 Доход$"),             add_income_start),
                MessageHandler(filters.Regex("^📊 Статистика$"),        stats_menu),
                MessageHandler(filters.Regex("^📋 История$"),           show_history),
                MessageHandler(filters.Regex("^💰 Бюджет$"),            budget_menu),
                MessageHandler(filters.Regex("^🎯 Цели$"),              goals_menu_handler),
                MessageHandler(filters.Regex("^🔄 Регулярные$"),        recurring_menu_handler),
                MessageHandler(filters.Regex("^⚙️ Настройки$"),        settings_menu_handler),
                MessageHandler(filters.Regex("^🔍 Поиск$"),             search_start),
                MessageHandler(filters.Regex("^📤 Экспорт$"),           export_menu),
                CallbackQueryHandler(del_last_callback,            pattern="^del_last$"),
            ],
            ADD_AMOUNT:    [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_amount)],
            ADD_CATEGORY:  [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_category)],
            ADD_COMMENT:   [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_comment)],
            ADD_DATE:      [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_date)],
            STATS_MENU: [
                MessageHandler(filters.Regex("^📅 Этот месяц$"),     stats_this_month),
                MessageHandler(filters.Regex("^📅 Прошлый месяц$"),  stats_last_month),
                MessageHandler(filters.Regex("^📆 Год$"),            stats_year),
                MessageHandler(filters.Regex("^📆 Сравнить месяцы$"),stats_compare),
                MessageHandler(filters.Regex("^🗓 По дням недели$"), stats_weekday),
                MessageHandler(filters.Regex("^🏆 Топ трат$"),       stats_top),
                MessageHandler(filters.Regex("^💚 Доходы$"),         stats_income),
                MessageHandler(filters.Regex("^⚖️ Баланс$"),         stats_balance),
                MessageHandler(filters.Regex("^💱 Изменить валюту$"),currency_start),
                MessageHandler(filters.Regex("^📊 Моя статистика$"), my_stats),
                MessageHandler(filters.Regex("^🗑 Удалить данные$"),  delete_all_data),
                MessageHandler(back_filter, back_to_main),
                CallbackQueryHandler(delete_all_callback, pattern="^(confirm|cancel)_delete_all$"),
            ],
            BUDGET_MENU: [
                MessageHandler(filters.Regex("^💵 Установить бюджет$"), budget_set_start),
                MessageHandler(filters.Regex("^📊 Статус бюджета$"),    budget_status),
                MessageHandler(filters.Regex("^🔒 Лимит категории$"),   limit_set_start),
                MessageHandler(filters.Regex("^📋 Все лимиты$"),        limits_show),
                MessageHandler(filters.Regex("^🗑 Удалить лимит$"),     limit_delete),
                MessageHandler(back_filter, back_to_main),
                CallbackQueryHandler(limit_delete_callback, pattern="^dellim:"),
            ],
            SET_BUDGET_AMOUNT:   [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, budget_set_amount)],
            SET_LIMIT_CATEGORY:  [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, limit_set_category)],
            SET_LIMIT_AMOUNT:    [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, limit_set_amount)],
            GOALS_MENU: [
                MessageHandler(filters.Regex("^🎯 Новая цель$"),     goal_new_start),
                MessageHandler(filters.Regex("^📋 Мои цели$"),       goals_show),
                MessageHandler(filters.Regex("^💸 Пополнить цель$"), goal_contrib_start),
                MessageHandler(filters.Regex("^🗑 Удалить цель$"),   goal_delete),
                MessageHandler(back_filter, back_to_main),
                CallbackQueryHandler(goal_contrib_callback, pattern="^contrib:"),
                CallbackQueryHandler(goal_delete_callback,  pattern="^delgoal:"),
            ],
            SET_GOAL_NAME:   [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, goal_set_name)],
            SET_GOAL_AMOUNT: [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, goal_set_amount)],
            SET_GOAL_CONTRIB:[
                MessageHandler(cancel_filter, cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, goal_contrib_amount),
                CallbackQueryHandler(goal_contrib_callback, pattern="^contrib:"),
            ],
            RECURRING_MENU: [
                MessageHandler(filters.Regex("^➕ Добавить регулярный$"), recurring_add_start),
                MessageHandler(filters.Regex("^📋 Мои регулярные$"),      recurring_show),
                MessageHandler(filters.Regex("^✅ Внести сейчас$"),        recurring_apply),
                MessageHandler(filters.Regex("^🗑 Удалить регулярный$"),   recurring_delete),
                MessageHandler(back_filter, back_to_main),
                CallbackQueryHandler(recurring_apply_callback,  pattern="^apprec:"),
                CallbackQueryHandler(recurring_delete_callback, pattern="^delrec:"),
            ],
            ADD_RECURRING_AMOUNT:   [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, recurring_add_amount)],
            ADD_RECURRING_CATEGORY: [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, recurring_add_category)],
            ADD_RECURRING_FREQ:     [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, recurring_add_freq)],
            SEARCH_QUERY:  [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, search_execute)],
            SET_CURRENCY:  [MessageHandler(cancel_filter, cancel), MessageHandler(filters.TEXT & ~filters.COMMAND, currency_set)],
            EXPORT_MENU: [
                MessageHandler(back_filter, back_to_main),
                MessageHandler(filters.TEXT & ~filters.COMMAND, export_do),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(cancel_filter, cancel),
            MessageHandler(back_filter,   back_to_main),
            MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_msg),
        ],
        allow_reentry=True,
    )


async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Функция для отправки напоминания"""
    try:
        # ЗАМЕНИ 861111111 на свой ID из бота @userinfobot
        await context.bot.send_message(chat_id=861111111, text="🔔 Тимур, пора записать расходы за сегодня!")
    except Exception as e:
        print(f"Ошибка при отправке напоминания: {e}")

def main():
    # Создаем приложение. Токен берется из настроек Render (Environment)
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Подключаем твой обработчик диалогов
    application.add_handler(build_conv_handler())

    # Настройка напоминалки (18:00 UTC = 21:00 по МСК)
    if application.job_queue:
        application.job_queue.run_daily(
            send_reminder, 
            time=datetime.strptime("18:00", "%H:%M").time()
        )

    # Исправление ошибки запуска (event loop) для Python 3.14 на Render
    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    print("🤖 Бот успешно запущен и готов к работе!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    init_db()  # Инициализируем базу данных перед запуском
    main()
