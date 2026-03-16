"""
Transfer Marketplace Bot — v3.1
================================
Features:
  - Arabic / English language selection on start
  - Send / Receive flow (BOTH fully working)
  - Currencies: USD, EUR, SYP (admin-toggleable)
  - Quick-amount buttons (50, 100, 150, 200, 250, 300, 500, 1000) for USD & EUR
  - Payment method step  : Bank Transfer / Cash / Mobile Wallet
  - Delivery method step : Bank Transfer / Cash / Mobile Wallet / In-person
  - Live market rate snapshot (exchangerate.host) stored with every trade
  - Full error handling with try/except on every handler
  - Rate limiting (1.5 s between messages)
  - Admin panel  : /admin  — toggle currencies on/off
  - Admin stats  : /stats  — trade summary
  - Unknown command handler (bilingual)
  - File + console logging  (bot.log)
  - Graceful shutdown on SIGTERM/SIGINT
  - /about command
  - Posts confirmed trade to Telegram group
  - Sends confirmation email to user
  - 💹 Live exchange rates button on main menu (scraped from sp-today.com)

Dependencies:  pip install aiogram==2.25.1 requests beautifulsoup4
"""

import logging
import sqlite3
import requests
import smtplib
import signal
import sys
import asyncio
import re
from datetime import datetime
from email.mime.text import MIMEText
from collections import defaultdict
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ══════════════════════════════════════════════
#  CONFIGURATION  —  fill in before running
# ══════════════════════════════════════════════
API_TOKEN  = '8761727553:AAGYuGCN3GmeO6_g3PjI5e_dJ4-UjEj33EQ'
ADMIN_ID   = 6866161662         # Your personal Telegram user ID
GROUP_ID   = -1003698564133     # Target group
GMAIL_USER = "your-email@gmail.com"
GMAIL_PASS = "your-app-password"   # Use a Google App Password, not your main password

# ══════════════════════════════════════════════
#  LOGGING  (file + console)
# ══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("transfer_bot")

# ══════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════
QUICK_AMOUNTS   = [50, 100, 150, 200, 250, 300, 500, 1000]
RATE_LIMIT_SECS = 1.5
EMAIL_RE        = re.compile(r"^[^\@\s]+@[^\@\s]+\.[^\@\s]+$")

PAYMENT_METHODS = {
    "ar": [
        ("🏦 تحويل بنكي",    "bank"),
        ("💵 نقداً",          "cash"),
        ("📱 محفظة موبايل",  "wallet"),
    ],
    "en": [
        ("🏦 Bank Transfer", "bank"),
        ("💵 Cash",          "cash"),
        ("📱 Mobile Wallet", "wallet"),
    ],
}

DELIVERY_METHODS = {
    "ar": [
        ("🏦 تحويل بنكي",    "bank"),
        ("💵 نقداً",          "cash"),
        ("📱 محفظة موبايل",  "wallet"),
        ("🤝 وجهاً لوجه",    "inperson"),
    ],
    "en": [
        ("🏦 Bank Transfer", "bank"),
        ("💵 Cash",          "cash"),
        ("📱 Mobile Wallet", "wallet"),
        ("🤝 In-person",     "inperson"),
    ],
}

# ══════════════════════════════════════════════
#  RATE LIMITER
# ══════════════════════════════════════════════
_last_seen: dict = defaultdict(float)

def is_rate_limited(user_id: int) -> bool:
    now = asyncio.get_event_loop().time()
    if now - _last_seen[user_id] < RATE_LIMIT_SECS:
        return True
    _last_seen[user_id] = now
    return False

# ══════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════
def init_db() -> None:
    with sqlite3.connect("market.db") as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   TEXT,    username TEXT,   email    TEXT,
                type      TEXT,    asset    TEXT,
                amount    REAL,    price    REAL,
                currency  TEXT,    payment  TEXT,
                delivery  TEXT,    total    REAL,
                usd_snap  REAL,    eur_snap REAL,
                status    TEXT,    date     TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                item    TEXT PRIMARY KEY,
                enabled INTEGER DEFAULT 1
            )
        """)
        conn.execute("INSERT OR IGNORE INTO settings VALUES ('USD', 1)")
        conn.execute("INSERT OR IGNORE INTO settings VALUES ('EUR', 1)")
        conn.execute("INSERT OR IGNORE INTO settings VALUES ('SYP', 1)")
        conn.commit()

def get_enabled_currencies() -> list:
    with sqlite3.connect("market.db") as conn:
        rows = conn.execute(
            "SELECT item FROM settings WHERE enabled=1 ORDER BY item"
        ).fetchall()
    return [r[0] for r in rows]

def get_all_settings() -> list:
    with sqlite3.connect("market.db") as conn:
        return conn.execute("SELECT item, enabled FROM settings ORDER BY item").fetchall()

def toggle_setting(item: str) -> int:
    with sqlite3.connect("market.db") as conn:
        cur = conn.execute("SELECT enabled FROM settings WHERE item=?", (item,)).fetchone()
        new_val = 0 if cur[0] else 1
        conn.execute("UPDATE settings SET enabled=? WHERE item=?", (new_val, item))
        conn.commit()
    return new_val

def get_market_rates() -> tuple:
    """Fetch live USD→EUR rate. Returns (1.0, eur_rate) or (0.0, 0.0) on failure."""
    try:
        data = requests.get(
            "https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=5
        ).json()
        return 1.0, data["rates"]["EUR"]
    except Exception:
        try:
            data = requests.get(
                "https://api.exchangerate.host/latest?base=USD&symbols=EUR", timeout=5
            ).json()
            return 1.0, data["rates"]["EUR"]
        except Exception:
            return 0.0, 0.0

# ══════════════════════════════════════════════
#  CURRENCY FLAGS MAP
# ══════════════════════════════════════════════
CURRENCY_FLAGS = {
    "USD": "🇺🇸", "EUR": "🇪🇺", "TRY": "🇹🇷", "SAR": "🇸🇦",
    "AED": "🇦🇪", "EGP": "🇪🇬", "GBP": "🇬🇧", "QAR": "🇶🇦",
    "KWD": "🇰🇼", "JOD": "🇯🇴", "BHD": "🇧🇭", "OMR": "🇴🇲",
    "CAD": "🇨🇦", "AUD": "🇦🇺", "CHF": "🇨🇭", "SEK": "🇸🇪",
    "NOK": "🇳🇴", "DKK": "🇩🇰", "RUB": "🇷🇺", "LYD": "🇱🇾",
    "NZD": "🇳🇿", "SGD": "🇸🇬", "MYR": "🇲🇾", "BRL": "🇧🇷",
    "ZAR": "🇿🇦", "MAD": "🇲🇦", "DZD": "🇩🇿", "TND": "🇹🇳",
    "IQD": "🇮🇶", "IRR": "🇮🇷",
}

# ══════════════════════════════════════════════
#  SP-TODAY SCRAPER
# ══════════════════════════════════════════════
def get_sptoday_rates() -> list:
    """
    Fetch live exchange rates from sp-today API.
    Returns list of dicts with keys: code, name_ar, flag, buy, sell,
    change_day, change_week, change_month, change_year, day_high, day_low.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://sp-today.com",
            "Referer": "https://sp-today.com/currencies",
        }
        resp = requests.get(
            "https://api-v2.sp-today.com/api/v1/overview?lang=ar&city=damascus",
            headers=headers, timeout=10
        )
        payload = resp.json()
        raw_rates = payload["data"]["rates"]

        result = []
        for item in raw_rates:
            city = item.get("cities", {}).get("damascus", {})
            if not city:
                continue
            result.append({
                "code":         item.get("code", "???"),
                "name_ar":      item.get("name_ar", item.get("name", "")),
                "flag":         item.get("flag", "💵"),
                "buy":          city.get("buy", 0),
                "sell":         city.get("sell", 0),
                "change_day":   city.get("change", 0),
                "change_week":  city.get("change_week", 0),
                "change_month": city.get("change_month", 0),
                "change_year":  city.get("change_year", 0),
                "day_high":     city.get("day_high", 0),
                "day_low":      city.get("day_low", 0),
            })

        logger.info("sp-today API: fetched %d currency rates", len(result))
        return result

    except Exception as exc:
        logger.error("get_sptoday_rates: %s", exc)
        return []


def get_gold_rates() -> list:
    """
    Fetch live gold prices from sp-today API.
    Returns list of dicts with keys: karat, buy, sell,
    change_day, change_week, change_month, change_year, day_high, day_low.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://sp-today.com",
            "Referer": "https://sp-today.com/gold",
        }
        resp = requests.get(
            "https://api-v2.sp-today.com/api/v1/gold",
            headers=headers, timeout=10
        )
        payload = resp.json()
        raw_karats = payload["data"]["karats"]

        result = []
        for item in raw_karats:
            city = item.get("cities", {}).get("damascus", {})
            if not city:
                continue
            result.append({
                "karat":        item.get("karat", "??K"),
                "buy":          city.get("buy", 0),
                "sell":         city.get("sell", 0),
                "change_day":   city.get("change", 0),
                "change_week":  city.get("change_week", 0),
                "change_month": city.get("change_month", 0),
                "change_year":  city.get("change_year", 0),
                "day_high":     city.get("day_high", 0),
                "day_low":      city.get("day_low", 0),
            })

        logger.info("sp-today gold API: fetched %d karats", len(result))
        return result

    except Exception as exc:
        logger.error("get_gold_rates: %s", exc)
        return []


def format_gold_message(gold: list, lang: str) -> str:
    """Format gold rates into a clean Telegram HTML message."""
    if not gold:
        return (
            "⚠️ تعذّر تحميل أسعار الذهب حالياً. حاول مرة أخرى لاحقاً."
            if lang == "ar" else
            "⚠️ Could not load gold prices right now. Please try again later."
        )

    now = datetime.now().strftime("%H:%M")
    if lang == "ar":
        title = f"🥇 <b>أسعار الذهب في سوريا (ل.س)</b>\n🕐 آخر تحديث: {now}"
        buy_l  = "شراء"
        sell_l = "بيع"
        high_l = "أعلى"
        low_l  = "أدنى"
        gram_l = "غرام"
    else:
        title = f"🥇 <b>Gold Prices in Syria (SYP)</b>\n🕐 Last updated: {now}"
        buy_l  = "Buy"
        sell_l = "Sell"
        high_l = "High"
        low_l  = "Low"
        gram_l = "gram"

    lines = [title, ""]

    for g in gold:
        karat  = g["karat"]
        buy    = f"{g['buy']:,}"
        sell   = f"{g['sell']:,}"
        high   = f"{g['day_high']:,}"
        low    = f"{g['day_low']:,}"
        chg_day   = g["change_day"]
        chg_week  = g["change_week"]

        # Daily badge
        if chg_day > 0:
            day_badge = f"📈 +{chg_day:.2f}%"
        elif chg_day < 0:
            day_badge = f"📉 {chg_day:.2f}%"
        else:
            day_badge = "➖ —"

        lines.append(f"┌ 🥇 <b>{karat}</b>  ({gram_l})  {day_badge}")
        lines.append(f"│  💰 <b>{buy_l}:</b>  <code>{buy}</code>   <b>{sell_l}:</b>  <code>{sell}</code>")
        lines.append(f"│  📊 <b>{high_l}:</b> <code>{high}</code>   <b>{low_l}:</b> <code>{low}</code>")
        lines.append(f"└  {_fmt_change(chg_week, 'أسبوعي', 'Week', lang)}")
        lines.append("")  # blank line between karats

    source = (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📌 <a href='https://sp-today.com/gold'>sp-today.com/gold</a>"
    )
    lines.append(source)
    return "\n".join(lines)


def _fmt_change(val: float, label_ar: str, label_en: str, lang: str) -> str:
    """Format a change percentage value with arrow, color indicator and label."""
    label = label_ar if lang == "ar" else label_en
    if val > 0:
        return f"📈 <b>{label}:</b> <i>+{val:.2f}%</i>"
    elif val < 0:
        return f"📉 <b>{label}:</b> <i>{val:.2f}%</i>"
    else:
        return f"➖ <b>{label}:</b> <i>0.00%</i>"


def format_rates_message(rates: list, lang: str) -> str:
    """Format the API rates into a clean Telegram HTML message."""
    if not rates:
        return (
            "⚠️ تعذّر تحميل الأسعار حالياً. حاول مرة أخرى لاحقاً."
            if lang == "ar"
            else "⚠️ Could not load rates right now. Please try again later."
        )

    now = datetime.now().strftime("%H:%M")
    if lang == "ar":
        title    = f"💹 <b>أسعار الصرف مقابل الليرة السورية</b>\n🕐 آخر تحديث: {now}"
        buy_l    = "شراء"
        sell_l   = "بيع"
        high_l   = "أعلى"
        low_l    = "أدنى"
    else:
        title    = f"💹 <b>Exchange Rates vs Syrian Pound (SYP)</b>\n🕐 Last updated: {now}"
        buy_l    = "Buy"
        sell_l   = "Sell"
        high_l   = "High"
        low_l    = "Low"

    lines = [title, ""]

    for r in rates:
        flag         = r["flag"]
        code         = r["code"]
        name_display = r["name_ar"] if lang == "ar" else code
        buy          = f"{r['buy']:,}"
        sell         = f"{r['sell']:,}"
        high         = f"{r['day_high']:,}"
        low          = f"{r['day_low']:,}"
        chg_day      = r["change_day"]
        chg_week     = r["change_week"]
        chg_month    = r["change_month"]
        chg_year     = r.get("change_year", 0)

        # Daily badge: show change if non-zero, else show dash
        if chg_day > 0:
            day_badge = f"📈 +{chg_day:.2f}%"
        elif chg_day < 0:
            day_badge = f"📉 {chg_day:.2f}%"
        else:
            day_badge = "➖ —"
        lines.append(f"┌ {flag} <b>{code}</b>  {name_display}  <code>100</code>  {day_badge}")
        lines.append(f"│  💰 <b>{buy_l}:</b>  <code>{buy}</code>   <b>{sell_l}:</b>  <code>{sell}</code>")
        lines.append(f"│  📊 <b>{high_l}:</b> <code>{high}</code>   <b>{low_l}:</b> <code>{low}</code>")
        lines.append(f"└  {_fmt_change(chg_week, 'أسبوعي', 'Week', lang)}")
        lines.append("")   # blank line between currencies

    source = (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📌 <a href='https://sp-today.com/currencies'>sp-today.com</a>"
    )
    lines.append(source)
    return "\n".join(lines)


def save_trade(data: dict, user: types.User) -> tuple:
    total = data["amount"] * data["price"]
    date  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    usd_snap, eur_snap = get_market_rates()
    with sqlite3.connect("market.db") as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (user_id, username, email, type, asset, amount, price, currency,
                payment, delivery, total, usd_snap, eur_snap, status, date)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                str(user.id), user.username or "", data.get("email", ""),
                data["type"], data["asset"], data["amount"], data["price"],
                data["currency"], data.get("payment", ""), data.get("delivery", ""),
                total, usd_snap, eur_snap, "PENDING", date,
            ),
        )
        trade_id = cur.lastrowid
        conn.commit()
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
    return row

def get_stats() -> dict:
    with sqlite3.connect("market.db") as conn:
        total     = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        buy_cnt   = conn.execute("SELECT COUNT(*) FROM trades WHERE type='buy'").fetchone()[0]
        sell_cnt  = conn.execute("SELECT COUNT(*) FROM trades WHERE type='sell'").fetchone()[0]
        pending   = conn.execute("SELECT COUNT(*) FROM trades WHERE status='PENDING'").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM trades WHERE status='COMPLETED'").fetchone()[0]
        volumes   = conn.execute(
            "SELECT asset, SUM(amount) FROM trades GROUP BY asset"
        ).fetchall()
    return {
        "total": total, "buy": buy_cnt, "sell": sell_cnt,
        "pending": pending, "completed": completed, "volumes": volumes,
    }

# ══════════════════════════════════════════════
#  EMAIL
# ══════════════════════════════════════════════
def send_confirmation_email(recipient: str, row: tuple) -> None:
    # row cols: id, user_id, username, email, type, asset, amount, price,
    #           currency, payment, delivery, total, usd_snap, eur_snap, status, date
    try:
        rate_line = f"1 USD = {row[13]:.4f} EUR" if row[13] else "N/A"
        body = (
            f"تأكيد الصفقة / Trade Confirmation #{row[0]}\n\n"
            f"النوع / Type      : {row[4]}\n"
            f"العملة / Asset    : {row[5]}\n"
            f"المبلغ / Amount   : {row[6]:,.2f}\n"
            f"السعر / Price     : {row[7]:,.2f} {row[8]}\n"
            f"الدفع / Payment   : {row[9]}\n"
            f"التسليم / Deliver : {row[10]}\n"
            f"الإجمالي / Total  : {row[11]:,.2f} {row[8]}\n"
            f"سعر السوق / Rate  : {rate_line}\n"
            f"الحالة / Status   : {row[14]}\n"
            f"التاريخ / Date    : {row[15]}\n"
        )
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"إيصال صفقة / Trade Receipt #{row[0]}"
        msg["From"]    = GMAIL_USER
        msg["To"]      = recipient
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            server.send_message(msg)
        logger.info("Confirmation email sent to %s", recipient)
    except Exception as exc:
        logger.error("Email error: %s", exc)

# ══════════════════════════════════════════════
#  BOT + DISPATCHER
# ══════════════════════════════════════════════
bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp  = Dispatcher(bot, storage=MemoryStorage())

# ══════════════════════════════════════════════
#  FSM STATES
# ══════════════════════════════════════════════
class MarketFlow(StatesGroup):
    language = State()
    type     = State()
    asset    = State()
    amount   = State()
    currency = State()
    price    = State()
    payment  = State()   # ← payment method (bank/cash/wallet)
    delivery = State()   # ← delivery method (bank/cash/wallet/inperson)
    email    = State()

# ══════════════════════════════════════════════
#  LOCALISED STRINGS
# ══════════════════════════════════════════════
TX = {
    "ar": {
        "welcome": (
            "🌟 <b>مرحبًا بك في بوت التحويل!</b> 🌟\n\n"
            "نسعى لجعل تجربة التحويل لديك سلسة، آمنة وذكية.\n\n"
            "💰 اختر العملة التي ترغب في استلامها\n"
            "💳 واختر طريقة الدفع المفضلة لديك\n\n"
            "للبدء، اضغط على /start واستعد لتجربة تحويل فريدة وسريعة! 🚀\n\n"
            "اختر لغتك للبدء 👇\n"
            "<i>Select your language to begin 👇</i>"
        ),
        "select_type"     : "📋 <b>اختر نوع العملية:</b>",
        "btn_buy"         : "🛒 شراء",
        "btn_sell"        : "💸 بيع",
        "select_asset_b"  : "💱 <b>اختر العملة التي تريد شراءها:</b>",
        "select_asset_sl" : "💱 <b>اختر العملة التي تريد بيعها:</b>",
        "enter_amount"    : "💵 <b>أدخل المبلغ:</b>\n\nأو اختر مبلغًا سريعًا 👇",
        "custom_amount"   : "أو اكتب مبلغًا مخصصًا 👇",
        "select_pay_cur"  : "🌍 <b>اختر عملة الدفع:</b>",
        "enter_price"     : "💲 <b>أدخل السعر المطلوب لكل وحدة:</b>",
        "select_payment"  : "💳 <b>اختر طريقة الدفع:</b>",
        "select_delivery" : "🚚 <b>اختر طريقة التسليم:</b>",
        "enter_email"     : "📧 <b>أدخل بريدك الإلكتروني للتأكيد (اختياري):</b>",
        "btn_skip_email"  : "⏭️ تخطي",
        "btn_rates"       : "💹 أسعار الصرف",
        "btn_gold"        : "🥇 أسعار الذهب",
        "btn_back"        : "🏠 العودة للقائمة",
        "bad_amount"      : "⚠️ مبلغ غير صحيح. الرجاء إدخال رقم موجب.",
        "bad_email"       : "⚠️ بريد إلكتروني غير صحيح. أدخله مجدداً أو اضغط تخطي.",
        "about": (
            "ℹ️ <b>عن البوت</b>\n\n"
            "بوت التحويل v3.1\n"
            "منصة آمنة وسريعة لتحويل العملات.\n\n"
            "الأوامر المتاحة:\n"
            "/start — بدء تحويل جديد\n"
            "/about — معلومات عن البوت\n\n"
            "للدعم تواصل مع المشرف."
        ),
        "type_lbl"     : "النوع",
        "asset_lbl"    : "العملة",
        "amount_lbl"   : "المبلغ",
        "price_lbl"    : "السعر",
        "payment_lbl"  : "طريقة الدفع",
        "delivery_lbl" : "طريقة التسليم",
        "total_lbl"    : "الإجمالي",
        "rate_lbl"     : "سعر السوق",
        "date_lbl"     : "التاريخ",
        "done_title"   : "✅ تم تسجيل طلبك بنجاح!",
    },
    "en": {
        "welcome": (
            "🌟 <b>Welcome to the Transfer Bot!</b> 🌟\n\n"
            "We make your transfer experience smooth, secure and smart.\n\n"
            "💰 Choose the currency you want to receive\n"
            "💳 Choose your preferred payment method\n\n"
            "Press /start to begin a fast and unique transfer experience! 🚀\n\n"
            "<i>اختر لغتك للبدء 👇</i>\n"
            "Select your language to begin 👇"
        ),
        "select_type"     : "📋 <b>Select operation type:</b>",
        "btn_buy"         : "🛒 Buy",
        "btn_sell"        : "💸 Sell",
        "select_asset_b"  : "💱 <b>Select currency to buy:</b>",
        "select_asset_sl" : "💱 <b>Select currency to sell:</b>",
        "enter_amount"    : "💵 <b>Enter the amount:</b>\n\nOr pick a quick amount 👇",
        "custom_amount"   : "Or type a custom amount 👇",
        "select_pay_cur"  : "🌍 <b>Select payment currency:</b>",
        "enter_price"     : "💲 <b>Enter the price per unit:</b>",
        "select_payment"  : "💳 <b>Select payment method:</b>",
        "select_delivery" : "🚚 <b>Select delivery method:</b>",
        "enter_email"     : "📧 <b>Enter your email for confirmation (optional):</b>",
        "btn_skip_email"  : "⏭️ Skip",
        "btn_rates"       : "💹 Exchange Rates",
        "btn_gold"        : "🥇 Gold Prices",
        "btn_back"        : "🏠 Back to Menu",
        "bad_amount"      : "⚠️ Invalid amount. Please enter a positive number.",
        "bad_email"       : "⚠️ Invalid email address. Try again or press Skip.",
        "about": (
            "ℹ️ <b>About the Bot</b>\n\n"
            "Transfer Bot v3.1\n"
            "A fast and secure currency transfer platform.\n\n"
            "Available commands:\n"
            "/start — Begin a new transfer\n"
            "/about — Bot information\n\n"
            "Contact the admin for support."
        ),
        "type_lbl"     : "Type",
        "asset_lbl"    : "Asset",
        "amount_lbl"   : "Amount",
        "price_lbl"    : "Price",
        "payment_lbl"  : "Payment Method",
        "delivery_lbl" : "Delivery Method",
        "total_lbl"    : "Total",
        "rate_lbl"     : "Market Rate",
        "date_lbl"     : "Date",
        "done_title"   : "✅ Your order has been submitted!",
    },
}

def t(lang: str, key: str) -> str:
    return TX.get(lang, TX["en"]).get(key, key)

# ══════════════════════════════════════════════
#  KEYBOARD HELPERS
# ══════════════════════════════════════════════
def _back_btn(lang: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(t(lang, "btn_back"), callback_data="go_start")

def lang_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("🇸🇦 العربية", callback_data="lang_ar"),
        InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
    )

def type_kb(lang: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(t(lang, "btn_buy"),  callback_data="type_buy"),
        InlineKeyboardButton(t(lang, "btn_sell"), callback_data="type_sell"),
    )
    return kb

def asset_kb(lang: str) -> InlineKeyboardMarkup:
    currencies = get_enabled_currencies()
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(*[InlineKeyboardButton(c, callback_data=f"ast_{c}") for c in currencies])
    kb.add(_back_btn(lang))
    return kb

def amount_kb(lang: str, asset: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    if asset in ("USD", "EUR"):
        kb.add(*[InlineKeyboardButton(str(a), callback_data=f"amt_{a}") for a in QUICK_AMOUNTS])
    kb.add(_back_btn(lang))
    return kb

def pay_cur_kb(lang: str) -> InlineKeyboardMarkup:
    currencies = get_enabled_currencies()
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(*[InlineKeyboardButton(c, callback_data=f"cur_{c}") for c in currencies])
    kb.add(_back_btn(lang))
    return kb

def payment_method_kb(lang: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for label, value in PAYMENT_METHODS.get(lang, PAYMENT_METHODS["en"]):
        kb.add(InlineKeyboardButton(label, callback_data=f"pay_{value}"))
    kb.add(_back_btn(lang))
    return kb

def delivery_method_kb(lang: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for label, value in DELIVERY_METHODS.get(lang, DELIVERY_METHODS["en"]):
        kb.add(InlineKeyboardButton(label, callback_data=f"del_{value}"))
    kb.add(_back_btn(lang))
    return kb

def back_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup().add(_back_btn(lang))

def admin_kb(settings: list) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    for item, enabled in settings:
        icon = "✅" if enabled else "❌"
        kb.add(InlineKeyboardButton(f"{icon}  {item}", callback_data=f"toggle_{item}"))
    return kb

# ══════════════════════════════════════════════
#  WELCOME HELPER
# ══════════════════════════════════════════════
WELCOME_TEXT = (
    "🌟 <b>مرحبًا بك في بوت التحويل!</b> 🌟\n\n"
    "نسعى لجعل تجربة التحويل لديك سلسة، آمنة وذكية.\n\n"
    "💰 اختر العملة التي ترغب في استلامها\n"
    "💳 واختر طريقة الدفع المفضلة لديك\n\n"
    "للبدء، اضغط على /start واستعد لتجربة تحويل فريدة وسريعة! 🚀\n\n"
    "اختر لغتك للبدء 👇\n"
    "<i>Select your language to begin 👇</i>"
)

async def show_welcome(target, edit: bool = False) -> None:
    if edit:
        await target.edit_text(WELCOME_TEXT, reply_markup=lang_kb())
    else:
        await target.answer(WELCOME_TEXT, reply_markup=lang_kb())

# ══════════════════════════════════════════════
#  ── /start ──
# ══════════════════════════════════════════════
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

PERSISTENT_KB = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3).add(
    KeyboardButton("🏠 القائمة / Menu"),
    KeyboardButton("💹 الأسعار / Rates"),
    KeyboardButton("🥇 الذهب / Gold"),
)

@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    try:
        await state.finish()
        await message.answer("👇", reply_markup=PERSISTENT_KB)
        await show_welcome(message)
        await MarketFlow.language.set()
    except Exception as exc:
        logger.error("cmd_start: %s", exc)

# ══════════════════════════════════════════════
#  ── /rates shortcut ──
# ══════════════════════════════════════════════
@dp.message_handler(commands=["rates"], state="*")
async def cmd_rates(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "ar")
        loading = "⏳ جارٍ تحميل الأسعار..." if lang == "ar" else "⏳ Loading rates..."
        msg = await message.answer(loading)
        rates = get_sptoday_rates()
        text  = format_rates_message(rates, lang)
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🏠 القائمة / Menu", callback_data="back_to_main")
        )
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as exc:
        logger.error("cmd_rates: %s", exc)

# ══════════════════════════════════════════════
#  ── /gold command ──
# ══════════════════════════════════════════════
@dp.message_handler(commands=["gold"], state="*")
async def cmd_gold(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "ar")
        loading = "⏳ جارٍ تحميل أسعار الذهب..." if lang == "ar" else "⏳ Loading gold prices..."
        msg = await message.answer(loading)
        gold = get_gold_rates()
        text = format_gold_message(gold, lang)
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🏠 القائمة / Menu", callback_data="back_to_main")
        )
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as exc:
        logger.error("cmd_gold: %s", exc)

# ══════════════════════════════════════════════
#  ── /help ──
# ══════════════════════════════════════════════
@dp.message_handler(commands=["help"], state="*")
async def cmd_help(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "ar")
        if lang == "ar":
            text = (
                "❓ <b>المساعدة</b>\n\n"
                "🏠 /start — القائمة الرئيسية\n"
                "💹 /rates — أسعار الصرف الحية\n"

                "📊 /stats — إحصائيات (للمشرف)\n"
                "⚙️ /admin — لوحة التحكم (للمشرف)\n\n"
                "للبدء اضغط /start"
            )
        else:
            text = (
                "❓ <b>Help</b>\n\n"
                "🏠 /start — Main Menu\n"
                "💹 /rates — Live Exchange Rates\n"

                "📊 /stats — Statistics (Admin)\n"
                "⚙️ /admin — Admin Panel (Admin)\n\n"
                "Press /start to begin"
            )
        await message.answer(text)
    except Exception as exc:
        logger.error("cmd_help: %s", exc)

# ── Persistent keyboard button handlers ──
@dp.message_handler(lambda m: m.text in ["🏠 القائمة / Menu"], state="*")
async def kb_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await show_welcome(message)
    await MarketFlow.language.set()

@dp.message_handler(lambda m: m.text in ["🥇 الذهب / Gold"], state="*")
async def kb_gold(message: types.Message, state: FSMContext):
    await cmd_gold(message, state)

@dp.message_handler(lambda m: m.text in ["💹 الأسعار / Rates"], state="*")
async def kb_rates(message: types.Message, state: FSMContext):
    await cmd_rates(message, state)


# ══════════════════════════════════════════════
#  ── /stats  (admin only) ──
# ══════════════════════════════════════════════
@dp.message_handler(commands=["stats"], state="*")
async def cmd_stats(message: types.Message):
    try:
        if message.from_user.id != ADMIN_ID:
            await message.answer("🚫 Unauthorised.")
            return
        s = get_stats()
        vol_lines = "\n".join(
            f"  ├ {asset}: <b>{vol:,.2f}</b>" for asset, vol in s["volumes"]
        ) or "  └ No trades yet"
        text = (
            "📊 <b>إحصائيات / Trade Statistics</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 الإجمالي / Total  : <b>{s['total']}</b>\n"
            f"  ├ 🛒 شراء / Buy    : <b>{s['buy']}</b>\n"
            f"  └ 💸 بيع / Sell    : <b>{s['sell']}</b>\n\n"
            f"🔄 الحالة / Status\n"
            f"  ├ ⏳ Pending        : <b>{s['pending']}</b>\n"
            f"  └ ✅ Completed      : <b>{s['completed']}</b>\n\n"
            f"💰 الحجم / Volume by Asset\n"
            f"{vol_lines}"
        )
        await message.answer(text)
    except Exception as exc:
        logger.error("cmd_stats: %s", exc)

# ══════════════════════════════════════════════
#  ── /admin ──
# ══════════════════════════════════════════════
@dp.message_handler(commands=["admin"])
async def cmd_admin(message: types.Message):
    try:
        if message.from_user.id != ADMIN_ID:
            await message.answer("🚫 Unauthorised.")
            return
        settings = get_all_settings()
        await message.answer(
            "⚙️ <b>Admin Panel</b>\nTap a currency to toggle it on/off:",
            reply_markup=admin_kb(settings),
        )
    except Exception as exc:
        logger.error("cmd_admin: %s", exc)

@dp.callback_query_handler(lambda c: c.data.startswith("toggle_"))
async def cb_toggle(callback: types.CallbackQuery):
    try:
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("🚫 Unauthorised", show_alert=True)
            return
        item    = callback.data.split("_", 1)[1]
        new_val = toggle_setting(item)
        status  = "enabled ✅" if new_val else "disabled ❌"
        await callback.message.edit_reply_markup(reply_markup=admin_kb(get_all_settings()))
        await callback.answer(f"{item} {status}")
    except Exception as exc:
        logger.error("cb_toggle: %s", exc)

# ══════════════════════════════════════════════
#  ── LANGUAGE ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("lang_"), state=MarketFlow.language)
async def cb_language(callback: types.CallbackQuery, state: FSMContext):
    try:
        lang = callback.data.split("_", 1)[1]
        await state.update_data(language=lang)
        await callback.message.edit_text(t(lang, "select_type"), reply_markup=type_kb(lang))
        await MarketFlow.type.set()
    except Exception as exc:
        logger.error("cb_language: %s", exc)

# ══════════════════════════════════════════════
#  ── TYPE  (Buy / Sell) ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("type_"), state=MarketFlow.type)
async def cb_type(callback: types.CallbackQuery, state: FSMContext):
    try:
        data       = await state.get_data()
        lang       = data.get("language", "en")
        trade_type = callback.data.split("_", 1)[1]
        await state.update_data(type=trade_type)

        asset_key_map = {
            "buy":  "select_asset_b",
            "sell": "select_asset_sl",
        }
        asset_key = asset_key_map.get(trade_type, "select_asset_b")
        await callback.message.edit_text(t(lang, asset_key), reply_markup=asset_kb(lang))
        await MarketFlow.asset.set()
    except Exception as exc:
        logger.error("cb_type: %s", exc)

# ══════════════════════════════════════════════
#  ── ASSET ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("ast_"), state=MarketFlow.asset)
async def cb_asset(callback: types.CallbackQuery, state: FSMContext):
    try:
        data  = await state.get_data()
        lang  = data.get("language", "en")
        asset = callback.data.split("_", 1)[1]
        await state.update_data(asset=asset)

        text = t(lang, "enter_amount")
        if asset not in ("USD", "EUR"):
            text += f"\n{t(lang, 'custom_amount')}"
        await callback.message.edit_text(text, reply_markup=amount_kb(lang, asset))
        await MarketFlow.amount.set()
    except Exception as exc:
        logger.error("cb_asset: %s", exc)

# ══════════════════════════════════════════════
#  ── AMOUNT  (quick button) ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("amt_"), state=MarketFlow.amount)
async def cb_amount_quick(callback: types.CallbackQuery, state: FSMContext):
    try:
        data   = await state.get_data()
        lang   = data.get("language", "en")
        amount = float(callback.data.split("_", 1)[1])
        await state.update_data(amount=amount)
        await callback.message.edit_text(t(lang, "select_pay_cur"), reply_markup=pay_cur_kb(lang))
        await MarketFlow.currency.set()
    except Exception as exc:
        logger.error("cb_amount_quick: %s", exc)

# ══════════════════════════════════════════════
#  ── AMOUNT  (typed) ──
# ══════════════════════════════════════════════
@dp.message_handler(state=MarketFlow.amount)
async def msg_amount(message: types.Message, state: FSMContext):
    try:
        if is_rate_limited(message.from_user.id):
            return
        data = await state.get_data()
        lang = data.get("language", "en")
        try:
            amount = float(message.text.replace(",", "").strip())
            assert amount > 0
        except (ValueError, AssertionError):
            await message.answer(t(lang, "bad_amount"))
            return
        await state.update_data(amount=amount)
        await message.answer(t(lang, "select_pay_cur"), reply_markup=pay_cur_kb(lang))
        await MarketFlow.currency.set()
    except Exception as exc:
        logger.error("msg_amount: %s", exc)

# ══════════════════════════════════════════════
#  ── PAYMENT CURRENCY ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("cur_"), state=MarketFlow.currency)
async def cb_currency(callback: types.CallbackQuery, state: FSMContext):
    try:
        data     = await state.get_data()
        lang     = data.get("language", "en")
        currency = callback.data.split("_", 1)[1]
        await state.update_data(currency=currency)
        await callback.message.edit_text(t(lang, "enter_price"), reply_markup=back_kb(lang))
        await MarketFlow.price.set()
    except Exception as exc:
        logger.error("cb_currency: %s", exc)

# ══════════════════════════════════════════════
#  ── PRICE ──
# ══════════════════════════════════════════════
@dp.message_handler(state=MarketFlow.price)
async def msg_price(message: types.Message, state: FSMContext):
    try:
        if is_rate_limited(message.from_user.id):
            return
        data = await state.get_data()
        lang = data.get("language", "en")
        try:
            price = float(message.text.replace(",", "").strip())
            assert price > 0
        except (ValueError, AssertionError):
            await message.answer(t(lang, "bad_amount"))
            return
        await state.update_data(price=price)
        await message.answer(t(lang, "select_payment"), reply_markup=payment_method_kb(lang))
        await MarketFlow.payment.set()
    except Exception as exc:
        logger.error("msg_price: %s", exc)

# ══════════════════════════════════════════════
#  ── PAYMENT METHOD ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("pay_"), state=MarketFlow.payment)
async def cb_payment(callback: types.CallbackQuery, state: FSMContext):
    try:
        data    = await state.get_data()
        lang    = data.get("language", "en")
        value   = callback.data.split("_", 1)[1]
        methods = PAYMENT_METHODS.get(lang, PAYMENT_METHODS["en"])
        label   = next((lbl for lbl, val in methods if val == value), value)
        await state.update_data(payment=label)
        await callback.message.edit_text(t(lang, "select_delivery"), reply_markup=delivery_method_kb(lang))
        await MarketFlow.delivery.set()
    except Exception as exc:
        logger.error("cb_payment: %s", exc)

# ══════════════════════════════════════════════
#  ── DELIVERY METHOD ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("del_"), state=MarketFlow.delivery)
async def cb_delivery(callback: types.CallbackQuery, state: FSMContext):
    try:
        data    = await state.get_data()
        lang    = data.get("language", "en")
        value   = callback.data.split("_", 1)[1]
        methods = DELIVERY_METHODS.get(lang, DELIVERY_METHODS["en"])
        label   = next((lbl for lbl, val in methods if val == value), value)
        await state.update_data(delivery=label)
        email_kb = InlineKeyboardMarkup(row_width=2).add(
            InlineKeyboardButton(t(lang, "btn_skip_email"), callback_data="skip_email"),
            InlineKeyboardButton(t(lang, "btn_back"),       callback_data="go_back"),
        )
        await callback.message.edit_text(t(lang, "enter_email"), reply_markup=email_kb)
        await MarketFlow.email.set()
    except Exception as exc:
        logger.error("cb_delivery: %s", exc)

# ══════════════════════════════════════════════
#  ── EMAIL + FINAL SUMMARY ──
# ══════════════════════════════════════════════
async def _finish_trade(lang: str, data: dict, user, send_to: str | None, reply_target):
    """Shared logic: save trade, build summary, post to group, send email if provided."""
    row       = save_trade(data, user)
    total     = row[11]
    eur_snap  = row[13]
    rate_line = f"1 USD = {eur_snap:.4f} EUR" if eur_snap else "N/A"

    summary = (
        f"{t(lang, 'done_title')}\n\n"
        f"🆔 <b>#{row[0]}</b>\n"
        f"{'━' * 26}\n"
        f"📌 {t(lang, 'type_lbl')}      : <b>{data['type'].upper()}</b>\n"
        f"💱 {t(lang, 'asset_lbl')}     : <b>{data['asset']}</b>\n"
        f"📦 {t(lang, 'amount_lbl')}    : <b>{data['amount']:,.2f}</b>\n"
        f"💲 {t(lang, 'price_lbl')}     : <b>{data['price']:,.2f} {data['currency']}</b>\n"
        f"💳 {t(lang, 'payment_lbl')}   : <b>{data.get('payment', '-')}</b>\n"
        f"🚚 {t(lang, 'delivery_lbl')}  : <b>{data.get('delivery', '-')}</b>\n"
        f"💰 {t(lang, 'total_lbl')}     : <b>{total:,.2f} {data['currency']}</b>\n"
        f"📈 {t(lang, 'rate_lbl')}      : <b>{rate_line}</b>\n"
        f"📅 {t(lang, 'date_lbl')}      : {row[15]}\n"
    )

    if hasattr(reply_target, 'message'):
        await reply_target.message.edit_text(summary, reply_markup=back_kb(lang))
    else:
        await reply_target.answer(summary, reply_markup=back_kb(lang))

    try:
        await bot.send_message(GROUP_ID, summary)
    except Exception as ge:
        logger.warning("Group post failed: %s", ge)

    if send_to:
        send_confirmation_email(send_to, row)


@dp.callback_query_handler(lambda c: c.data == "skip_email", state=MarketFlow.email)
async def cb_skip_email(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "en")
        await state.update_data(email="")
        data = await state.get_data()
        await _finish_trade(lang, data, callback.from_user, None, callback)
        await state.finish()
    except Exception as exc:
        logger.error("cb_skip_email: %s", exc)


@dp.message_handler(state=MarketFlow.email)
async def msg_email(message: types.Message, state: FSMContext):
    try:
        if is_rate_limited(message.from_user.id):
            return
        data  = await state.get_data()
        lang  = data.get("language", "en")
        email = message.text.strip()

        if not EMAIL_RE.match(email):
            email_kb = InlineKeyboardMarkup(row_width=2).add(
                InlineKeyboardButton(t(lang, "btn_skip_email"), callback_data="skip_email"),
                InlineKeyboardButton(t(lang, "btn_back"),       callback_data="go_back"),
            )
            await message.answer(t(lang, "bad_email"), reply_markup=email_kb)
            return

        await state.update_data(email=email)
        data = await state.get_data()
        await _finish_trade(lang, data, message.from_user, email, message)
        await state.finish()

    except Exception as exc:
        logger.error("msg_email: %s", exc)

# ══════════════════════════════════════════════
#  ── EXCHANGE RATES (sp-today.com) ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data == "show_gold", state="*")
async def cb_show_gold(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "ar")
        loading = "⏳ جارٍ تحميل أسعار الذهب..." if lang == "ar" else "⏳ Loading gold prices..."
        await callback.answer(loading)
        gold = get_gold_rates()
        text = format_gold_message(gold, lang)
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton(t(lang, "btn_back"), callback_data="go_start")
        )
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as exc:
        logger.error("cb_show_gold: %s", exc)


@dp.callback_query_handler(lambda c: c.data == "show_rates", state="*")
async def cb_show_rates(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "ar")

        # Show a loading message while fetching
        loading = "⏳ جارٍ تحميل الأسعار..." if lang == "ar" else "⏳ Loading rates..."
        await callback.answer(loading)

        rates = get_sptoday_rates()
        text  = format_rates_message(rates, lang)

        # Back button returns to the type-selection menu
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton(t(lang, "btn_back"), callback_data="go_start")
        )
        await callback.message.edit_text(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.error("cb_show_rates: %s", exc)


# ══════════════════════════════════════════════
#  ── BACK TO START ──
# ══════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data in ("go_start", "back_to_main"), state="*")
async def cb_go_start(callback: types.CallbackQuery, state: FSMContext):
    try:
        await state.finish()
        await show_welcome(callback.message, edit=True)
        await MarketFlow.language.set()
    except Exception as exc:
        logger.error("cb_go_start: %s", exc)

# ══════════════════════════════════════════════
#  ── UNKNOWN COMMANDS ──
# ══════════════════════════════════════════════
@dp.message_handler(lambda m: m.text and m.text.startswith("/"))
async def unknown_command(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        lang = data.get("language", "en")
        if lang == "ar":
            await message.answer("❓ أمر غير معروف.\nاكتب /start للبدء.")
        else:
            await message.answer("❓ Unknown command.\nType /start to begin.")
    except Exception as exc:
        logger.error("unknown_command: %s", exc)

# ══════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════
def _handle_signal(signum, frame):
    logger.info("Received signal %s — shutting down gracefully.", signum)
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)

# ══════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════
async def on_startup(dispatcher):
    await bot.delete_webhook(drop_pending_updates=True)
    # ── Set bot command menu (the "/" button in Telegram) ──
    from aiogram.types import BotCommand, BotCommandScopeDefault
    commands = [
        BotCommand("start",   "🏠 القائمة الرئيسية / Main Menu"),
        BotCommand("rates",   "💹 أسعار الصرف / Exchange Rates"),
        BotCommand("gold",    "🥇 أسعار الذهب / Gold Prices"),
        BotCommand("stats",   "📊 الإحصائيات / Statistics (Admin)"),
        BotCommand("admin",   "⚙️ لوحة التحكم / Admin Panel"),

        BotCommand("help",    "❓ المساعدة / Help"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    logger.info("✅ Bot is running!")

if __name__ == "__main__":
    init_db()
    logger.info("Transfer Bot v3.0 starting…")
    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup,
    )
