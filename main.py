import asyncio
import aiohttp
import pandas as pd
import os
import time
import logging
import warnings
from datetime import datetime, timedelta, timezone
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TOKEN or not CHAT_ID:
    raise ValueError("TOKEN and CHAT_ID must be set in .env file")

# Временная зона МСК
MSK_TZ = timezone(timedelta(hours=3))

# Тикеры
TICKERS = ["SBER", "GAZP", "LKOH", "GMKN", "VTBR", "ROSN", "TATN", "NVTK", "PLZL", "SNGS"]

# Настройки
INTERVAL = 1  # минут
CANDLES = 100  # свечей для анализа

# URL как в первом боте
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities"

# Статистика
start_time = time.time()
last_signals = {}
total_cycles = 0
signals_found = 0
ticks_with_data = 0
ticks_without_data = 0
last_status_time = time.time()
steps_failed = {"step1": 0, "step2": 0, "step3": 0, "step4": 0, "step5": 0, "step6": 0}

# Глобальная переменная для хранения application
application = None

# ================= ФУНКЦИИ ВРЕМЕНИ =================

def get_msk_time():
    """Текущее время МСК"""
    return datetime.now(MSK_TZ)

# ================= MOEX API =================

async def fetch_json(session, url):
    """Простая функция как в первом боте"""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
    except:
        pass
    return None


async def get_candles(session, ticker):
    msk_now = get_msk_time()
    
    # Запрашиваем только последние ~2 часа вместо всего дня
    from_dt = msk_now - timedelta(hours=2)
    from_str = from_dt.strftime('%Y-%m-%d %H:%M:%S')
    till_str = msk_now.strftime('%Y-%m-%d %H:%M:%S')
    
    url = (f"{BASE_URL}/{ticker}/candles.json"
           f"?from={from_str}&till={till_str}&interval={INTERVAL}&iss.meta=off&iss.only=candles")
    
    data = await fetch_json(session, url)
    
    if not data or 'candles' not in data:
        logger.warning(f"❌ {ticker}: Нет данных")
        return None
    
    rows = data['candles']['data']
    cols = data['candles']['columns']
    
    if not rows:
        logger.warning(f"❌ {ticker}: Пустой массив")
        return None
    
    df = pd.DataFrame(rows, columns=cols)
    
    if 'end' in df.columns:
        df = df.rename(columns={'end': 'date'})
    elif 'begin' in df.columns:
        df = df.rename(columns={'begin': 'date'})
    else:
        logger.warning(f"❌ {ticker}: Нет колонки с датой")
        return None
    
    need = ['date', 'open', 'high', 'low', 'close', 'volume']
    available = [c for c in need if c in df.columns]
    
    if len(available) < 5:
        logger.warning(f"❌ {ticker}: Не хватает колонок")
        return None
    
    df = df[available].copy()
    
    # API возвращает время уже в МСК — НЕ прибавляем +3
    df['date'] = pd.to_datetime(df['date'])
    
    for c in available:
        if c != 'date':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = df.dropna().sort_values('date').reset_index(drop=True)
    
    if len(df) < 5:
        logger.warning(f"❌ {ticker}: Мало свечей ({len(df)})")
        return None
    
    last_price = df['close'].iloc[-1]
    last_volume = df['volume'].iloc[-1]
    first_time = df['date'].iloc[0]
    last_time = df['date'].iloc[-1]
    
    msk_now_naive = msk_now.replace(tzinfo=None)
    time_diff = (msk_now_naive - last_time).total_seconds() / 60
    
    # Отсеиваем если последняя свеча старше 5 минут
    if time_diff > 16:
        logger.warning(f"⚠️ {ticker}: Данные устарели ({time_diff:.0f} мин), пропускаем")
        return None
    
    if time_diff < 0:
        logger.warning(f"⚠️ {ticker}: Время в будущем, пропускаем")
        return None
    
    logger.info(f"✅ {ticker}: {len(df)} св. | {first_time.strftime('%H:%M')}→{last_time.strftime('%H:%M')} МСК | "
                f"Цена: {last_price:.2f} | Объем: {last_volume:.0f} | Отставание: {time_diff:.0f}мин")
    return df

async def get_orderbook(session, ticker):
    """Получение стакана"""
    url = f"{BASE_URL}/{ticker}/orderbook.json"
    
    try:
        async with session.get(url) as response:
            if response.status != 200:
                return None, None
            
            data = await response.json()
            if not data or 'orderbook' not in data:
                return None, None
            
            orderbook_data = data['orderbook']['data']
            if not orderbook_data or not orderbook_data[0]:
                return None, None
            
            row = orderbook_data[0]
            bids = row[2] if len(row) > 2 and row[2] else []
            asks = row[3] if len(row) > 3 and row[3] else []
            
            bid_vol = sum(b[1] for b in bids if len(b) > 1) if bids else 0
            ask_vol = sum(a[1] for a in asks if len(a) > 1) if asks else 0
            
            if bid_vol > 0 or ask_vol > 0:
                return bid_vol, ask_vol
    except:
        pass
    
    return None, None


# ================= TECHNICAL ANALYSIS =================

def calculate_liquidity_levels(df):
    """Уровни ликвидности"""
    try:
        high_lvl = df["high"].rolling(window=20).max().iloc[-2]
        low_lvl = df["low"].rolling(window=20).min().iloc[-2]
        return high_lvl, low_lvl
    except:
        return None, None


def detect_liquidity_grab(df):
    """Захват ликвидности"""
    try:
        prev = df.iloc[-2]
        last = df.iloc[-1]
        
        high_lvl, low_lvl = calculate_liquidity_levels(df)
        if high_lvl is None:
            return None
        
        if prev["high"] > high_lvl and last["close"] < prev["high"]:
            return "SHORT"
        
        if prev["low"] < low_lvl and last["close"] > prev["low"]:
            return "LONG"
        
        return None
    except:
        return None


def check_displacement(df, multiplier=1.2):
    """Импульсное движение"""
    try:
        last = df.iloc[-1]
        body = abs(last["close"] - last["open"])
        avg_body = (df["close"] - df["open"]).abs().rolling(window=20).mean().iloc[-1]
        
        if pd.isna(avg_body) or avg_body == 0:
            return False
        
        return body > avg_body * multiplier
    except:
        return False


def check_volume_spike(df, multiplier=1.2):
    """Всплеск объема"""
    try:
        last_vol = df.iloc[-1]["volume"]
        avg_vol = df["volume"].rolling(window=20).mean().iloc[-1]
        
        if pd.isna(avg_vol) or avg_vol == 0:
            return False
        
        return last_vol > avg_vol * multiplier
    except:
        return False


def find_fair_value_gap(df):
    """Fair Value Gap"""
    try:
        if len(df) < 3:
            return None
        
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
        
        if c1["high"] < c3["low"]:
            return (float(c1["high"]), float(c3["low"]))
        
        if c1["low"] > c3["high"]:
            return (float(c3["high"]), float(c1["low"]))
        
        return None
    except:
        return None


def analyze_orderbook_bias(bid_vol, ask_vol):
    """Анализ стакана"""
    if bid_vol is None or ask_vol is None or (bid_vol == 0 and ask_vol == 0):
        return "NO_DATA"
    
    if ask_vol == 0:
        return "STRONG_BUY" if bid_vol > 0 else "NO_DATA"
    if bid_vol == 0:
        return "STRONG_SELL" if ask_vol > 0 else "NO_DATA"
    
    ratio = bid_vol / ask_vol
    
    if ratio > 1.3:
        return "STRONG_BUY"
    elif ratio > 1.1:
        return "BUY"
    elif ratio < 0.7:
        return "STRONG_SELL"
    elif ratio < 0.9:
        return "SELL"
    
    return "NEUTRAL"


# ================= SIGNAL GENERATION =================

def generate_trading_signal(df, bid_vol, ask_vol, ticker=""):
    """Генерация сигнала"""
    global steps_failed
    
    if df is None or len(df) < 20:
        return None
    
    # Шаг 1: Захват ликвидности
    side = detect_liquidity_grab(df)
    if not side:
        steps_failed["step1"] += 1
        return None
    
    # Шаг 2: Импульс
    if not check_displacement(df):
        steps_failed["step2"] += 1
        return None
    
    # Шаг 3: Объем
    if not check_volume_spike(df):
        steps_failed["step3"] += 1
        return None
    
    # Шаг 4: FVG
    fvg = find_fair_value_gap(df)
    if not fvg:
        steps_failed["step4"] += 1
        return None
    
    # Шаг 5: Стакан
    ob_bias = analyze_orderbook_bias(bid_vol, ask_vol)
    
    if ob_bias != "NO_DATA":
        if side == "LONG" and ob_bias in ["SELL", "STRONG_SELL"]:
            steps_failed["step5"] += 1
            return None
        if side == "SHORT" and ob_bias in ["BUY", "STRONG_BUY"]:
            steps_failed["step5"] += 1
            return None
    
    # Шаг 6: Близость к FVG
    current_price = df.iloc[-1]["close"]
    fvg_low, fvg_high = min(fvg), max(fvg)
    
    dist_low = abs(current_price - fvg_low) / fvg_low if fvg_low > 0 else 999
    dist_high = abs(current_price - fvg_high) / fvg_high if fvg_high > 0 else 999
    
    if dist_low >= 0.02 and dist_high >= 0.02:
        steps_failed["step6"] += 1
        return None
    
    last_time = df['date'].iloc[-1]
    logger.info(f"🎯 {ticker}: СИГНАЛ {side}! Цена={current_price:.2f} | Свеча: {last_time.strftime('%H:%M:%S')} МСК")
    
    return {
        "side": side,
        "price": float(current_price),
        "fvg": fvg,
        "bias": ob_bias,
        "bid_vol": bid_vol or 0,
        "ask_vol": ask_vol or 0,
        "volume_ratio": (bid_vol / ask_vol) if (ask_vol and ask_vol > 0) else 0,
        "timestamp": last_time
    }


# ================= TELEGRAM =================

async def send_telegram_signal(ticker, signal, bot):
    """Отправка сигнала"""
    global signals_found
    
    signal_key = f"{ticker}_{signal['side']}_{signal['timestamp'].strftime('%Y%m%d_%H%M')}"
    if signal_key in last_signals:
        return
    
    try:
        fvg_low, fvg_high = min(signal['fvg']), max(signal['fvg'])
        fvg_mid = (fvg_low + fvg_high) / 2
        emoji = "🟢" if signal['side'] == "LONG" else "🔴"
        msk_time = signal['timestamp'].strftime('%H:%M:%S')
        
        message = (
            f"{emoji} <b>{ticker}</b> - <b>{signal['side']}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💵 Цена: <b>{signal['price']:.2f}</b>\n"
            f"📊 FVG: <b>{fvg_low:.2f}</b> - <b>{fvg_high:.2f}</b>\n"
            f"🎯 Mid: <b>{fvg_mid:.2f}</b>\n"
            f"📈 Стакан: <b>{signal['bias']}</b>\n"
            f"⏰ {msk_time} МСК"
        )
        
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
        
        last_signals[signal_key] = time.time()
        signals_found += 1
        
        now = time.time()
        for k in list(last_signals.keys()):
            if now - last_signals[k] > 3600:
                del last_signals[k]
        
        logger.info(f"📤 Сигнал отправлен: {ticker} {signal['side']}")
        
    except Exception as e:
        logger.error(f"Ошибка Telegram: {e}")


async def send_hourly_status(bot):
    """Ежечасный отчет"""
    global last_status_time
    
    current_time = time.time()
    if current_time - last_status_time < 3600:
        return
    
    last_status_time = current_time
    
    try:
        uptime = time.time() - start_time
        h, m = int(uptime // 3600), int((uptime % 3600) // 60)
        total_fails = sum(steps_failed.values())
        msk_now = get_msk_time()
        
        message = (
            f"📊 <b>ЕЖЕЧАСНЫЙ ОТЧЕТ</b>\n"
            f"🕐 {msk_now.strftime('%H:%M')} МСК\n\n"
            f"⏱ Аптайм: {h}ч {m}м\n"
            f"🔄 Циклов: {total_cycles}\n"
            f"📤 Сигналов: {signals_found}\n\n"
            f"📈 Тикеров: {len(TICKERS)}\n"
            f"✅ С данными: {ticks_with_data}\n"
            f"❌ Без данных: {ticks_without_data}\n\n"
            f"<b>Отказы по шагам:</b>\n"
            f"1️⃣ Захват ликвидности: {steps_failed['step1']}\n"
            f"2️⃣ Импульс (1.2x): {steps_failed['step2']}\n"
            f"3️⃣ Объем (1.2x): {steps_failed['step3']}\n"
            f"4️⃣ FVG: {steps_failed['step4']}\n"
            f"5️⃣ Стакан: {steps_failed['step5']}\n"
            f"6️⃣ Расстояние до FVG: {steps_failed['step6']}\n"
            f"📊 Всего отказов: {total_fails}"
        )
        
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
        logger.info("📤 Ежечасный отчет отправлен")
        
    except Exception as e:
        logger.error(f"Ошибка отчета: {e}")


# ================= COMMANDS =================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msk_now = get_msk_time()
    await update.message.reply_text(
        f"🚀 <b>SMC Trading Bot v13.0</b>\n\n"
        f"🕐 {msk_now.strftime('%H:%M:%S')} МСК\n"
        f"📊 Тикеров: {len(TICKERS)}\n"
        f"⏱ Интервал: {INTERVAL} мин\n"
        f"🎯 Множители: 1.2x\n"
        f"📅 Фильтр: строго сегодня\n\n"
        f"📋 <b>Команды</b>\n"
        f"▪️ /status - статистика\n"
        f"▪️ /test - анализ отказов\n"
        f"▪️ /tickers - список тикеров\n"
        f"▪️ /signals - сигналы за час\n"
        f"▪️ /help - справка",
        parse_mode='HTML'
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = time.time() - start_time
    h, m = int(uptime // 3600), int((uptime % 3600) // 60)
    msk_now = get_msk_time()
    
    await update.message.reply_text(
        f"📊 <b>Статус</b>\n"
        f"🕐 {msk_now.strftime('%H:%M:%S')} МСК\n\n"
        f"⏱ Аптайм: {h}ч {m}м\n"
        f"🔄 Циклов: {total_cycles}\n"
        f"📤 Сигналов: {signals_found}\n"
        f"✅ С данными: {ticks_with_data}\n"
        f"❌ Без данных: {ticks_without_data}",
        parse_mode='HTML'
    )


async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_fails = sum(steps_failed.values())
    msk_now = get_msk_time()
    
    message = (
        f"📊 <b>АНАЛИЗ ОТКАЗОВ</b>\n"
        f"🕐 {msk_now.strftime('%H:%M:%S')} МСК\n\n"
        f"🔄 Циклов: {total_cycles}\n"
        f"✅ С данными: {ticks_with_data}\n"
        f"❌ Без данных: {ticks_without_data}\n\n"
        f"<b>Где отсеиваются сигналы</b>\n"
        f"1️⃣ Захват ликвидности: {steps_failed['step1']}\n"
        f"2️⃣ Импульс (1.2x): {steps_failed['step2']}\n"
        f"3️⃣ Объем (1.2x): {steps_failed['step3']}\n"
        f"4️⃣ FVG: {steps_failed['step4']}\n"
        f"5️⃣ Стакан: {steps_failed['step5']}\n"
        f"6️⃣ Расстояние до FVG: {steps_failed['step6']}\n"
        f"📊 Всего отказов: {total_fails}"
    )
    
    await update.message.reply_text(message, parse_mode='HTML')


async def tickers_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tickers_list = "\n".join(f"• {t}" for t in TICKERS)
    await update.message.reply_text(
        f"📊 <b>Тикеры ({len(TICKERS)})</b>\n\n{tickers_list}",
        parse_mode='HTML'
    )


async def signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    recent = {k: v for k, v in last_signals.items() if time.time() - v < 3600}
    if not recent:
        await update.message.reply_text("📊 Нет сигналов за последний час")
        return
    
    # Без HTML для динамических данных, чтобы избежать ошибок парсинга
    text = f"📊 Сигналы за час ({len(recent)}):\n\n"
    for key, ts in sorted(recent.items(), key=lambda x: x[1], reverse=True)[:10]:
        parts = key.split('_')
        if len(parts) >= 2:
            emoji = "🟢" if parts[1] == "LONG" else "🔴"
            time_str = datetime.fromtimestamp(ts).strftime('%H:%M')
            text += f"{emoji} {parts[0]} - {parts[1]} ({time_str})\n"
    
    await update.message.reply_text(text)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📚 <b>SMC Стратегия v13.0</b>\n\n"
        "🔍 <b>Алгоритм проверки</b>\n"
        "1️⃣ Захват ликвидности (20 свечей)\n"
        "2️⃣ Импульс (1.2x среднего)\n"
        "3️⃣ Объем (1.2x среднего)\n"
        "4️⃣ Fair Value Gap (3 свечи)\n"
        "5️⃣ Анализ стакана\n"
        "6️⃣ Близость к FVG (меньше 2%%)\n\n"
        "🕐 Время: МСК (UTC+3)\n"
        "📅 Данные: строго сегодня\n"
        "⚠️ Будущее время отсеивается\n\n"
        "📋 <b>Команды</b>\n"
        "▪️ /start - запуск бота\n"
        "▪️ /status - текущая статистика\n"
        "▪️ /test - анализ отказов\n"
        "▪️ /tickers - список тикеров\n"
        "▪️ /signals - сигналы за час\n"
        "▪️ /help - справка",
        parse_mode='HTML'
    )


# ================= MAIN LOOP =================

async def process_ticker(session, ticker, bot):
    """Обработка одного тикера"""
    global ticks_with_data, ticks_without_data
    
    try:
        df = await get_candles(session, ticker)
        if df is None:
            ticks_without_data += 1
            return
        
        ticks_with_data += 1
        
        bid_vol, ask_vol = await get_orderbook(session, ticker)
        
        signal = generate_trading_signal(df, bid_vol, ask_vol, ticker)
        
        if signal:
            await send_telegram_signal(ticker, signal, bot)
            
    except Exception as e:
        logger.error(f"❌ {ticker}: {e}")


async def main_loop():
    """Основной цикл с polling для обработки команд"""
    global total_cycles, application
    
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Добавляем все обработчики команд
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("test", test_cmd))
    application.add_handler(CommandHandler("tickers", tickers_cmd))
    application.add_handler(CommandHandler("signals", signals_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    
    # Запускаем polling для приема команд
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    logger.info("✅ Telegram polling запущен, команды будут обрабатываться")
    
    bot = application.bot
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        msk_now = get_msk_time()
        
        logger.info("=" * 60)
        logger.info(f"🚀 SMC Trading Bot v13.0 ЗАПУЩЕН | {msk_now.strftime('%H:%M:%S')} МСК")
        logger.info(f"📊 Тикеров: {len(TICKERS)}")
        logger.info(f"⏱ Интервал: {INTERVAL} мин")
        logger.info(f"🎯 Множители: 1.2x")
        logger.info(f"📅 Фильтр: только сегодня ({msk_now.strftime('%Y-%m-%d')})")
        logger.info(f"⚠️ Будущее время отсеивается")
        logger.info("=" * 60)
        
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=f"✅ <b>Бот v13.0 запущен!</b>\n\n"
                     f"🕐 {msk_now.strftime('%H:%M:%S')} МСК\n"
                     f"📊 {len(TICKERS)} тикеров\n"
                     f"📅 Строго сегодняшние данные\n\n"
                     f"📋 <b>Команды бота</b>\n"
                     f"▪️ /status - статистика\n"
                     f"▪️ /test - анализ отказов\n"
                     f"▪️ /tickers - список тикеров\n"
                     f"▪️ /signals - сигналы за час\n"
                     f"▪️ /help - справка",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Не удалось отправить стартовое сообщение: {e}")
        
        # Основной цикл сканирования
        while True:
            try:
                total_cycles += 1
                
                # Вычисляем время до следующей минуты
                now = time.time()
                seconds_to_next = (INTERVAL * 60) - (now % (INTERVAL * 60))
                wait_time = max(seconds_to_next + 1, 1)
                
                logger.info(f"⏳ Цикл #{total_cycles}: ожидание {wait_time:.0f}с")
                await asyncio.sleep(wait_time)
                
                msk_now = get_msk_time()
                logger.info(f"🔄 Цикл #{total_cycles}: сканирование... ({msk_now.strftime('%H:%M:%S')} МСК)")
                
                # Запускаем обработку всех тикеров параллельно
                tasks = [process_ticker(session, ticker, bot) for ticker in TICKERS]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                logger.info(f"✅ Цикл #{total_cycles}: данных: {ticks_with_data}, без данных: {ticks_without_data}, сигналов: {signals_found}")
                
                # Отправляем ежечасный отчет
                await send_hourly_status(bot)
                
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Ошибка цикла: {e}")
                await asyncio.sleep(60)
    
    # Останавливаем polling при завершении
    await application.updater.stop()
    await application.stop()


if __name__ == "__main__":
    try:
        start_time = time.time()
        logger.info("Запуск SMC Trading Bot v13.0...")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Бот завершил работу")