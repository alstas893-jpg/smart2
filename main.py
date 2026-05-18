import asyncio
import aiohttp
import pandas as pd
import os
import time
import logging
import sys
from datetime import datetime, timedelta
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from dotenv import load_dotenv

# Отключаем предупреждение pandas
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.DEBUG,  # Временно DEBUG для диагностики
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
    logger.error("❌ TOKEN или CHAT_ID не найдены в .env файле!")
    sys.exit(1)

# Список тикеров для мониторинга
TICKERS = ["SBER", "GAZP", "LKOH", "GMKN", "VTBR", "ROSN", "TATN", "NVTK", "PLZL", "SNGS"]

# Настройки свечей
INTERVAL = 1  # минут
CANDLES = 100  # количество свечей для анализа

# Правильные URL
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities"

# Глобальные переменные
start_time = time.time()
last_signals = {}
total_cycles = 0
signals_found = 0
ticks_with_data = 0
ticks_without_data = 0
api_errors = 0

# ================= MOEX API FUNCTIONS =================

async def fetch_json(session, url, retries=3):
    """Безопасное получение JSON с расширенной диагностикой"""
    global api_errors
    
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
    }
    
    for attempt in range(retries):
        try:
            logger.debug(f"📡 HTTP запрос (попытка {attempt+1}/{retries}): {url[:100]}...")
            
            async with session.get(url, headers=headers) as response:
                ct = response.headers.get('content-type', '')
                status = response.status
                
                logger.debug(f"📡 Ответ: статус={status}, content-type={ct[:50]}")
                
                # Пропускаем HTML ответы
                if 'html' in ct.lower():
                    logger.debug(f"⚠️ Получен HTML вместо JSON (статус={status})")
                    api_errors += 1
                    return None
                
                if status != 200:
                    logger.debug(f"⚠️ HTTP {status} для {url[:100]}")
                    if attempt < retries - 1:
                        await asyncio.sleep(1)
                    continue
                
                if 'json' in ct.lower():
                    try:
                        data = await response.json()
                        logger.debug(f"✅ JSON получен успешно")
                        return data
                    except Exception as e:
                        logger.debug(f"❌ Ошибка парсинга JSON: {e}")
                        api_errors += 1
                        return None
                else:
                    logger.debug(f"⚠️ Неожиданный content-type: {ct}")
                    api_errors += 1
                    return None
                    
        except asyncio.TimeoutError:
            logger.debug(f"⏱ Таймаут (попытка {attempt+1})")
            if attempt < retries - 1:
                await asyncio.sleep(1)
        except aiohttp.ClientError as e:
            logger.debug(f"🌐 Ошибка соединения: {e}")
            api_errors += 1
            if attempt < retries - 1:
                await asyncio.sleep(1)
        except Exception as e:
            logger.debug(f"❌ Неожиданная ошибка: {e}")
            api_errors += 1
            if attempt < retries - 1:
                await asyncio.sleep(1)
    
    return None


async def get_candles(session, ticker):
    """Получение свечных данных с полной диагностикой"""
    till = datetime.now().strftime('%Y-%m-%d')
    frm = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    url = (f"{BASE_URL}/{ticker}/candles.json"
           f"?from={frm}&till={till}&interval={INTERVAL}&iss.meta=off&iss.only=candles")
    
    logger.debug(f"📊 Запрос свечей для {ticker}")
    
    data = await fetch_json(session, url)
    if not data:
        logger.warning(f"❌ {ticker}: API не вернул данные")
        return None
    
    if 'candles' not in data:
        logger.warning(f"❌ {ticker}: В ответе нет ключа 'candles'. Ключи: {list(data.keys())}")
        return None
    
    try:
        rows = data['candles']['data']
        cols = data['candles']['columns']
        
        logger.debug(f"📊 {ticker}: Получено {len(rows)} строк, колонки: {cols}")
        
        if not rows:
            logger.warning(f"❌ {ticker}: Пустой массив свечей (0 записей)")
            return None
        
        # Показываем первую и последнюю строку для диагностики
        logger.debug(f"📊 {ticker}: Первая строка: {rows[0]}")
        logger.debug(f"📊 {ticker}: Последняя строка: {rows[-1]}")
        
        # Создаем DataFrame
        df = pd.DataFrame(rows, columns=cols)
        logger.debug(f"📊 {ticker}: DataFrame создан, shape={df.shape}")
        
        # Переименовываем колонки
        if 'begin' in df.columns:
            df = df.rename(columns={'begin': 'date'})
        else:
            logger.error(f"❌ {ticker}: Колонка 'begin' не найдена!")
            return None
        
        # Проверяем наличие нужных колонок
        need = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [c for c in need if c not in df.columns]
        if missing_cols:
            logger.error(f"❌ {ticker}: Отсутствуют колонки: {missing_cols}")
            return None
        
        df = df[need].copy()
        
        # Конвертируем типы
        df['date'] = pd.to_datetime(df['date'])
        for c in need[1:]:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        
        # Удаляем NaN и сортируем
        before_drop = len(df)
        df = df.dropna()
        after_drop = len(df)
        logger.debug(f"📊 {ticker}: Удалено NaN: {before_drop - after_drop} строк")
        
        df = df.sort_values('date')
        df = df.tail(CANDLES)
        
        if len(df) < 50:
            logger.warning(f"❌ {ticker}: Мало свечей ({len(df)} < 50)")
            return None
        
        last_price = df['close'].iloc[-1]
        last_volume = df['volume'].iloc[-1]
        first_date = df['date'].iloc[0]
        last_date = df['date'].iloc[-1]
        
        logger.info(f"✅ {ticker}: {len(df)} свечей | {first_date} → {last_date} | Цена: {last_price:.2f} | Объем: {last_volume:.0f}")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker}: Ошибка обработки данных - {type(e).__name__}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


async def get_orderbook(session, ticker):
    """Получение стакана - тихо, без ошибок"""
    
    urls = [
        f"{BASE_URL}/{ticker}/orderbook.json",
        f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/orderbook.json"
    ]
    
    for url in urls:
        try:
            async with session.get(url) as response:
                ct = response.headers.get('content-type', '')
                if 'json' not in ct.lower():
                    continue
                
                if response.status != 200:
                    continue
                
                data = await response.json()
                if not data or 'orderbook' not in data:
                    continue
                
                orderbook_data = data['orderbook']['data']
                if not orderbook_data or not orderbook_data[0]:
                    continue
                
                row = orderbook_data[0]
                bids = row[2] if len(row) > 2 and row[2] else []
                asks = row[3] if len(row) > 3 and row[3] else []
                
                bid_vol = sum(b[1] for b in bids if len(b) > 1) if bids else 0
                ask_vol = sum(a[1] for a in asks if len(a) > 1) if asks else 0
                
                if bid_vol > 0 or ask_vol > 0:
                    logger.debug(f"📖 {ticker}: Стакан BID={bid_vol:.0f} ASK={ask_vol:.0f}")
                    return bid_vol, ask_vol
                    
        except:
            continue
    
    return None, None


# ================= TECHNICAL ANALYSIS =================

def calculate_liquidity_levels(df):
    """Расчет уровней ликвидности"""
    try:
        high_lvl = df["high"].rolling(window=20).max().iloc[-2]
        low_lvl = df["low"].rolling(window=20).min().iloc[-2]
        return high_lvl, low_lvl
    except:
        return None, None


def detect_liquidity_grab(df):
    """Обнаружение захвата ликвидности"""
    try:
        prev = df.iloc[-2]
        last = df.iloc[-1]
        
        high_lvl, low_lvl = calculate_liquidity_levels(df)
        
        if high_lvl is None or low_lvl is None:
            return None
        
        # Медвежий захват
        if prev["high"] > high_lvl and last["close"] < prev["high"]:
            logger.debug(f"🔴 Обнаружен SHORT захват ликвидности (high={high_lvl:.2f}, prev_high={prev['high']:.2f}, close={last['close']:.2f})")
            return "SHORT"
        
        # Бычий захват
        if prev["low"] < low_lvl and last["close"] > prev["low"]:
            logger.debug(f"🟢 Обнаружен LONG захват ликвидности (low={low_lvl:.2f}, prev_low={prev['low']:.2f}, close={last['close']:.2f})")
            return "LONG"
        
        return None
    except Exception as e:
        logger.debug(f"Ошибка в detect_liquidity_grab: {e}")
        return None


def check_displacement(df, multiplier=1.5):
    """Проверка расширенного движения"""
    try:
        last = df.iloc[-1]
        body = abs(last["close"] - last["open"])
        
        avg_body = (df["close"] - df["open"]).abs().rolling(window=20).mean().iloc[-1]
        
        if pd.isna(avg_body) or avg_body == 0:
            return False
        
        result = body > avg_body * multiplier
        logger.debug(f"Displacement: body={body:.2f}, avg={avg_body:.2f}, ratio={body/avg_body:.2f}, result={result}")
        return result
    except:
        return False


def check_volume_spike(df, multiplier=1.5):
    """Проверка всплеска объема"""
    try:
        last_vol = df.iloc[-1]["volume"]
        avg_vol = df["volume"].rolling(window=20).mean().iloc[-1]
        
        if pd.isna(avg_vol) or avg_vol == 0:
            return False
        
        result = last_vol > avg_vol * multiplier
        logger.debug(f"Volume: last={last_vol:.0f}, avg={avg_vol:.0f}, ratio={last_vol/avg_vol:.2f}, result={result}")
        return result
    except:
        return False


def find_fair_value_gap(df):
    """Поиск Fair Value Gap"""
    try:
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
        
        logger.debug(f"FVG анализ: c1(h={c1['high']:.2f},l={c1['low']:.2f}) c2(h={c2['high']:.2f},l={c2['low']:.2f}) c3(h={c3['high']:.2f},l={c3['low']:.2f})")
        
        # Бычий FVG
        if c1["high"] < c3["low"]:
            fvg = (float(c1["high"]), float(c3["low"]))
            logger.debug(f"🟢 Найден бычий FVG: {fvg}")
            return fvg
        
        # Медвежий FVG
        if c1["low"] > c3["high"]:
            fvg = (float(c3["high"]), float(c1["low"]))
            logger.debug(f"🔴 Найден медвежий FVG: {fvg}")
            return fvg
        
        return None
    except Exception as e:
        logger.debug(f"Ошибка в find_fair_value_gap: {e}")
        return None


def analyze_orderbook_bias(bid_vol, ask_vol):
    """Анализ смещения в стакане"""
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
    """Генерация торгового сигнала с детальным логированием"""
    if df is None or len(df) < 50:
        logger.debug(f"{ticker}: Недостаточно данных ({len(df) if df is not None else 0} свечей)")
        return None
    
    logger.debug(f"\n{'='*50}")
    logger.debug(f"🔍 {ticker}: НАЧАЛО АНАЛИЗА СИГНАЛА")
    logger.debug(f"{'='*50}")
    
    # Шаг 1: Захват ликвидности
    side = detect_liquidity_grab(df)
    if not side:
        logger.debug(f"❌ {ticker}: Шаг 1 - нет захвата ликвидности")
        return None
    
    logger.info(f"✅ {ticker}: Шаг 1 - захват ликвидности {side}")
    
    # Шаг 2: Расширенное движение
    if not check_displacement(df):
        logger.debug(f"❌ {ticker}: Шаг 2 - нет displacement")
        return None
    
    logger.info(f"✅ {ticker}: Шаг 2 - есть displacement")
    
    # Шаг 3: Всплеск объема
    if not check_volume_spike(df):
        logger.debug(f"❌ {ticker}: Шаг 3 - нет всплеска объема")
        return None
    
    logger.info(f"✅ {ticker}: Шаг 3 - всплеск объема")
    
    # Шаг 4: Fair Value Gap
    fvg = find_fair_value_gap(df)
    if not fvg:
        logger.debug(f"❌ {ticker}: Шаг 4 - нет FVG")
        return None
    
    logger.info(f"✅ {ticker}: Шаг 4 - найден FVG {fvg}")
    
    # Шаг 5: Анализ стакана
    ob_bias = analyze_orderbook_bias(bid_vol, ask_vol)
    logger.debug(f"📖 {ticker}: Стакан: {ob_bias}")
    
    if ob_bias != "NO_DATA":
        if side == "LONG" and ob_bias in ["SELL", "STRONG_SELL"]:
            logger.info(f"❌ {ticker}: LONG отвергнут стаканом ({ob_bias})")
            return None
        if side == "SHORT" and ob_bias in ["BUY", "STRONG_BUY"]:
            logger.info(f"❌ {ticker}: SHORT отвергнут стаканом ({ob_bias})")
            return None
    
    logger.info(f"✅ {ticker}: Шаг 5 - стакан OK")
    
    # Фильтр близости к FVG
    current_price = df.iloc[-1]["close"]
    fvg_low, fvg_high = min(fvg), max(fvg)
    
    dist_to_low = abs(current_price - fvg_low) / fvg_low if fvg_low > 0 else 999
    dist_to_high = abs(current_price - fvg_high) / fvg_high if fvg_high > 0 else 999
    
    logger.debug(f"📍 {ticker}: Цена={current_price:.2f}, FVG=({fvg_low:.2f}, {fvg_high:.2f}), расстояние={min(dist_to_low, dist_to_high)*100:.2f}%")
    
    if dist_to_low >= 0.02 and dist_to_high >= 0.02:
        logger.info(f"❌ {ticker}: Цена далеко от FVG ({min(dist_to_low, dist_to_high)*100:.2f}%)")
        return None
    
    logger.info(f"✅ {ticker}: Шаг 6 - цена близко к FVG")
    
    # ВСЕ ШАГИ ПРОЙДЕНЫ!
    logger.info(f"\n{'='*50}")
    logger.info(f"🎯🎯🎯 {ticker}: СИГНАЛ НАЙДЕН! {side} 🎯🎯🎯")
    logger.info(f"{'='*50}\n")
    
    return {
        "side": side,
        "price": float(current_price),
        "fvg": fvg,
        "bias": ob_bias if ob_bias != "NO_DATA" else "N/A",
        "bid_vol": bid_vol if bid_vol else 0,
        "ask_vol": ask_vol if ask_vol else 0,
        "volume_ratio": (bid_vol / ask_vol) if (ask_vol and ask_vol > 0) else 0,
        "timestamp": pd.Timestamp.now()
    }


# ================= TELEGRAM NOTIFICATIONS =================

async def send_telegram_signal(ticker, signal, bot):
    """Отправка сигнала в Telegram"""
    global signals_found
    
    signal_key = f"{ticker}_{signal['side']}_{signal['timestamp'].strftime('%Y%m%d_%H%M')}"
    
    if signal_key in last_signals:
        return
    
    try:
        fvg_low, fvg_high = min(signal['fvg']), max(signal['fvg'])
        fvg_mid = (fvg_low + fvg_high) / 2
        
        emoji = "🟢" if signal['side'] == "LONG" else "🔴"
        
        bid_str = f"{signal['bid_vol']:.0f}"
        ask_str = f"{signal['ask_vol']:.0f}"
        
        if signal['bid_vol'] >= 1_000_000:
            bid_str = f"{signal['bid_vol']/1_000_000:.1f}M"
        elif signal['bid_vol'] >= 1_000:
            bid_str = f"{signal['bid_vol']/1_000:.0f}K"
            
        if signal['ask_vol'] >= 1_000_000:
            ask_str = f"{signal['ask_vol']/1_000_000:.1f}M"
        elif signal['ask_vol'] >= 1_000:
            ask_str = f"{signal['ask_vol']/1_000:.0f}K"
        
        message = (
            f"{emoji} <b>{ticker}</b> - <b>{signal['side']}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💵 Цена: <b>{signal['price']:.2f}</b>\n"
            f"📊 FVG: <b>{fvg_low:.2f}</b> - <b>{fvg_high:.2f}</b>\n"
            f"🎯 Mid FVG: <b>{fvg_mid:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Стакан: <b>{signal['bias']}</b>\n"
            f"🟢 BID: <b>{bid_str}</b> | 🔴 ASK: <b>{ask_str}</b>\n"
            f"📊 Ratio: <b>{signal['volume_ratio']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⏰ {signal['timestamp'].strftime('%H:%M:%S')}"
        )
        
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
        
        last_signals[signal_key] = time.time()
        signals_found += 1
        
        # Очистка старых сигналов
        current_time = time.time()
        old_keys = [k for k, v in last_signals.items() if current_time - v > 3600]
        for k in old_keys:
            del last_signals[k]
        
        logger.info(f"📤 СИГНАЛ ОТПРАВЛЕН В TELEGRAM: {ticker} {signal['side']}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")


# ================= COMMANDS =================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    text = (
        "🚀 <b>SMC Trading Bot v5.0</b>\n\n"
        "📊 <b>Мониторинг в реальном времени:</b>\n"
        f"• Тикеров: {len(TICKERS)}\n"
        f"• Интервал: {INTERVAL} мин\n"
        "• Стратегия: Smart Money Concepts\n\n"
        "🔍 <b>Анализ:</b>\n"
        "• Захват ликвидности\n"
        "• Импульсное движение\n"
        "• Всплеск объема\n"
        "• Fair Value Gap\n"
        "• Стакан заявок\n\n"
        "<b>📋 Команды:</b>\n"
        "/status - статистика\n"
        "/tickers - тикеры\n"
        "/signals - сигналы\n"
        "/help - справка"
    )
    await update.message.reply_text(text, parse_mode='HTML')


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /status"""
    uptime = time.time() - start_time
    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)
    
    text = (
        "📊 <b>Статус мониторинга:</b>\n\n"
        f"✅ Бот активен\n"
        f"⏱ Аптайм: {hours}ч {minutes}м\n"
        f"🔄 Циклов: {total_cycles}\n"
        f"📤 Сигналов: {signals_found}\n"
        f"📈 Тикеров: {len(TICKERS)}\n"
        f"✅ С данными: {ticks_with_data}\n"
        f"❌ Без данных: {ticks_without_data}\n"
        f"🌐 Ошибок API: {api_errors}\n"
        f"⏱ Интервал: {INTERVAL} мин\n\n"
        f"<i>Лог: trading_bot.log</i>"
    )
    await update.message.reply_text(text, parse_mode='HTML')


async def tickers_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /tickers"""
    tickers_list = "\n".join([f"• {t}" for t in TICKERS])
    text = f"📊 <b>Тикеры ({len(TICKERS)}):</b>\n\n{tickers_list}"
    await update.message.reply_text(text, parse_mode='HTML')


async def signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /signals"""
    recent = {k: v for k, v in last_signals.items() if time.time() - v < 3600}
    
    if not recent:
        await update.message.reply_text("📊 Нет сигналов за последний час")
        return
    
    text = f"📊 <b>Сигналы за час ({len(recent)}):</b>\n\n"
    for signal_key, timestamp in sorted(recent.items(), key=lambda x: x[1], reverse=True)[:10]:
        parts = signal_key.split('_')
        if len(parts) >= 2:
            ticker, side = parts[0], parts[1]
            emoji = "🟢" if side == "LONG" else "🔴"
            time_str = datetime.fromtimestamp(timestamp).strftime('%H:%M')
            text += f"{emoji} <b>{ticker}</b> - {side} ({time_str})\n"
    
    await update.message.reply_text(text, parse_mode='HTML')


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    text = (
        "📚 <b>SMC Trading Bot - Справка</b>\n\n"
        "<b>Стратегия Smart Money (5 шагов):</b>\n\n"
        "1️⃣ Захват ликвидности (20 периодов)\n"
        "2️⃣ Импульс (тело > 1.5x среднего)\n"
        "3️⃣ Объем (> 1.5x среднего)\n"
        "4️⃣ Fair Value Gap (3 свечи)\n"
        "5️⃣ Стакан (BID/ASK)\n\n"
        "<b>📋 Команды:</b>\n"
        "/start, /status, /tickers, /signals, /help"
    )
    await update.message.reply_text(text, parse_mode='HTML')


# ================= MAIN PROCESSING =================

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
        logger.error(f"❌ {ticker}: Критическая ошибка - {e}")


async def health_check(bot):
    """Проверка работоспособности"""
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text="✅ <b>Бот v5.0 запущен!</b>\n"
                 f"📊 Тикеров: {len(TICKERS)}\n"
                 f"⏱ Интервал: {INTERVAL} мин\n"
                 f"📝 Лог: trading_bot.log\n\n"
                 "<i>/status - статистика</i>",
            parse_mode='HTML'
        )
    except:
        pass


async def main_loop():
    """Основной цикл"""
    global total_cycles, start_time
    
    application = Application.builder().token(TOKEN).build()
    bot = application.bot
    
    # Команды
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("tickers", tickers_cmd))
    application.add_handler(CommandHandler("signals", signals_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    
    await application.initialize()
    await application.start()
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("=" * 60)
        logger.info("🚀 SMC Trading Bot v5.0 ЗАПУЩЕН")
        logger.info(f"📊 Тикеров: {len(TICKERS)}")
        logger.info(f"⏱ Интервал: {INTERVAL} мин")
        logger.info(f"📝 Уровень логирования: DEBUG")
        logger.info("=" * 60)
        
        await health_check(bot)
        
        while True:
            try:
                total_cycles += 1
                
                now = time.time()
                seconds_to_next = (INTERVAL * 60) - (now % (INTERVAL * 60))
                wait_time = max(seconds_to_next + 1, 1)
                
                logger.info(f"\n{'='*60}")
                logger.info(f"⏳ ЦИКЛ #{total_cycles}: ожидание {wait_time:.0f}с")
                logger.info(f"{'='*60}")
                
                await asyncio.sleep(wait_time)
                
                logger.info(f"🔄 ЦИКЛ #{total_cycles}: сканирование...")
                
                tasks = [process_ticker(session, ticker, bot) for ticker in TICKERS]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                logger.info(f"✅ ЦИКЛ #{total_cycles}: завершен | Данных: {ticks_with_data} | Без данных: {ticks_without_data} | Ошибок API: {api_errors}")
                
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Ошибка цикла: {e}")
                await asyncio.sleep(60)
    
    await application.stop()


if __name__ == "__main__":
    try:
        start_time = time.time()
        logger.info("Запуск SMC Trading Bot v5.0...")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Бот завершил работу")