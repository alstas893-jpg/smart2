import asyncio
import aiohttp
import pandas as pd
import os
import time
import logging
from datetime import datetime, timedelta
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from dotenv import load_dotenv

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

# ================= MOEX API FUNCTIONS =================

async def fetch_json(session, url, retries=3):
    """Безопасное получение JSON"""
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
    }
    
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                ct = response.headers.get('content-type', '')
                
                # Пропускаем HTML ответы
                if 'html' in ct.lower():
                    return None
                
                if response.status != 200:
                    if attempt < retries - 1:
                        await asyncio.sleep(1)
                    continue
                
                if 'json' in ct.lower():
                    try:
                        return await response.json()
                    except:
                        return None
                else:
                    return None
                    
        except asyncio.TimeoutError:
            if attempt < retries - 1:
                await asyncio.sleep(1)
        except:
            if attempt < retries - 1:
                await asyncio.sleep(1)
    
    return None


async def get_candles(session, ticker):
    """Получение свечных данных с диагностикой"""
    till = datetime.now().strftime('%Y-%m-%d')
    frm = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    url = (f"{BASE_URL}/{ticker}/candles.json"
           f"?from={frm}&till={till}&interval={INTERVAL}&iss.meta=off&iss.only=candles")
    
    data = await fetch_json(session, url)
    if not data or 'candles' not in data:
        logger.warning(f"❌ {ticker}: API не вернул данные")
        return None
    
    try:
        rows = data['candles']['data']
        cols = data['candles']['columns']
        
        if not rows:
            logger.warning(f"❌ {ticker}: Пустой массив свечей (0 записей)")
            return None
        
        df = pd.DataFrame(rows, columns=cols)
        df = df.rename(columns={'begin': 'date'})
        
        need = ['date', 'open', 'high', 'low', 'close', 'volume']
        available_cols = [c for c in need if c in df.columns]
        
        if len(available_cols) < 5:
            logger.warning(f"❌ {ticker}: Нет нужных колонок. Доступны: {list(df.columns)}")
            return None
        
        df = df[available_cols].copy()
        
        df['date'] = pd.to_datetime(df['date'])
        for c in available_cols:
            if c != 'date':
                df[c] = pd.to_numeric(df[c], errors='coerce')
        
        df = df.dropna().sort_values('date')
        df = df.tail(CANDLES)
        
        if len(df) < 50:
            logger.warning(f"❌ {ticker}: Мало свечей ({len(df)} < 50)")
            return None
        
        last_price = df['close'].iloc[-1]
        last_volume = df['volume'].iloc[-1]
        logger.info(f"✅ {ticker}: {len(df)} свечей | Цена: {last_price:.2f} | Объем: {last_volume:.0f}")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker}: Ошибка обработки данных - {e}")
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
    if len(df) < 21:
        return None, None
    
    high_lvl = df["high"].rolling(window=20).max().iloc[-2]
    low_lvl = df["low"].rolling(window=20).min().iloc[-2]
    
    return high_lvl, low_lvl


def detect_liquidity_grab(df):
    """Обнаружение захвата ликвидности"""
    if len(df) < 21:
        return None
    
    prev = df.iloc[-2]
    last = df.iloc[-1]
    
    high_lvl, low_lvl = calculate_liquidity_levels(df)
    
    if high_lvl is None or low_lvl is None:
        return None
    
    # Медвежий захват
    if prev["high"] > high_lvl and last["close"] < prev["high"]:
        logger.debug(f"🔴 Обнаружен SHORT захват ликвидности")
        return "SHORT"
    
    # Бычий захват
    if prev["low"] < low_lvl and last["close"] > prev["low"]:
        logger.debug(f"🟢 Обнаружен LONG захват ликвидности")
        return "LONG"
    
    return None


def check_displacement(df, multiplier=1.5):
    """Проверка расширенного движения"""
    if len(df) < 21:
        return False
    
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    
    avg_body = (df["close"] - df["open"]).abs().rolling(window=20).mean().iloc[-1]
    
    if pd.isna(avg_body) or avg_body == 0:
        return False
    
    return body > avg_body * multiplier


def check_volume_spike(df, multiplier=1.5):
    """Проверка всплеска объема"""
    if len(df) < 21:
        return False
    
    last_vol = df.iloc[-1]["volume"]
    avg_vol = df["volume"].rolling(window=20).mean().iloc[-1]
    
    if pd.isna(avg_vol) or avg_vol == 0:
        return False
    
    return last_vol > avg_vol * multiplier


def find_fair_value_gap(df):
    """Поиск Fair Value Gap"""
    if len(df) < 3:
        return None
    
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    
    # Бычий FVG
    if c1["high"] < c3["low"]:
        logger.debug(f"🟢 Найден бычий FVG")
        return (float(c1["high"]), float(c3["low"]))
    
    # Медвежий FVG
    if c1["low"] > c3["high"]:
        logger.debug(f"🔴 Найден медвежий FVG")
        return (float(c3["high"]), float(c1["low"]))
    
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
        return None
    
    # Шаг 1: Захват ликвидности
    side = detect_liquidity_grab(df)
    if not side:
        return None
    
    logger.info(f"🔍 {ticker}: Шаг 1 пройден - захват ликвидности {side}")
    
    # Шаг 2: Расширенное движение
    if not check_displacement(df):
        logger.debug(f"❌ {ticker}: Нет displacement")
        return None
    
    logger.info(f"🔍 {ticker}: Шаг 2 пройден - есть displacement")
    
    # Шаг 3: Всплеск объема
    if not check_volume_spike(df):
        logger.debug(f"❌ {ticker}: Нет всплеска объема")
        return None
    
    logger.info(f"🔍 {ticker}: Шаг 3 пройден - всплеск объема")
    
    # Шаг 4: Fair Value Gap
    fvg = find_fair_value_gap(df)
    if not fvg:
        logger.debug(f"❌ {ticker}: Нет FVG")
        return None
    
    logger.info(f"🔍 {ticker}: Шаг 4 пройден - найден FVG {fvg}")
    
    # Шаг 5: Анализ стакана
    ob_bias = analyze_orderbook_bias(bid_vol, ask_vol)
    
    if ob_bias != "NO_DATA":
        if side == "LONG" and ob_bias in ["SELL", "STRONG_SELL"]:
            logger.info(f"❌ {ticker}: LONG отвергнут стаканом ({ob_bias})")
            return None
        if side == "SHORT" and ob_bias in ["BUY", "STRONG_BUY"]:
            logger.info(f"❌ {ticker}: SHORT отвергнут стаканом ({ob_bias})")
            return None
    
    # Фильтр близости к FVG
    current_price = df.iloc[-1]["close"]
    fvg_low, fvg_high = min(fvg), max(fvg)
    
    price_in_range = False
    if fvg_low > 0:
        dist_to_low = abs(current_price - fvg_low) / fvg_low
        dist_to_high = abs(current_price - fvg_high) / fvg_high
        if dist_to_low < 0.02 or dist_to_high < 0.02:
            price_in_range = True
    
    if not price_in_range:
        logger.info(f"❌ {ticker}: Цена {current_price:.2f} далеко от FVG {fvg}")
        return None
    
    logger.info(f"🎯 {ticker}: ВСЕ ШАГИ ПРОЙДЕНЫ! Сигнал {side}")
    
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
        
        # Форматируем объемы
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
        
        logger.info(f"📤 СИГНАЛ ОТПРАВЛЕН: {ticker} {signal['side']}")
        
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")
    except:
        pass


# ================= COMMANDS =================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    tickers_str = ", ".join(TICKERS[:5])
    text = (
        "🚀 <b>SMC Trading Bot v4.0</b>\n\n"
        "📊 <b>Мониторинг в реальном времени:</b>\n"
        f"• Тикеров: {len(TICKERS)}\n"
        f"• Первые 5: {tickers_str}...\n"
        f"• Интервал: {INTERVAL} мин\n"
        "• Стратегия: Smart Money Concepts\n\n"
        "🔍 <b>Что ищет бот:</b>\n"
        "• Захват ликвидности (Liquidity Grab)\n"
        "• Импульсное движение (Displacement)\n"
        "• Всплеск объема (Volume Spike)\n"
        "• Fair Value Gap (FVG)\n"
        "• Подтверждение стаканом\n\n"
        "<b>📋 Команды:</b>\n"
        "/status - статус мониторинга\n"
        "/tickers - все тикеры\n"
        "/signals - последние сигналы\n"
        "/help - справка по стратегии"
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
        f"⏱ Интервал: {INTERVAL} мин\n"
        f"✅ С данными: {ticks_with_data}\n"
        f"❌ Без данных: {ticks_without_data}\n\n"
        f"<i>Сканирование каждые {INTERVAL} мин</i>"
    )
    await update.message.reply_text(text, parse_mode='HTML')


async def tickers_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /tickers"""
    tickers_list = "\n".join([f"• {t}" for t in TICKERS])
    text = (
        f"📊 <b>Отслеживаемые тикеры ({len(TICKERS)}):</b>\n\n"
        f"{tickers_list}"
    )
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
        "<b>🎯 Стратегия Smart Money:</b>\n\n"
        "1️⃣ <b>Захват ликвидности</b>\n"
        "   Пробой 20-периодного max/min\n"
        "   и возврат цены обратно\n\n"
        "2️⃣ <b>Импульсное движение</b>\n"
        "   Тело свечи > 1.5x среднего\n"
        "   за 20 периодов\n\n"
        "3️⃣ <b>Всплеск объема</b>\n"
        "   Объем > 1.5x среднего\n"
        "   за 20 периодов\n\n"
        "4️⃣ <b>Fair Value Gap</b>\n"
        "   Ценовой разрыв между\n"
        "   1 и 3 свечой назад\n\n"
        "5️⃣ <b>Стакан</b>\n"
        "   Соотношение BID/ASK\n"
        "   (если доступен)\n\n"
        "<b>📋 Команды:</b>\n"
        "/start - о боте\n"
        "/status - статистика\n"
        "/tickers - список\n"
        "/signals - сигналы\n"
        "/help - справка"
    )
    await update.message.reply_text(text, parse_mode='HTML')


# ================= MAIN PROCESSING =================

async def process_ticker(session, ticker, bot):
    """Обработка одного тикера с диагностикой"""
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
        logger.error(f"❌ {ticker}: Ошибка - {e}")


async def health_check(bot):
    """Проверка работоспособности"""
    try:
        tickers_str = ", ".join(TICKERS[:5])
        await bot.send_message(
            chat_id=CHAT_ID,
            text="✅ <b>Бот запущен!</b>\n"
                 f"📊 Тикеров: {len(TICKERS)}\n"
                 f"📋 {tickers_str}...\n"
                 f"⏱ Интервал: {INTERVAL} мин\n"
                 f"🔧 SMC Стратегия активна\n\n"
                 "<i>Команды: /start, /status, /tickers, /signals, /help</i>",
            parse_mode='HTML'
        )
        logger.info("Health check sent")
    except:
        pass


async def main_loop():
    """Основной цикл с командами"""
    global total_cycles, start_time
    
    # Инициализация бота с командами
    application = Application.builder().token(TOKEN).build()
    bot = application.bot
    
    # Добавляем команды
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("tickers", tickers_cmd))
    application.add_handler(CommandHandler("signals", signals_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    
    # Запускаем polling
    await application.initialize()
    await application.start()
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("=" * 60)
        logger.info("🚀 SMC Trading Bot v4.0 Started")
        logger.info(f"📊 {len(TICKERS)} tickers: {', '.join(TICKERS)}")
        logger.info(f"⏱ Interval: {INTERVAL} min")
        logger.info("📋 Commands: /start /status /tickers /signals /help")
        logger.info("=" * 60)
        
        await health_check(bot)
        
        while True:
            try:
                total_cycles += 1
                
                # Синхронизация с закрытием свечи
                now = time.time()
                seconds_to_next = (INTERVAL * 60) - (now % (INTERVAL * 60))
                wait_time = max(seconds_to_next + 1, 1)
                
                logger.info(f"⏳ Цикл #{total_cycles}: ожидание {wait_time:.0f}с до следующей свечи")
                await asyncio.sleep(wait_time)
                
                logger.info(f"🔄 Цикл #{total_cycles}: сканирование {len(TICKERS)} тикеров...")
                
                tasks = [process_ticker(session, ticker, bot) for ticker in TICKERS]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                logger.info(f"✅ Цикл #{total_cycles}: завершен (данных: {ticks_with_data}, без данных: {ticks_without_data})")
                
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Ошибка в главном цикле: {e}")
                await asyncio.sleep(60)
    
    await application.stop()


# ================= ENTRY POINT =================

if __name__ == "__main__":
    try:
        start_time = time.time()
        logger.info("Запуск SMC Trading Bot v4.0...")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Бот завершил работу")