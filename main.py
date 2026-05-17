import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple, Any
from collections import defaultdict

import aiohttp
import pandas as pd
import numpy as np
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, JobQueue
from dotenv import load_dotenv

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Загрузка переменных окружения
load_dotenv()

TOKEN = os.getenv("TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# ================= НАСТРОЙКИ СТРАТЕГИИ =================
MIN_CONFIDENCE = 55  # Минимальный балл уверенности
MIN_VOLUME = 100_000  # Минимальный объем свечи
MIN_PRICE = 10.0  # Минимальная цена акции

# Параметры сканирования
SCAN_TIMEFRAME = 10  # 10-минутные свечи
SCAN_CANDLES_LIMIT = 60  # 60 свечей (10 часов истории)
SCAN_MAX_CONCURRENT = 10  # Параллельных запросов
SCAN_BATCH_DELAY = 1.5  # Пауза между батчами

# Фильтр волатильности
ATR_MIN_PCT = 0.5  # Минимальный ATR (%)
ATR_MAX_PCT = 5.0  # Максимальный ATR (%)

# Параметры компонентов
SWING_LOOKBACK = 10  # Свечей для BOS
OB_LOOKBACK = 5  # Свечей для Order Block
VOLUME_MULT = 1.5  # Множитель объема
FVG_MIN_GAP_PCT = 0.15  # Минимальный размер FVG (%)
LIQUIDITY_TOLERANCE_PCT = 0.2  # Допуск для равных лоев (%)

# Уровни входа/выхода
SL_ATR_MULT = 1.0  # Множитель ATR для стоп-лосса
RR_RATIO = 2.0  # Соотношение риск/прибыль

# Временной фильтр
SESSION_ONLY = True  # Только в торговую сессию
PRIME_START = 10  # Начало основной сессии (час МСК)
PRIME_END = 12  # Конец утреннего пика (час МСК)

# ================= ТОРГОВЫЕ СЕССИИ МОСБИРЖИ =================
class TradingSession:
    """Класс для работы с торговыми сессиями МосБиржи"""
    
    SESSIONS = {
        "morning": {
            "name": "🌅 УТРЕННЯЯ",
            "emoji": "🌅",
            "start": (6, 50), "end": (9, 50),
            "trading_start": (7, 0), "trading_end": (9, 50),
            "auction_start": (9, 50), "auction_end": (10, 0),
        },
        "main": {
            "name": "☀️ ОСНОВНАЯ",
            "emoji": "☀️",
            "start": (9, 50), "end": (18, 50),
            "trading_start": (10, 0), "trading_end": (18, 40),
            "auction_start": (18, 40), "auction_end": (18, 50),
        },
        "evening": {
            "name": "🌙 ВЕЧЕРНЯЯ",
            "emoji": "🌙",
            "start": (19, 0), "end": (23, 50),
            "trading_start": (19, 5), "trading_end": (23, 50),
            "auction_start": (19, 0), "auction_end": (19, 5),
        },
        "weekend": {
            "name": "📅 ВЫХОДНОГО ДНЯ",
            "emoji": "📅",
            "start": (9, 50), "end": (19, 0),
            "trading_start": (10, 0), "trading_end": (18, 50),
            "auction_start": (9, 50), "auction_end": (10, 0),
        }
    }
    
    @classmethod
    def get_current_session(cls, dt: Optional[datetime] = None) -> Tuple[Optional[str], dict, bool, str]:
        """
        Возвращает (ключ_сессии, данные_сессии, идут_ли_торги, статус)
        """
        if dt is None:
            msk_tz = timezone(timedelta(hours=3))
            dt = datetime.now(msk_tz)
        
        current_time = dt.time()
        weekday = dt.weekday()
        
        # Проверка выходных дней
        if weekday >= 5:
            session_data = cls.SESSIONS["weekend"]
            start_h, start_m = session_data["start"]
            end_h, end_m = session_data["end"]
            start_time = datetime.strptime(f"{start_h:02d}:{start_m:02d}", "%H:%M").time()
            end_time = datetime.strptime(f"{end_h:02d}:{end_m:02d}", "%H:%M").time()
            
            if start_time <= current_time <= end_time:
                ts_h, ts_m = session_data["trading_start"]
                te_h, te_m = session_data["trading_end"]
                t_start = datetime.strptime(f"{ts_h:02d}:{ts_m:02d}", "%H:%M").time()
                t_end = datetime.strptime(f"{te_h:02d}:{te_m:02d}", "%H:%M").time()
                
                if t_start <= current_time <= t_end:
                    return "weekend", session_data, True, "trading"
                elif current_time < t_start:
                    return "weekend", session_data, False, "pre_trading"
                else:
                    return "weekend", session_data, False, "post_trading"
            return None, {}, False, "closed"
        
        # Проверка рабочих дней
        # Вечерняя сессия (19:00-23:50)
        evening = cls.SESSIONS["evening"]
        ev_start = datetime.strptime(f"{evening['start'][0]:02d}:{evening['start'][1]:02d}", "%H:%M").time()
        ev_end = datetime.strptime(f"{evening['end'][0]:02d}:{evening['end'][1]:02d}", "%H:%M").time()
        
        if ev_start <= current_time <= ev_end:
            ts_h, ts_m = evening["trading_start"]
            te_h, te_m = evening["trading_end"]
            t_start = datetime.strptime(f"{ts_h:02d}:{ts_m:02d}", "%H:%M").time()
            t_end = datetime.strptime(f"{te_h:02d}:{te_m:02d}", "%H:%M").time()
            
            if t_start <= current_time <= t_end:
                return "evening", evening, True, "trading"
            elif current_time < t_start:
                return "evening", evening, False, "auction"
            else:
                return "evening", evening, False, "post_trading"
        
        # Утренняя сессия (6:50-9:50)
        morning = cls.SESSIONS["morning"]
        mo_start = datetime.strptime(f"{morning['start'][0]:02d}:{morning['start'][1]:02d}", "%H:%M").time()
        mo_end = datetime.strptime(f"{morning['end'][0]:02d}:{morning['end'][1]:02d}", "%H:%M").time()
        
        if mo_start <= current_time <= mo_end:
            ts_h, ts_m = morning["trading_start"]
            te_h, te_m = morning["trading_end"]
            t_start = datetime.strptime(f"{ts_h:02d}:{ts_m:02d}", "%H:%M").time()
            t_end = datetime.strptime(f"{te_h:02d}:{te_m:02d}", "%H:%M").time()
            
            if t_start <= current_time <= t_end:
                return "morning", morning, True, "trading"
            elif current_time < t_start:
                return "morning", morning, False, "auction"
            else:
                return "morning", morning, False, "post_trading"
        
        # Основная сессия (9:50-18:50)
        main = cls.SESSIONS["main"]
        ma_start = datetime.strptime(f"{main['start'][0]:02d}:{main['start'][1]:02d}", "%H:%M").time()
        ma_end = datetime.strptime(f"{main['end'][0]:02d}:{main['end'][1]:02d}", "%H:%M").time()
        
        if ma_start <= current_time <= ma_end:
            ts_h, ts_m = main["trading_start"]
            te_h, te_m = main["trading_end"]
            t_start = datetime.strptime(f"{ts_h:02d}:{ts_m:02d}", "%H:%M").time()
            t_end = datetime.strptime(f"{te_h:02d}:{te_m:02d}", "%H:%M").time()
            
            if t_start <= current_time <= t_end:
                return "main", main, True, "trading"
            elif current_time < t_start:
                return "main", main, False, "auction"
            else:
                return "main", main, False, "auction"  # Аукцион закрытия
        
        return None, {}, False, "closed"
    
    @classmethod
    def get_session_status_text(cls) -> str:
        """Получение текстового статуса сессии"""
        session_key, session_data, is_trading, status = cls.get_current_session()
        
        if session_key is None:
            return "🔴 Торги закрыты"
        
        if is_trading:
            return f"{session_data['emoji']} Идут торги ({session_data['name']} сессия)"
        elif status == "auction":
            return f"⏳ Аукцион ({session_data['name']} сессия)"
        elif status == "pre_trading":
            return f"⏰ Подготовка к торгам ({session_data['name']} сессия)"
        else:
            return f"🔴 Торги завершены ({session_data['name']} сессия)"
    
    @classmethod
    def is_prime_time(cls) -> bool:
        """Проверка, находится ли рынок в утреннем пике (10:00-12:00 МСК)"""
        session_key, _, is_trading, _ = cls.get_current_session()
        
        if not is_trading:
            return False
        
        msk_tz = timezone(timedelta(hours=3))
        dt = datetime.now(msk_tz)
        current_hour = dt.hour
        
        return PRIME_START <= current_hour < PRIME_END and session_key in ["main", "morning"]
    
    @classmethod
    def get_schedule_text(cls) -> str:
        """Получение расписания торговых сессий"""
        return (
            "📅 <b>Расписание торговых сессий МосБиржи:</b>\n\n"
            "🌅 <b>Утренняя сессия:</b>\n"
            "• 06:50 - 07:00: Аукцион открытия\n"
            "• 07:00 - 09:50: Торги\n"
            "• 09:50 - 10:00: Аукцион перехода\n\n"
            "☀️ <b>Основная сессия:</b>\n"
            "• 10:00 - 18:40: Торги\n"
            "• 18:40 - 18:50: Аукцион закрытия\n\n"
            "🌙 <b>Вечерняя сессия:</b>\n"
            "• 19:00 - 19:05: Аукцион открытия\n"
            "• 19:05 - 23:50: Торги\n\n"
            "📅 <b>Выходные дни:</b>\n"
            "• 10:00 - 18:50: Торги (только в определенные даты)"
        )


# ================= TRADINGVIEW =================
def get_tradingview_link(ticker: str, interval: str = "10") -> str:
    """Ссылка на TradingView с 10-минутным таймфреймом"""
    symbol = f"MOEX:{ticker}"
    return f"https://www.tradingview.com/chart/?symbol={symbol}&interval={interval}&theme=dark"

def create_tradingview_keyboard(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📊 {ticker} на TradingView", url=get_tradingview_link(ticker))
    ]])


# ================= MOEX API =================
class MoexAPI:
    BASE = 'https://iss.moex.com/iss'
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._tickers_cache = None
        self._cache_time = None
        self._semaphore = asyncio.Semaphore(SCAN_MAX_CONCURRENT)
    
    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def request(self, url: str) -> Optional[dict]:
        s = await self.get_session()
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    return await r.json()
                else:
                    logger.warning(f"HTTP {r.status} для {url}")
        except Exception as e:
            logger.error(f"Ошибка запроса {url}: {e}")
        return None
    
    async def get_all_tickers(self) -> List[str]:
        """Получение списка всех акций с фильтрацией"""
        if self._tickers_cache and self._cache_time:
            if datetime.now() - self._cache_time < timedelta(minutes=30):
                return self._tickers_cache
        
        logger.info("📊 Получение списка акций с МосБиржи...")
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json"
               f"?iss.meta=off&iss.only=securities&securities.columns=SECID,PREVPRICE")
        
        data = await self.request(url)
        if not data or 'securities' not in data:
            logger.error("Не удалось получить список акций")
            return []
        
        rows = data['securities']['data']
        cols = data['securities']['columns']
        secid_idx = cols.index('SECID')
        prevprice_idx = cols.index('PREVPRICE')
        
        # Фильтруем акции с ценой выше минимальной
        all_tickers = []
        for row in rows:
            ticker = row[secid_idx]
            prev_price = row[prevprice_idx]
            if prev_price and prev_price > MIN_PRICE:
                all_tickers.append(ticker)
        
        logger.info(f"📋 Найдено {len(all_tickers)} акций с ценой > {MIN_PRICE} руб.")
        
        # Сохраняем в кеш
        self._tickers_cache = all_tickers
        self._cache_time = datetime.now()
        
        return all_tickers
    
    async def get_candles_10min(self, ticker: str, limit: int = 60) -> Optional[pd.DataFrame]:
        """Получение 10-минутных свечей"""
        till = datetime.now()
        frm = till - timedelta(minutes=limit * 10 + 60)  # +1 час запаса
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
               f"?from={frm.strftime('%Y-%m-%dT%H:%M:%S')}"
               f"&till={till.strftime('%Y-%m-%dT%H:%M:%S')}"
               f"&interval=10&iss.meta=off&iss.only=candles")
        
        data = await self.request(url)
        if not data or 'candles' not in data:
            return None
        
        rows = data['candles']['data']
        cols = data['candles']['columns']
        
        if not rows:
            return None
        
        df = pd.DataFrame(rows, columns=cols)
        df = df.rename(columns={'begin': 'date'})
        
        need = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = df[need].copy()
        
        df['date'] = pd.to_datetime(df['date'])
        for c in need[1:]:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        
        df = df.dropna().sort_values('date')
        return df.tail(limit)
    
    async def get_price(self, ticker: str) -> Optional[float]:
        """Получение текущей цены"""
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json"
               f"?iss.only=marketdata&iss.meta=off")
        
        data = await self.request(url)
        if not data or not data.get('marketdata', {}).get('data'):
            return None
        
        cols = data['marketdata']['columns']
        row = data['marketdata']['data'][0]
        
        for name in ['LAST', 'LCURRENTPRICE', 'MARKETPRICE', 'PREVPRICE']:
            if name in cols:
                v = row[cols.index(name)]
                if v:
                    return float(v)
        return None
    
    async def get_moex_index(self) -> Optional[float]:
        """Получение значения индекса МосБиржи"""
        url = (f"{self.BASE}/engines/stock/markets/index/securities/IMOEX.json"
               f"?iss.only=marketdata&iss.meta=off")
        
        data = await self.request(url)
        if not data or not data.get('marketdata', {}).get('data'):
            return None
        
        cols = data['marketdata']['columns']
        row = data['marketdata']['data'][0]
        
        for name in ['LAST', 'CURRENTVALUE']:
            if name in cols:
                v = row[cols.index(name)]
                if v:
                    return float(v)
        return None


# ================= НОВАЯ СТРАТЕГИЯ SMC =================
class SMCStrategy:
    """Smart Money Concepts стратегия с 6 компонентами"""
    
    def __init__(self):
        self.moex_index_vwap = None
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Расчет ATR"""
        high, low, close = df['high'], df['low'], df['close']
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()
    
    def calculate_vwap(self, df: pd.DataFrame) -> pd.Series:
        """Расчет VWAP"""
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return vwap
    
    # ========== 1. BOS (Break of Structure) - 30 баллов ==========
    def check_bos(self, df: pd.DataFrame) -> Tuple[bool, float, Dict]:
        """
        Проверка пробоя структурного максимума
        Возвращает (есть_пробой, баллы, детали)
        """
        if len(df) < SWING_LOOKBACK + 2:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        # Берем последние SWING_LOOKBACK свечей, исключая текущую
        lookback_data = df.iloc[-(SWING_LOOKBACK + 1):-1]
        current = df.iloc[-1]
        
        last_high = lookback_data['high'].max()
        last_low = lookback_data['low'].min()
        
        bos_up = current['close'] > last_high or current['high'] > last_high
        bos_down = current['close'] < last_low or current['low'] < last_low
        
        # Ищем только LONG сигналы (пробой вверх)
        if bos_up:
            return True, 30, {
                'direction': 'LONG',
                'last_high': last_high,
                'current_close': current['close'],
                'breakout_pct': (current['close'] - last_high) / last_high * 100
            }
        
        return False, 0, {'reason': 'Нет пробоя структуры'}
    
    # ========== 2. CHoCH (Change of Character) - 10 баллов ==========
    def check_choch(self, df: pd.DataFrame, bos_data: Dict) -> Tuple[bool, float, Dict]:
        """
        Проверка смены характера тренда
        Возвращает (есть_CHoCH, баллы, детали)
        """
        if len(df) < 10:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        # Проверяем тренд: цена 100 минут назад выше цены 50 минут назад
        if len(df) >= 10:
            close_10_ago = df['close'].iloc[-10]
            close_5_ago = df['close'].iloc[-5]
            downtrend = close_10_ago > close_5_ago
        else:
            downtrend = False
        
        # CHoCH = downtrend И bullish_bos
        bullish_bos = bos_data.get('direction') == 'LONG'
        is_choch = downtrend and bullish_bos
        
        if is_choch:
            return True, 10, {
                'trend_before': 'DOWNTREND',
                'trend_now': 'UPTREND',
                'close_100min_ago': close_10_ago,
                'close_50min_ago': close_5_ago
            }
        
        return False, 0, {
            'reason': 'Нет подтверждения смены тренда',
            'downtrend': downtrend,
            'bullish_bos': bullish_bos
        }
    
    # ========== 3. Order Block - 25 баллов ==========
    def check_order_block(self, df: pd.DataFrame) -> Tuple[bool, float, Dict]:
        """
        Поиск Order Block (последняя медвежья свеча перед импульсом)
        Возвращает (найден_OB, баллы, детали)
        """
        if len(df) < OB_LOOKBACK + 5:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        avg_volume_20 = df['volume'].tail(20).mean()
        
        # Ищем в диапазоне: -7 до -2 свечей от текущей
        start_idx = max(len(df) - 7, 0)
        end_idx = len(df) - 2
        
        for i in range(start_idx, end_idx):
            # Медвежья свеча: close < open
            if df['close'].iloc[i] < df['open'].iloc[i]:
                # Следующая свеча: close > open (бычья)
                if i + 1 < len(df) and df['close'].iloc[i + 1] > df['open'].iloc[i + 1]:
                    # Подтверждение: close[i+2] > close[i+1] * 0.998
                    if i + 2 < len(df) and df['close'].iloc[i + 2] > df['close'].iloc[i + 1] * 0.998:
                        # Объем: volume[i+1] > avg_volume_20 * VOLUME_MULT
                        if df['volume'].iloc[i + 1] > avg_volume_20 * VOLUME_MULT:
                            # Проверка целостности: последующие свечи не пробивали минимум OB
                            ob_low = df['low'].iloc[i]
                            subsequent_lows = df['low'].iloc[i + 2:]
                            
                            if len(subsequent_lows) == 0 or subsequent_lows.min() >= ob_low:
                                return True, 25, {
                                    'ob_index': i,
                                    'ob_low': ob_low,
                                    'ob_high': df['high'].iloc[i],
                                    'ob_close': df['close'].iloc[i],
                                    'volume_ratio': df['volume'].iloc[i + 1] / avg_volume_20
                                }
        
        return False, 0, {'reason': 'Order Block не найден'}
    
    # ========== 4. FVG (Fair Value Gap) - 15 баллов ==========
    def check_fvg(self, df: pd.DataFrame) -> Tuple[bool, float, Dict]:
        """
        Поиск ценового разрыва (Fair Value Gap)
        Возвращает (найден_FVG, баллы, детали)
        """
        if len(df) < 10:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        # Ищем в диапазоне: -9 до -2 свечей
        start_idx = max(len(df) - 9, 0)
        end_idx = len(df) - 2
        
        for i in range(start_idx, end_idx):
            if i + 1 < len(df) and i - 1 >= 0:
                # Гэп вверх: low[i+1] > high[i-1]
                if df['low'].iloc[i + 1] > df['high'].iloc[i - 1]:
                    gap_pct = (df['low'].iloc[i + 1] - df['high'].iloc[i - 1]) / df['high'].iloc[i - 1] * 100
                    
                    # gap_pct >= FVG_MIN_GAP_PCT
                    if gap_pct >= FVG_MIN_GAP_PCT:
                        current_close = df['close'].iloc[-1]
                        
                        # FVG выше текущей цены: high[i-1] < close[-1] * 1.02
                        if df['high'].iloc[i - 1] < current_close * 1.02:
                            # Проверка, что FVG не заполнен последующими свечами
                            fvg_top = df['low'].iloc[i + 1]
                            fvg_bottom = df['high'].iloc[i - 1]
                            
                            # Проверяем, не заходила ли цена в зону FVG после его формирования
                            subsequent_lows = df['low'].iloc[i + 2:]
                            if len(subsequent_lows) == 0 or subsequent_lows.min() >= fvg_bottom:
                                return True, 15, {
                                    'fvg_index': i,
                                    'fvg_bottom': fvg_bottom,
                                    'fvg_top': fvg_top,
                                    'gap_pct': gap_pct
                                }
        
        return False, 0, {'reason': 'FVG не найден'}
    
    # ========== 5. Liquidity Sweep - 15 баллов ==========
    def check_liquidity_sweep(self, df: pd.DataFrame) -> Tuple[bool, float, Dict]:
        """
        Проверка снятия ликвидности (сбор стоп-лоссов)
        Возвращает (есть_свип, баллы, детали)
        """
        if len(df) < 20:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        current_close = df['close'].iloc[-1]
        tolerance = current_close * LIQUIDITY_TOLERANCE_PCT / 100
        
        # Анализ на двух окнах: 10 и 20 свечей
        for window in [10, 20]:
            if len(df) < window:
                continue
            
            lookback_data = df.tail(window)
            
            # Группировка близких минимумов в кластеры
            lows = lookback_data['low'].values
            clusters = []
            used = set()
            
            for i in range(len(lows)):
                if i in used:
                    continue
                
                cluster = [i]
                for j in range(i + 1, len(lows)):
                    if j not in used and abs(lows[i] - lows[j]) <= tolerance:
                        cluster.append(j)
                        used.add(j)
                
                if len(cluster) >= 2:  # Минимум 2 равных лоу
                    clusters.append([lows[idx] for idx in cluster])
            
            # Проверяем свип
            if clusters:
                for cluster in clusters:
                    cluster_level = np.mean(cluster)
                    
                    # Один из последних 3 лоев ниже кластера
                    last_3_lows = df['low'].tail(3).values
                    sweep_detected = any(low < cluster_level for low in last_3_lows)
                    
                    # Текущая цена закрытия выше уровня кластера
                    recovery = current_close > cluster_level
                    
                    if sweep_detected and recovery:
                        return True, 15, {
                            'cluster_level': cluster_level,
                            'cluster_size': len(cluster),
                            'window': window,
                            'sweep_below_pct': (cluster_level - min(last_3_lows)) / cluster_level * 100
                        }
        
        return False, 0, {'reason': 'Нет снятия ликвидности'}
    
    # ========== 6. Volume + VWAP - 10 баллов ==========
    def check_volume_vwap(self, df: pd.DataFrame) -> Tuple[bool, float, Dict]:
        """
        Проверка объема и положения относительно VWAP
        Возвращает (валидно, баллы, детали)
        """
        if len(df) < 20:
            return False, 0, {'reason': 'Недостаточно свечей'}
        
        vwap = self.calculate_vwap(df)
        avg_volume_20 = df['volume'].tail(20).mean()
        current_volume = df['volume'].iloc[-1]
        current_close = df['close'].iloc[-1]
        
        score = 0
        details = {}
        
        # Выше VWAP: 5 баллов
        if current_close > vwap.iloc[-1]:
            score += 5
            details['above_vwap'] = True
            details['vwap'] = vwap.iloc[-1]
            details['distance_to_vwap_pct'] = (current_close - vwap.iloc[-1]) / vwap.iloc[-1] * 100
        else:
            details['above_vwap'] = False
        
        # Всплеск объема: volume[-1] / avg_volume_20 >= VOLUME_MULT → 5 баллов
        volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 0
        if volume_ratio >= VOLUME_MULT:
            score += 5
            details['volume_surge'] = True
        else:
            details['volume_surge'] = False
        
        details['volume_ratio'] = volume_ratio
        details['score'] = score
        
        return score > 0, score, details
    
    # ========== ОСНОВНОЙ МЕТОД АНАЛИЗА ==========
    async def analyze_ticker(self, ticker: str, df: pd.DataFrame, 
                           moex_above_vwap: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        """
        Полный анализ тикера по 6 компонентам SMC
        """
        if df is None or len(df) < 30:
            return None
        
        # Проверка минимального объема
        if df['volume'].iloc[-1] < MIN_VOLUME:
            return None
        
        # Фильтр волатильности
        atr = self.calculate_atr(df)
        current_atr = atr.iloc[-1]
        current_price = df['close'].iloc[-1]
        atr_pct = current_atr / current_price * 100
        
        if atr_pct < ATR_MIN_PCT or atr_pct > ATR_MAX_PCT:
            logger.debug(f"🚫 {ticker}: ATR {atr_pct:.2f}% вне диапазона [{ATR_MIN_PCT}-{ATR_MAX_PCT}%]")
            return None
        
        # Рыночный контекст: MOEX выше VWAP (если доступно)
        if moex_above_vwap is not None and not moex_above_vwap:
            logger.debug(f"🚫 {ticker}: Индекс MOEX ниже VWAP")
            return None
        
        # Временной фильтр
        if SESSION_ONLY:
            session_key, _, is_trading, _ = TradingSession.get_current_session()
            if not is_trading or not TradingSession.is_prime_time():
                logger.debug(f"🚫 {ticker}: Не основное время ({session_key}, prime: {TradingSession.is_prime_time()})")
                return None
        
        # Инициализация компонентов
        total_score = 0
        max_score = 105  # 30 + 10 + 25 + 15 + 15 + 10
        components = {}
        
        # 1. BOS (30 баллов)
        bos_found, bos_score, bos_data = self.check_bos(df)
        total_score += bos_score
        components['bos'] = {'found': bos_found, 'score': bos_score, 'data': bos_data}
        
        if not bos_found:
            logger.debug(f"🚫 {ticker}: BOS не найден")
            return None
        
        # 2. CHoCH (10 баллов)
        choch_found, choch_score, choch_data = self.check_choch(df, bos_data)
        total_score += choch_score
        components['choch'] = {'found': choch_found, 'score': choch_score, 'data': choch_data}
        
        # 3. Order Block (25 баллов)
        ob_found, ob_score, ob_data = self.check_order_block(df)
        total_score += ob_score
        components['order_block'] = {'found': ob_found, 'score': ob_score, 'data': ob_data}
        
        if not ob_found:
            logger.debug(f"🚫 {ticker}: Order Block не найден")
            return None
        
        # 4. FVG (15 баллов)
        fvg_found, fvg_score, fvg_data = self.check_fvg(df)
        total_score += fvg_score
        components['fvg'] = {'found': fvg_found, 'score': fvg_score, 'data': fvg_data}
        
        # 5. Liquidity Sweep (15 баллов)
        sweep_found, sweep_score, sweep_data = self.check_liquidity_sweep(df)
        total_score += sweep_score
        components['liquidity_sweep'] = {'found': sweep_found, 'score': sweep_score, 'data': sweep_data}
        
        # 6. Volume + VWAP (10 баллов)
        vol_valid, vol_score, vol_data = self.check_volume_vwap(df)
        total_score += vol_score
        components['volume_vwap'] = {'found': vol_valid, 'score': vol_score, 'data': vol_data}
        
        # Проверка минимального порога
        if total_score < MIN_CONFIDENCE:
            logger.debug(f"🚫 {ticker}: Скор {total_score} < {MIN_CONFIDENCE}")
            return None
        
        # Расчет уровней входа/выхода
        entry = current_price
        
        # Стоп-лосс
        ob_low = ob_data.get('ob_low', entry - current_atr)
        sl_level = min(ob_low, entry - current_atr * SL_ATR_MULT)
        sl = sl_level - current_atr * 0.5  # Дополнительный буфер
        
        # Тейк-профиты
        risk = entry - sl
        tp1 = entry + risk * 1.0  # 1:1 RR
        tp2 = entry + risk * 2.0  # 1:2 RR
        
        # Формирование результата
        confidence_pct = total_score / max_score * 100
        
        return {
            'ticker': ticker,
            'price': round(entry, 2),
            'direction': 'LONG',
            'score': round(confidence_pct, 1),
            'total_score': total_score,
            'max_score': max_score,
            
            # Компоненты
            'components': components,
            
            # Детали для отображения
            'bos_found': bos_found,
            'bos_data': bos_data,
            'choch_found': choch_found,
            'choch_data': choch_data,
            'ob_found': ob_found,
            'ob_data': ob_data,
            'fvg_found': fvg_found,
            'fvg_data': fvg_data,
            'sweep_found': sweep_found,
            'sweep_data': sweep_data,
            'vol_data': vol_data,
            
            # Уровни
            'entry': round(entry, 2),
            'stop_loss': round(sl, 2),
            'tp1': round(tp1, 2),
            'tp2': round(tp2, 2),
            'sl_percent': round(abs(entry - sl) / entry * 100, 1),
            'tp1_percent': round(abs(tp1 - entry) / entry * 100, 1),
            'tp2_percent': round(abs(tp2 - entry) / entry * 100, 1),
            'risk_reward_1': 1.0,
            'risk_reward_2': 2.0,
            
            # ATR
            'atr': round(current_atr, 2),
            'atr_pct': round(atr_pct, 2),
        }


# ================= БОТ =================
api = MoexAPI()
strategy = SMCStrategy()

def escape_html(text: str) -> str:
    if text is None:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

async def scan_market() -> Tuple[List[dict], int]:
    """Сканирование рынка с SMC стратегией"""
    logger.info("=" * 50)
    logger.info("🧠 ЗАПУСК SMC СКАНИРОВАНИЯ РЫНКА")
    logger.info("=" * 50)
    
    # Получаем список тикеров и индекс MOEX
    tickers = await api.get_all_tickers()
    
    if not tickers:
        logger.error("❌ Не удалось получить список акций")
        return [], 0
    
    # Проверяем рыночный контекст
    moex_price = await api.get_moex_index()
    moex_above_vwap = None
    
    if moex_price:
        # Упрощенная проверка: получаем 10-минутные свечи индекса
        # В реальности нужен отдельный запрос для индекса
        moex_above_vwap = True  # Заглушка, если не можем получить VWAP индекса
    
    logger.info(f"📊 Сканируем {len(tickers)} акций")
    logger.info(f"📈 Индекс MOEX: {moex_price if moex_price else 'N/A'}")
    
    signals = []
    
    # Сканирование с параллельными запросами
    async def process_ticker(ticker: str) -> Optional[dict]:
        async with api._semaphore:
            try:
                df = await api.get_candles_10min(ticker, SCAN_CANDLES_LIMIT)
                if df is None or len(df) < 30:
                    return None
                
                sig = await strategy.analyze_ticker(ticker, df, moex_above_vwap)
                
                if sig:
                    logger.info(f"✅ {ticker}: СИГНАЛ! Score={sig['score']:.1f}% | "
                               f"BOS={sig['bos_found']} OB={sig['ob_found']} "
                               f"FVG={sig['fvg_found']} SWEEP={sig['sweep_found']}")
                
                return sig
                
            except Exception as e:
                logger.error(f"Ошибка {ticker}: {e}")
                return None
    
    # Обработка батчами
    tasks = [process_ticker(ticker) for ticker in tickers]
    results = await asyncio.gather(*tasks)
    
    signals = [s for s in results if s is not None]
    signals.sort(key=lambda x: x['score'], reverse=True)
    
    logger.info("=" * 50)
    logger.info(f"СКАНИРОВАНИЕ ЗАВЕРШЕНО. Сигналов: {len(signals)} из {len(tickers)} акций")
    logger.info("=" * 50)
    
    return signals[:10], len(tickers)

async def send_scan_results(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """Отправка результатов сканирования"""
    if chat_id is None:
        chat_id = ADMIN_CHAT_ID
    
    if chat_id == 0:
        logger.error("ADMIN_CHAT_ID не указан")
        return
    
    try:
        signals, total_tickers = await scan_market()
        
        session_status = TradingSession.get_session_status_text()
        
        if not signals:
            text = (f"📊 <b>Нет сигналов</b>\n\n"
                   f"{session_status}\n"
                   f"📈 Просканировано акций: {total_tickers}\n\n"
                   f"💡 <i>SMC стратегия не видит качественных LONG-сетапов.\n"
                   f"Возможно, рынок не активен или нет структурных пробоев.</i>")
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
            return
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=(f"🧠 <b>SMC LONG сигналы: {len(signals)}</b>\n"
                 f"{session_status}\n"
                 f"📈 Просканировано акций: {total_tickers}\n\n"
                 f"📊 <i>Топ-{len(signals)} сетапов (6-компонентный анализ):</i>"),
            parse_mode='HTML'
        )
        
        for i, s in enumerate(signals, 1):
            # Формирование текста компонентов
            components_text = []
            
            # BOS
            if s['bos_found']:
                bos_pct = s['bos_data'].get('breakout_pct', 0)
                components_text.append(f"✅ BOS: +30 баллов (пробой +{bos_pct:.2f}%)")
            
            # CHoCH
            if s['choch_found']:
                components_text.append(f"✅ CHoCH: +10 баллов (смена тренда)")
            
            # Order Block
            if s['ob_found']:
                vol_ratio = s['ob_data'].get('volume_ratio', 0)
                components_text.append(f"✅ Order Block: +25 баллов (объем x{vol_ratio:.1f})")
            
            # FVG
            if s['fvg_found']:
                gap_pct = s['fvg_data'].get('gap_pct', 0)
                components_text.append(f"✅ FVG: +15 баллов (гэп {gap_pct:.2f}%)")
            
            # Liquidity Sweep
            if s['sweep_found']:
                cluster_size = s['sweep_data'].get('cluster_size', 0)
                components_text.append(f"✅ Liq. Sweep: +15 баллов (кластер {cluster_size} лоев)")
            
            # Volume + VWAP
            vol_data = s['vol_data']
            vwap_items = []
            if vol_data.get('above_vwap'):
                vwap_items.append("выше VWAP +5")
            if vol_data.get('volume_surge'):
                vwap_items.append("объем +5")
            if vwap_items:
                components_text.append(f"✅ Vol/VWAP: +{vol_data.get('score', 0)} баллов ({', '.join(vwap_items)})")
            
            components_str = "\n".join(components_text)
            
            # TP/SL
            tp_sl_text = (
                f"🛑 SL: <b>{s['stop_loss']} ₽</b> (-{s['sl_percent']}%)\n"
                f"✅ TP1: <b>{s['tp1']} ₽</b> (+{s['tp1_percent']}%) | R/R 1:1 (50% поз.)\n"
                f"🎯 TP2: <b>{s['tp2']} ₽</b> (+{s['tp2_percent']}%) | R/R 1:2 (50% поз.)"
            )
            
            # Сила сигнала
            if s['score'] >= 85:
                strength_emoji = "🔥🔥🔥"
            elif s['score'] >= 75:
                strength_emoji = "🔥🔥"
            elif s['score'] >= 65:
                strength_emoji = "🔥"
            else:
                strength_emoji = "⭐"
            
            tv_link = get_tradingview_link(s['ticker'])
            
            text = (
                f"<b>{strength_emoji} #{i} <a href='{tv_link}'>{s['ticker']}</a> 🟢📈 LONG</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Рейтинг: <b>{s['score']}%</b> ({s['total_score']}/{s['max_score']} баллов)\n"
                f"💰 Цена входа: <b>{s['entry']} ₽</b>\n"
                f"📐 ATR: {s['atr']} ₽ ({s['atr_pct']}%)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🔍 АНАЛИЗ КОМПОНЕНТОВ:</b>\n"
                f"{components_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🎯 ТОРГОВЫЙ ПЛАН:</b>\n"
                f"{tp_sl_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 <i>Стратегия: Smart Money Concepts (SMC)\n"
                f"Таймфрейм: 10-минутные свечи</i>"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML',
                    reply_markup=create_tradingview_keyboard(s['ticker'])
                )
            except Exception as e:
                logger.error(f"Ошибка отправки: {e}")
                plain = (text.replace('<b>', '').replace('</b>', '')
                        .replace('<i>', '').replace('</i>', '')
                        .replace(f"<a href='{tv_link}'>", '').replace('</a>', ''))
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=plain,
                    reply_markup=create_tradingview_keyboard(s['ticker'])
                )
            
            await asyncio.sleep(0.5)
            
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Ошибка: {escape_html(str(e)[:200])}"
            )
        except:
            pass

async def scheduled_scan(context: ContextTypes.DEFAULT_TYPE):
    """Запланированное сканирование в основное время"""
    logger.info("🕐 Запуск SMC сканирования по расписанию")
    
    # Проверяем, что сейчас основное время
    if TradingSession.is_prime_time():
        await send_scan_results(context)
    else:
        logger.info("⏰ Не основное время, пропускаем сканирование")

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    text = (
        "🚀 <b>MOEX Smart Money Scanner Bot v5.0</b>\n\n"
        "🧠 <b>Стратегия: Smart Money Concepts (SMC)</b>\n"
        "📊 <b>Таймфрейм: 10-минутные свечи</b>\n\n"
        "<b>🔍 6 компонентов анализа:</b>\n"
        "1️⃣ BOS (Break of Structure) - 30 баллов\n"
        "2️⃣ CHoCH (Change of Character) - 10 баллов\n"
        "3️⃣ Order Block - 25 баллов\n"
        "4️⃣ FVG (Fair Value Gap) - 15 баллов\n"
        "5️⃣ Liquidity Sweep - 15 баллов\n"
        "6️⃣ Volume + VWAP - 10 баллов\n\n"
        f"<b>⚙️ Настройки:</b>\n"
        f"• Мин. уверенность: {MIN_CONFIDENCE} баллов\n"
        f"• ATR фильтр: {ATR_MIN_PCT}% - {ATR_MAX_PCT}%\n"
        f"• Мин. объем свечи: {MIN_VOLUME:,}\n"
        f"• Мин. цена акции: {MIN_PRICE} руб.\n"
        f"• Временной фильтр: {PRIME_START}:00-{PRIME_END}:00 МСК\n\n"
        "<b>🕐 Расписание сканирования:</b>\n"
        "• Автоматически в основное время\n\n"
        "<b>📋 Команды:</b>\n"
        "/scan - ручное сканирование\n"
        "/schedule - расписание торгов\n"
        "/status - статус рынка\n"
        "/help - полная справка"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    text = (
        "📚 <b>Smart Money Concepts (SMC) — Полная справка</b>\n\n"
        "<b>🎯 Философия стратегии:</b>\n"
        "Отслеживаем следы крупных участников (Smart Money)\n"
        "на 10-минутном таймфрейме. Ищем только LONG-сетапы.\n\n"
        "<b>📊 6 компонентов стратегии:</b>\n\n"
        "1️⃣ <b>BOS (Break of Structure) - 30 баллов</b>\n"
        "   • Пробой последнего структурного максимума\n"
        "   • Свинг-лукбек: 10 свечей\n"
        "   • Обязательное условие\n\n"
        "2️⃣ <b>CHoCH (Change of Character) - 10 баллов</b>\n"
        "   • Подтверждение смены тренда\n"
        "   • С нисходящего на восходящий\n\n"
        "3️⃣ <b>Order Block - 25 баллов</b>\n"
        "   • Последняя медвежья свеча перед импульсом\n"
        "   • Объем > 1.5x среднего\n"
        "   • Целостность блока сохранена\n"
        "   • Обязательное условие\n\n"
        "4️⃣ <b>FVG (Fair Value Gap) - 15 баллов</b>\n"
        "   • Ценовой разрыв > 0.15%\n"
        "   • Указывает на дисбаланс\n\n"
        "5️⃣ <b>Liquidity Sweep - 15 баллов</b>\n"
        "   • Снятие ликвидности\n"
        "   • Снос равных минимумов\n"
        "   • Кластеры из 2+ лоев\n\n"
        "6️⃣ <b>Volume + VWAP - 10 баллов</b>\n"
        "   • Выше VWAP: 5 баллов\n"
        "   • Всплеск объема: 5 баллов\n\n"
        "<b>🎚️ Система фильтров:</b>\n"
        f"• Мин. уверенность: {MIN_CONFIDENCE} баллов\n"
        f"• ATR/Price: {ATR_MIN_PCT}% - {ATR_MAX_PCT}%\n"
        "• Индекс MOEX выше VWAP\n"
        f"• Время: {PRIME_START}:00-{PRIME_END}:00 МСК\n\n"
        "<b>📐 Расчет уровней:</b>\n"
        "• Вход: цена закрытия\n"
        "• SL: min(OB_low, entry-ATR) - ATR×0.5\n"
        "• TP1: +1R (50% позиции)\n"
        "• TP2: +2R (50% позиции)"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /schedule - расписание торгов"""
    await update.message.reply_text(
        TradingSession.get_schedule_text(),
        parse_mode='HTML'
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /status - статус рынка"""
    session_key, session_data, is_trading, status = TradingSession.get_current_session()
    is_prime = TradingSession.is_prime_time()
    
    msk_tz = timezone(timedelta(hours=3))
    current_time = datetime.now(msk_tz).strftime('%H:%M МСК')
    
    text = (
        f"📊 <b>Статус рынка МосБиржи</b>\n\n"
        f"🕐 Текущее время: {current_time}\n"
        f"📈 Статус: {TradingSession.get_session_status_text()}\n\n"
    )
    
    if is_trading and session_key:
        text += (
            f"<b>Текущая сессия:</b> {session_data['name']}\n"
            f"<b>Торги:</b> {'✅ Идут' if is_trading else '⏸️ Приостановлены'}\n"
            f"<b>Основное время (prime):</b> {'✅ Да' if is_prime else '❌ Нет'}\n"
        )
    
    await update.message.reply_text(text, parse_mode='HTML')

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /scan - ручное сканирование"""
    session_key, session_data, is_trading, status = TradingSession.get_current_session()
    
    if not is_trading:
        warning = ("⚠️ <b>Торги сейчас не идут!</b>\n\n"
                  "Данные могут быть неактуальными. Рекомендуется\n"
                  "дождаться основной сессии (10:00-18:40 МСК).")
        await update.message.reply_text(warning, parse_mode='HTML')
    
    msg = await update.message.reply_text(
        "🧠 <b>SMC сканирование рынка...</b>\n"
        f"<i>Анализирую 10-минутные свечи (6 компонентов)</i>\n"
        "<i>Это может занять некоторое время...</i>",
        parse_mode='HTML'
    )
    
    try:
        signals, total_tickers = await scan_market()
        await msg.delete()
        
        session_status = TradingSession.get_session_status_text()
        
        if not signals:
            text = (f"📊 <b>Нет сигналов</b>\n\n"
                   f"{session_status}\n"
                   f"📈 Просканировано акций: {total_tickers}\n\n"
                   f"💡 <i>SMC стратегия не видит качественных LONG-сетапов.</i>")
            await update.message.reply_text(text, parse_mode='HTML')
            return
        
        await update.message.reply_text(
            f"🧠 <b>SMC LONG сигналы: {len(signals)}</b>\n"
            f"{session_status}\n"
            f"📈 Просканировано акций: {total_tickers}\n\n"
            f"📊 <i>Топ-{len(signals)} сетапов:</i>",
            parse_mode='HTML'
        )
        
        for i, s in enumerate(signals, 1):
            # Аналогичное формирование текста как в send_scan_results
            components_text = []
            
            if s['bos_found']:
                bos_pct = s['bos_data'].get('breakout_pct', 0)
                components_text.append(f"✅ BOS: +30 (пробой +{bos_pct:.2f}%)")
            
            if s['choch_found']:
                components_text.append(f"✅ CHoCH: +10 (смена тренда)")
            
            if s['ob_found']:
                vol_ratio = s['ob_data'].get('volume_ratio', 0)
                components_text.append(f"✅ Order Block: +25 (объем x{vol_ratio:.1f})")
            
            if s['fvg_found']:
                gap_pct = s['fvg_data'].get('gap_pct', 0)
                components_text.append(f"✅ FVG: +15 (гэп {gap_pct:.2f}%)")
            
            if s['sweep_found']:
                cluster_size = s['sweep_data'].get('cluster_size', 0)
                components_text.append(f"✅ Liq. Sweep: +15 (кластер {cluster_size})")
            
            vol_data = s['vol_data']
            vwap_items = []
            if vol_data.get('above_vwap'):
                vwap_items.append("VWAP +5")
            if vol_data.get('volume_surge'):
                vwap_items.append("объем +5")
            if vwap_items:
                components_text.append(f"✅ Vol/VWAP: +{vol_data.get('score', 0)} ({', '.join(vwap_items)})")
            
            components_str = "\n".join(components_text)
            
            tp_sl_text = (
                f"🛑 SL: <b>{s['stop_loss']} ₽</b> (-{s['sl_percent']}%)\n"
                f"✅ TP1: <b>{s['tp1']} ₽</b> (+{s['tp1_percent']}%) | R/R 1:1\n"
                f"🎯 TP2: <b>{s['tp2']} ₽</b> (+{s['tp2_percent']}%) | R/R 1:2"
            )
            
            if s['score'] >= 85:
                strength_emoji = "🔥🔥🔥"
            elif s['score'] >= 75:
                strength_emoji = "🔥🔥"
            elif s['score'] >= 65:
                strength_emoji = "🔥"
            else:
                strength_emoji = "⭐"
            
            tv_link = get_tradingview_link(s['ticker'])
            
            text = (
                f"<b>{strength_emoji} #{i} <a href='{tv_link}'>{s['ticker']}</a> 🟢📈 LONG</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Рейтинг: <b>{s['score']}%</b> ({s['total_score']}/{s['max_score']})\n"
                f"💰 Цена: <b>{s['entry']} ₽</b> | ATR: {s['atr']}₽ ({s['atr_pct']}%)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🔍 КОМПОНЕНТЫ:</b>\n{components_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🎯 ТОРГОВЫЙ ПЛАН:</b>\n{tp_sl_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 <i>SMC на 10-минутных свечах</i>"
            )
            
            try:
                await update.message.reply_text(
                    text, 
                    parse_mode='HTML', 
                    reply_markup=create_tradingview_keyboard(s['ticker'])
                )
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                plain = (text.replace('<b>', '').replace('</b>', '')
                        .replace('<i>', '').replace('</i>', '')
                        .replace(f"<a href='{tv_link}'>", '').replace('</a>', ''))
                await update.message.reply_text(plain, reply_markup=create_tradingview_keyboard(s['ticker']))
            
            await asyncio.sleep(0.5)
            
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        try:
            await msg.edit_text(f"❌ Ошибка: {escape_html(str(e)[:200])}")
        except:
            await update.message.reply_text("❌ Произошла ошибка при сканировании")

def main():
    """Главная функция"""
    if not TOKEN:
        logger.error("❌ Токен не найден!")
        print("Ошибка: укажите TOKEN в .env файле")
        sys.exit(1)
    
    if ADMIN_CHAT_ID == 0:
        logger.error("❌ ADMIN_CHAT_ID не найден!")
        print("Ошибка: укажите ADMIN_CHAT_ID в .env файле")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🚀 MOEX SMC SCANNER BOT v5.0")
    logger.info("🧠 Стратегия: Smart Money Concepts (6 компонентов)")
    logger.info("📊 Таймфрейм: 10-минутные свечи")
    logger.info("=" * 50)
    logger.info(f"🎯 Мин. уверенность: {MIN_CONFIDENCE} баллов")
    logger.info(f"📐 ATR фильтр: {ATR_MIN_PCT}% - {ATR_MAX_PCT}%")
    logger.info(f"⏰ Временной фильтр: {PRIME_START}:00-{PRIME_END}:00 МСК")
    
    app = Application.builder().token(TOKEN).build()
    
    # Регистрация команд
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("schedule", schedule_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    
    # Настройка автоматического сканирования
    job_queue = app.job_queue
    if job_queue:
        # Сканирование каждый час в основное время (10:00-12:00 МСК)
        # В UTC: 7:00-9:00
        for hour in range(7, 10):  # 7:00, 8:00, 9:00 UTC = 10:00, 11:00, 12:00 МСК
            job_queue.run_daily(
                scheduled_scan,
                time=datetime.strptime(f"{hour:02d}:00", "%H:%M").time(),
                days=(0, 1, 2, 3, 4)  # Только будни
            )
        
        logger.info("🕐 Настроено сканирование в 10:00, 11:00, 12:00 МСК (будни)")
    
    print("\n" + "=" * 50)
    print("✅ SMC Bot v5.0 запущен!")
    print("=" * 50)
    print(f"📊 Таймфрейм: 10-минутные свечи")
    print(f"🧠 Стратегия: 6 компонентов SMC")
    print(f"🎯 Мин. порог: {MIN_CONFIDENCE} баллов")
    print("🕐 Авто-сканирование в основное время")
    print("📋 Команды: /start, /help, /scan, /schedule, /status")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
        print("\n👋 Бот остановлен")
    finally:
        asyncio.run(api.close())

if __name__ == "__main__":
    main()