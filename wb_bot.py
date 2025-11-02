#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WB Logistics Parser Bot — оптимизированная версия с группировкой по городам
"""
from __future__ import annotations
import aiohttp
import asyncio
import json
import html
import os
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from types import SimpleNamespace
from functools import lru_cache
from asyncio import Semaphore

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

# ---------------- CONFIG ----------------
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "7835216446:AAGj8c9aSrAoLhNgoU0ZJ_UJWRmuRg2Xfec")
    API_LAST_MILE = "https://logistics.wb.ru/reports-service/api/v1/last-mile"
    API_WAYS = "https://drive.wb.ru/client-gateway/api/waysheets/v1/waysheets"
    STATE_FILE = Path("state.json")
    
    # Интервалы обновления
    UPDATE_INTERVAL_PARKS = 60  # 1 минута
    UPDATE_INTERVAL_WAYS = 60   # 1 минута
    FORCE_REFRESH_EVERY = 300   # 5 минут
    
    # Ограничения для нагрузки
    MAX_CONCURRENT_API_REQUESTS = 5
    MAX_CONCURRENT_TELEGRAM_REQUESTS = 3
    HTTP_TIMEOUT = 15
    MAX_RETRIES = 3
    
    # Лимиты Telegram
    MAX_MESSAGES_PER_CHAT_PER_MINUTE = 10
    DELAY_BETWEEN_CHAT_REQUESTS = 3  # Уменьшено, т.к. сообщений стало меньше
    
    # Conversation states
    WAITING_TOKEN = 1

# Настройка логирования
def setup_logging():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)

# ---------------- GLOBALS ----------------
state: Dict[str, Any] = {
    "users": {},
    "parks_tracked": [],  # Теперь храним по городам, а не по парковкам
    "ways_tracked": [],
    "ways_suppliers": {},
    "ways_seen": {}
}

# Семафоры для ограничения нагрузки
api_semaphore = Semaphore(Config.MAX_CONCURRENT_API_REQUESTS)
telegram_semaphore = Semaphore(Config.MAX_CONCURRENT_TELEGRAM_REQUESTS)
ways_lock = asyncio.Lock()

# Трекер запросов для каждого чата
chat_request_tracker: Dict[int, List[float]] = {}

# ---------------- State load/save ----------------
def load_state():
    global state
    if Config.STATE_FILE.exists():
        try:
            with open(Config.STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error("State load error: %s", e)
            state = {"users": {}, "parks_tracked": [], "ways_tracked": [], "ways_suppliers": {}, "ways_seen": {}}


def save_state():
    try:
        with open(Config.STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("State save error: %s", e)

# ---------------- LOAD DISTRIBUTION ----------------
def can_send_to_chat(chat_id: int) -> bool:
    """Проверяет можно ли отправить запрос в чат без превышения лимитов"""
    now = datetime.now().timestamp()
    if chat_id not in chat_request_tracker:
        chat_request_tracker[chat_id] = []
    
    # Очищаем старые запросы (старше 1 минуты)
    chat_request_tracker[chat_id] = [
        req_time for req_time in chat_request_tracker[chat_id] 
        if now - req_time < 60
    ]
    
    # Проверяем лимит
    if len(chat_request_tracker[chat_id]) >= Config.MAX_MESSAGES_PER_CHAT_PER_MINUTE:
        return False
    
    return True

def mark_chat_request(chat_id: int):
    """Отмечает запрос к чату"""
    now = datetime.now().timestamp()
    if chat_id not in chat_request_tracker:
        chat_request_tracker[chat_id] = []
    chat_request_tracker[chat_id].append(now)

async def safe_edit_with_rate_limit(bot, chat_id: int, msg_id: int, text: str, city: str = None, uid: str = None):
    """Безопасное редактирование с проверкой лимитов и очисткой несуществующих сообщений"""
    if not can_send_to_chat(chat_id):
        logger.warning("Rate limit for chat %s, skipping update", chat_id)
        return
    
    try:
        async with telegram_semaphore:
            await asyncio.wait_for(
                bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML"), 
                timeout=5
            )
        mark_chat_request(chat_id)
        logger.debug("Successfully edited message for chat %s", chat_id)
        
        # Обновляем время последнего успешного обновления
        if city and uid:
            for t in state.get("parks_tracked", []):
                if t["chat_id"] == chat_id and t["msg_id"] == msg_id and t["uid"] == uid:
                    t["last_time"] = datetime.now().isoformat()
                    break
        
    except Exception as e:
        error_str = str(e)
        # Удаляем сообщение из трекинга при ЛЮБОЙ ошибке "not found"
        if any(phrase in error_str for phrase in ["Message to edit not found", "message not found", "Bad Request: message to edit not found"]):
            logger.info("Message not found, removing from tracking: chat %s, msg %s, city %s", chat_id, msg_id, city)
            # Удаляем из трекинга ВНЕ ЗАВИСИМОСТИ от наличия city и uid
            state["parks_tracked"] = [t for t in state.get("parks_tracked", []) 
                                    if not (t["chat_id"] == chat_id and t["msg_id"] == msg_id)]
            save_state()
        elif "Too Many Requests" in error_str:
            logger.warning("Rate limit hit for chat %s, will retry later", chat_id)
        else:
            logger.error("Edit error for chat %s: %s", chat_id, e)

# ---------------- VALIDATION ----------------
def validate_token(token: str) -> bool:
    """Проверка формата токена"""
    if not token or len(token) < 50:
        return False
    return bool(re.match(r'^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$', token))

# ---------------- API CLIENT ----------------
class APIClient:
    @staticmethod
    async def fetch_with_retry(url, headers=None, json_data=None, retries=Config.MAX_RETRIES):
        """Выполняет запрос с повторными попытками"""
        for attempt in range(retries):
            try:
                timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    if json_data:
                        async with session.post(url, headers=headers, json=json_data) as resp:
                            return await APIClient.handle_response(resp, attempt, retries)
                    else:
                        async with session.get(url, headers=headers) as resp:
                            return await APIClient.handle_response(resp, attempt, retries)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning("Attempt %d/%d failed for %s: %s", attempt + 1, retries, url, e)
                if attempt == retries - 1:
                    raise e
                await asyncio.sleep(1 << attempt)
        return None

    @staticmethod
    async def handle_response(resp, attempt, retries):
        if resp.status == 429:
            wait_time = 1 << attempt
            logger.warning("Rate limited, waiting %ds", wait_time)
            await asyncio.sleep(wait_time)
            return None
        elif resp.status == 401:
            return {"status": 401, "data": None}
        elif resp.status != 200:
            logger.warning("HTTP %d for attempt %d", resp.status, attempt + 1)
            return None
        
        try:
            data = await resp.json()
            return {"status": 200, "data": data}
        except Exception as e:
            logger.error("JSON parse error: %s", e)
            return None

    @staticmethod
    async def fetch_api_last_mile(token: str, user_id: Optional[str] = None, bot=None) -> Optional[List[Dict[str, Any]]]:
        headers = {"Authorization": f"Bearer {token.strip()}"}
        
        async with api_semaphore:
            result = await APIClient.fetch_with_retry(Config.API_LAST_MILE, headers)
            
            if not result:
                return None
                
            if result["status"] == 401:
                logger.info("Token invalid for user %s", user_id)
                if user_id and str(user_id) in state.get("users", {}):
                    del state["users"][str(user_id)]
                    save_state()
                    try:
                        if bot:
                            async with telegram_semaphore:
                                await bot.send_message(
                                    chat_id=int(user_id),
                                    text="⚠️ Ваш token недействителен. Пожалуйста, заново введите через /token <токен>"
                                )
                    except Exception as e:
                        logger.error("Error sending token invalid message: %s", e)
                return None
                
            data = result["data"]
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                return data["data"]
            return None

    @staticmethod
    async def fetch_ways(token: str, supplier_id: int) -> Optional[List[Dict[str, Any]]]:
        headers = {
            "Authorization": f"Bearer {token.strip()}", 
            "Content-Type": "application/json"
        }
        
        date_close_dt = datetime.utcnow() + timedelta(days=1)
        date_close = date_close_dt.replace(microsecond=0).isoformat(timespec="seconds") + ".000Z"
        date_open_dt = date_close_dt - timedelta(days=6)
        date_open = date_open_dt.replace(microsecond=0).isoformat(timespec="seconds") + ".000Z"

        payload = {
            "date_open": date_open,
            "date_close": date_close,
            "supplier_id": int(supplier_id),
            "limit": 999,
            "offset": 0,
            "way_type_id": 0
        }

        async with api_semaphore:
            result = await APIClient.fetch_with_retry(Config.API_WAYS, headers, payload)
            
            if not result or result["status"] != 200:
                return None
                
            data = result["data"]
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], dict) and "waysheets" in data["data"]:
                    return data["data"]["waysheets"]
                if "waysheets" in data and isinstance(data["waysheets"], list):
                    return data["waysheets"]
                for v in data.values():
                    if isinstance(v, dict) and "waysheets" in v:
                        return v["waysheets"]
            if isinstance(data, list):
                return data
            return None

# ----------------- HELPERS -----------------
def esc(s: Optional[Any]) -> str:
    if s is None:
        return "—"
    return html.escape(str(s))

def format_dt_pretty(dt_str: Optional[str]) -> str:
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y в %H:%M")
    except Exception:
        return esc(dt_str)

def _parse_dt_safe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def fmt_num(x: Optional[Any]) -> str:
    if x is None or x == "—":
        return "—"
    try:
        return f"{int(x):,}".replace(",", " ")
    except Exception:
        try:
            return str(int(float(x)))
        except Exception:
            return esc(x)

# ----------------- DATA PROCESSING -----------------
def extract_all_parks_from_office(office: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Извлекает ВСЕ парковки из офиса"""
    result = []
    office_name = office.get("office_name") or office.get("src_office_name") or "—"
    for route in office.get("routes", []):
        parkings = route.get("parking")
        if isinstance(parkings, list):
            pl = parkings
        else:
            pl = [parkings] if parkings is not None else [0]
        for parking in pl:
            try:
                parking_val = int(parking or 0)
            except Exception:
                parking_val = 0
            result.append({
                "office_name": office_name,
                "parking": parking_val,
                "route_car_id": route.get("route_car_id"),
                "count_tares": int(route.get("count_tares") or 0),
                "count_shk": int(route.get("count_shk") or 0),
                "distance": float(route.get("distance") or 0),
                "normative_liters": float(route.get("normative_liters") or route.get("normative_liters") or 0),
                "total_volume_ml": float(route.get("volume_ml_by_content") or route.get("total_volume_ml") or 0),
            })
    return result

def calc_shk_percent(count_shk: float, distance: float) -> int:
    norm = distance * 4 + 800
    return int(round(count_shk / norm * 100)) if norm > 0 else 0

def calc_liters_percent(total_volume_ml: float, normative_liters: float) -> int:
    if not normative_liters:
        return 0
    return int(round((total_volume_ml / 1000 / normative_liters) * 100))

def format_city_with_all_parks(parks: List[Dict[str, Any]]) -> str:
    """Форматирует ОДНО сообщение со ВСЕМИ парковками города"""
    if not parks:
        return "Нет данных по парковкам"
    
    # Группируем парковки по городу
    city = parks[0]["office_name"]
    
    # Сортируем парковки по номеру
    parks.sort(key=lambda x: x["parking"])
    
    lines = [f"<b>{esc(city)}</b>"]  # Убрал пустую строку

    for p in parks:
        shk = calc_shk_percent(p["count_shk"], p["distance"])
        lit = calc_liters_percent(p["total_volume_ml"], p["normative_liters"])
        lines.append(
            f"П {esc(p['parking'])} | ID: {esc(p['route_car_id'])} | 📦 {esc(p['count_tares'])} | ШК: {esc(p['count_shk'])}\n"
            f"{shk}% ШК | {lit}% Литры | От нормы"
        )

    lines.append(f"\n🕒 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
    return "\n\n".join(lines)

def format_way(way: Dict[str, Any], route: Optional[Dict[str, Any]] = None, parking_number: Any = "—") -> str:
    if not way:
        return "❌ Нет данных о путевом листе"

    def val(*keys, src=None):
        for k in keys:
            v = src.get(k) if src else None
            if v not in (None, "", [], {}, "—"):
                return v
        return None

    way_id = val("way_sheet_id", src=way) or "—"
    office = val("src_office_name", "office_name", src=way) or "—"
    driver = val("driver_name", "driver", src=way) or "—"
    open_dt = format_dt_pretty(val("open_dt", src=way))
    car = val("vehicle_number_plate", "vehicle_number", "car_number", src=way) or "—"

    loaded_boxes = val("count_box", "count_tares", src=way)
    loaded_shk = val("count_shk", src=way)
    
    # Используем переданный номер парковки или берем из route
    parking = parking_number if parking_number != "—" else (val("parking", src=route) or "—")
    rest_tares = val("count_tares", src=route)
    rest_shk = val("count_shk", src=route)

    msg = (
        "🚛 <b>Открыт новый путевой</b>\n"
        f"🏙️ Город: {esc(office)}\n"
        f"🅿️ Парковка: {esc(parking)}\n"  # Добавлен номер парковки
        f"🆔 Путевой: {esc(way_id)}\n"
        f"⏰ Время: {esc(open_dt)}\n"
        f"👤 Водитель: {esc(driver)}\n"
        f"🚗 ТС: {esc(car)}\n\n"
        f"<b>Загружено:</b>\n"
        f"📦 Коробки: {fmt_num(loaded_boxes)}\n"
        f"🏷️ ШК: {fmt_num(loaded_shk)}"
    )

    if route:
        msg += (
            f"\n\n<b>Остаток по парковке:</b>\n"
            f"📦 Коробки: {fmt_num(rest_tares)}\n"
            f"🏷️ ШК: {fmt_num(rest_shk)}"
        )

    return msg

# ----------------- COMMANDS -----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🤖 <b>WB Logistics Parser</b>\n\n"
        "🔑 <b>/token</b> — установить bearer_token\n"
        "🏙️ <b>/add город</b> — добавить город для парковок\n"
        "🗑️ <b>/dell город</b> — удалить город\n"
        "📋 <b>/all</b> — показать доступные города\n\n"
        "🚛 <b>/put город</b> — добавить город для путевых\n"
        "❌ <b>/dellput город</b> — удалить город из путевых\n"
        "ℹ️ <b>/help</b> — помощь"
    )
    try:
        async with telegram_semaphore:
            await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        logger.error("SEND ERROR start: %s", e)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with telegram_semaphore:
        await update.message.reply_text("Если нужна помощь — обращайтесь к администратору.")

# TOKEN handlers
async def cmd_token_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        token = " ".join(context.args).strip()
        return await save_user_token(update, token)
    async with telegram_semaphore:
        await update.message.reply_text("🔑 Отправьте свой bearer_token одним сообщением:")
    return Config.WAITING_TOKEN

async def receive_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    return await save_user_token(update, token)

async def save_user_token(update: Update, token: str):
    uid = str(update.effective_user.id)
    token = token.strip().replace("\n", "").replace("\r", "")
    
    if not validate_token(token):
        async with telegram_semaphore:
            await update.message.reply_text("❌ Похоже, это не bearer_token. Отмена.")
        return ConversationHandler.END
        
    state.setdefault("users", {})[uid] = token
    save_state()
    async with telegram_semaphore:
        await update.message.reply_text("✅ Токен сохранён и будет использоваться для запросов.")
    return ConversationHandler.END

async def cancel_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with telegram_semaphore:
        await update.message.reply_text("❌ Ввод токена отменён.")
    return ConversationHandler.END

# ======= PARKS commands =======
async def cmd_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    token = state.get("users", {}).get(uid)
    if not token:
        async with telegram_semaphore:
            await update.message.reply_text("Сначала установите token: /token <токен>")
        return
        
    data = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)
    if not data:
        async with telegram_semaphore:
            await update.message.reply_text("Ошибка запроса last-mile или недействительный token.")
        return
        
    cities = sorted({o.get("office_name") or "—" for o in data})
    async with telegram_semaphore:
        await update.message.reply_text("🏙️ <b>Доступные города:</b>\n" + "\n".join(f"• {esc(c)}" for c in cities), parse_mode="HTML")

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет город для отслеживания - ОДНО сообщение на город"""
    uid = str(update.effective_user.id)
    token = state.get("users", {}).get(uid)
    if not token:
        async with telegram_semaphore:
            await update.message.reply_text("Сначала установите token: /token <токен>")
        return
        
    if not context.args:
        async with telegram_semaphore:
            await update.message.reply_text("Использование: /add <город>")
        return
        
    city = " ".join(context.args).strip()
    data = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)
    if not data:
        async with telegram_semaphore:
            await update.message.reply_text("Ошибка запроса last-mile или недействительный token.")
        return
        
    office = next((o for o in data if (o.get("office_name") or "").lower() == city.lower()), None)
    if not office:
        async with telegram_semaphore:
            await update.message.reply_text("❌ Город не найден.")
        return
        
    # Извлекаем ВСЕ парковки города
    all_parks = extract_all_parks_from_office(office)
    if not all_parks:
        async with telegram_semaphore:
            await update.message.reply_text("❌ В этом городе нет парковок.")
        return
        
    # Форматируем ОДНО сообщение со всеми парковками
    text = format_city_with_all_parks(all_parks)
    chat_id = update.effective_chat.id
    
    try:
        async with telegram_semaphore:
            msg = await update.message.reply_text(text, parse_mode="HTML")
    except Exception:
        async with telegram_semaphore:
            msg = await update.message.reply_text(text)
            
    # Сохраняем ОДНУ запись на город
    state.setdefault("parks_tracked", []).append({
        "chat_id": chat_id,
        "msg_id": msg.message_id,
        "city": city,
        "uid": uid,
        "last_text": text,
        "last_time": datetime.now().isoformat()
    })
    save_state()
    
    async with telegram_semaphore:
        await update.message.reply_text(f"✅ Город {esc(city)} добавлен для парковок. Отслеживается {len(all_parks)} парковок.")

async def cmd_dell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        async with telegram_semaphore:
            await update.message.reply_text("Использование: /dell <город>")
        return
        
    city = " ".join(context.args).strip()
    before = len(state.get("parks_tracked", []))
    state["parks_tracked"] = [t for t in state.get("parks_tracked", []) if not (t["city"].lower() == city.lower() and t["uid"] == uid)]
    save_state()
    
    result = "🗑️ Удалено." if len(state.get("parks_tracked", [])) < before else "❌ Не найдено."
    async with telegram_semaphore:
        await update.message.reply_text(result)

# ======= WAYS commands =======
async def cmd_put(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    token = state.get("users", {}).get(uid)
    if not token:
        async with telegram_semaphore:
            await update.message.reply_text("Сначала установите token: /token <токен>")
        return
        
    if not context.args:
        async with telegram_semaphore:
            await update.message.reply_text("Использование: /put <город>")
        return
        
    city = " ".join(context.args).strip()

    supplier = state.get("ways_suppliers", {}).get(uid)
    if not supplier:
        data = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)
        supplier_found = None
        if data:
            for office in data:
                for r in office.get("routes", []):
                    s = r.get("supplier_id")
                    if s:
                        supplier_found = s
                        break
                    if isinstance(r.get("suppliers"), list):
                        for sup in r["suppliers"]:
                            if isinstance(sup, dict) and sup.get("supplier_id"):
                                supplier_found = sup["supplier_id"]
                                break
                        if supplier_found:
                            break
                if supplier_found:
                    break
        if not supplier_found:
            async with telegram_semaphore:
                await update.message.reply_text("❌ Не удалось определить supplier_id.")
            return
            
        state.setdefault("ways_suppliers", {})[uid] = int(supplier_found)
        save_state()
        supplier = int(supplier_found)

    exists = any(t for t in state.get("ways_tracked", []) if t["chat_id"] == update.effective_chat.id and t["city"].lower() == city.lower() and t["uid"] == uid)
    if not exists:
        state.setdefault("ways_tracked", []).append({
            "chat_id": update.effective_chat.id,
            "city": city,
            "uid": uid
        })
        save_state()
        
    async with telegram_semaphore:
        await update.message.reply_text(f"✅ Город {esc(city)} добавлен для путевых.")

    items = await APIClient.fetch_ways(token, supplier)
    if not items:
        async with telegram_semaphore:
            await update.message.reply_text("Нет доступных путевых листов.")
        return
        
    items = [i for i in items if isinstance(i, dict) and (i.get("src_office_name") or "").strip().lower() == city.lower()]
    if not items:
        async with telegram_semaphore:
            await update.message.reply_text(f"Нет путевых для города {city}.")
        return

    latest = max(items, key=lambda it: (_parse_dt_safe(it.get("close_dt")) or _parse_dt_safe(it.get("open_dt")) or datetime.min))

    lastmile = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)
    route_obj = None
    if lastmile:
        for office in lastmile:
            if (office.get("office_name") or "").strip().lower() == city.lower():
                for r in office.get("routes", []):
                    if str(r.get("route_car_id")) == str(latest.get("route_car_id")):
                        route_obj = r
                        break
                break

    parking_number = route_obj.get("parking", "—") if route_obj else "—"
    msg = format_way(latest, route_obj, parking_number)
    try:
        async with telegram_semaphore:
            await update.message.reply_text(msg, parse_mode="HTML")
    except Exception:
        async with telegram_semaphore:
            await update.message.reply_text(msg)

async def cmd_dellput(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        async with telegram_semaphore:
            await update.message.reply_text("Использование: /dellput <город>")
        return
        
    city = " ".join(context.args).strip()
    before = len(state.get("ways_tracked", []))
    state["ways_tracked"] = [t for t in state.get("ways_tracked", []) if not (t["city"].lower() == city.lower() and t["uid"] == str(update.effective_user.id))]
    save_state()
    
    result = "🗑️ Удалено." if len(state.get("ways_tracked", [])) < before else "❌ Не найдено."
    async with telegram_semaphore:
        await update.message.reply_text(result)

# ======= DELETE ALL command =======
async def cmd_dellall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет все города из чата (парковки и путевые)"""
    chat_id = update.effective_chat.id
    uid = str(update.effective_user.id)
    
    # Удаляем парковки
    parks_before = len(state.get("parks_tracked", []))
    state["parks_tracked"] = [t for t in state.get("parks_tracked", []) 
                             if not (t["chat_id"] == chat_id and t["uid"] == uid)]
    parks_removed = parks_before - len(state.get("parks_tracked", []))
    
    # Удаляем путевые
    ways_before = len(state.get("ways_tracked", []))
    state["ways_tracked"] = [t for t in state.get("ways_tracked", []) 
                            if not (t["chat_id"] == chat_id and t["uid"] == uid)]
    ways_removed = ways_before - len(state.get("ways_tracked", []))
    
    save_state()
    
    result_msg = (
        f"🗑️ <b>Удаление завершено:</b>\n"
        f"• Парковки: {parks_removed} городов\n"
        f"• Путевые: {ways_removed} городов\n\n"
        f"Теперь в этом чате не будет обновлений."
    )
    
    async with telegram_semaphore:
        await update.message.reply_text(result_msg, parse_mode="HTML")
    logger.info("User %s removed all tracking from chat %s: %d parks, %d ways", 
                uid, chat_id, parks_removed, ways_removed)

# ----------------- UPDATERS (jobs) -----------------
async def parks_updater(context: ContextTypes.DEFAULT_TYPE):
    """Обновление парковок - ОДНО сообщение на город"""
    try:
        logger.info("Tick - обновление парков")
        tracked = list(state.get("parks_tracked", []))
        if not tracked or not state.get("users"):
            return

        # Группируем по пользователям для пакетных запросов
        user_tasks = {}
        for uid in set(t["uid"] for t in tracked):
            token = state.get("users", {}).get(uid)
            if token:
                user_tasks[uid] = APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)

        # Выполняем все запросы параллельно
        user_data_results = await asyncio.gather(*user_tasks.values(), return_exceptions=True)
        user_data = dict(zip(user_tasks.keys(), user_data_results))

        updates = []
        for tr in tracked:
            uid = tr["uid"]
            user_data_result = user_data.get(uid)
            
            if isinstance(user_data_result, Exception):
                logger.error("Error fetching data for user %s: %s", uid, user_data_result)
                continue
                
            if not user_data_result:
                continue
                
            office = next((o for o in user_data_result if (o.get("office_name") or "").lower() == tr["city"].lower()), None)
            if not office:
                continue
                
            # Извлекаем ВСЕ парковки города
            all_parks = extract_all_parks_from_office(office)
            if not all_parks:
                continue
                
            # Форматируем ОДНО сообщение со всеми парковками
            new_text = format_city_with_all_parks(all_parks)
            last_text = tr.get("last_text", "")
            
            try:
                last_time = datetime.fromisoformat(tr.get("last_time")) if tr.get("last_time") else datetime.now()
            except Exception:
                last_time = datetime.now()
                
            seconds_since = (datetime.now() - last_time).total_seconds()
            # Обновляем только если данные изменились или прошло много времени
            if new_text != last_text or seconds_since >= Config.FORCE_REFRESH_EVERY:
                tr["last_text"] = new_text
                tr["last_time"] = datetime.now().isoformat()
                updates.append((tr["chat_id"], tr["msg_id"], new_text))

        if updates:
            logger.info("Distributing %d city updates across chats", len(updates))
            # Отправляем обновления с распределением нагрузки
            for chat_id, msg_id, text in updates:
                if can_send_to_chat(chat_id):
                    await safe_edit_with_rate_limit(context.bot, chat_id, msg_id, text)
                    await asyncio.sleep(Config.DELAY_BETWEEN_CHAT_REQUESTS)
                else:
                    logger.warning("Rate limit for chat %s, skipping update", chat_id)
            
        save_state()
        
    except Exception as e:
        logger.error("Parks updater error: %s", e)

async def ways_poller(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет новые путевые"""
    if ways_lock.locked():
        logger.info("Ways poller skipped - previous run still in progress")
        return
        
    async with ways_lock:
        try:
            logger.info("=== НАЧАЛО ПРОВЕРКИ ПУТЕВЫХ ===")
            tracked_list = list(state.get("ways_tracked", []))
            logger.info("Отслеживается городов: %d", len(tracked_list))
            
            if not tracked_list:
                logger.info("Нет отслеживаемых городов")
                return

            # Группируем по пользователям и чатам
            by_user_chat = {}
            for tr in tracked_list:
                key = (tr["uid"], tr["chat_id"])  # Уникальный ключ: пользователь + чат
                by_user_chat.setdefault(key, []).append(tr)

            # Параллельная обработка пользователей и чатов
            user_tasks = []
            for (uid, chat_id), tracked_for_chat in by_user_chat.items():
                user_tasks.append(process_user_chat_ways(uid, chat_id, tracked_for_chat, context))

            await asyncio.gather(*user_tasks, return_exceptions=True)
            
            logger.info("=== ЗАВЕРШЕНИЕ ПРОВЕРКИ ПУТЕВЫХ ===")
            
        except Exception as e:
            logger.error("Ways poller error: %s", e)

async def process_user_chat_ways(uid: str, chat_id: int, tracked_for_chat: List, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает путевые для конкретного пользователя и чата"""
    try:
        logger.debug("Processing ways for user %s, chat %s, %d cities", uid, chat_id, len(tracked_for_chat))
        
        token = state.get("users", {}).get(uid)
        supplier = state.get("ways_suppliers", {}).get(uid)
        if not token:
            logger.warning("No token for user %s", uid)
            return
        if not supplier:
            logger.warning("No supplier for user %s", uid)
            return

        # Проверяем валидность токена
        test_data = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)
        if not test_data:
            logger.warning("Token invalid for user %s in ways poller", uid)
            # Удаляем невалидный токен
            if uid in state.get("users", {}):
                del state["users"][uid]
                save_state()
            return

        items = await APIClient.fetch_ways(token, supplier)
        if not items:
            return
            
        items = [i for i in items if isinstance(i, dict)]
        if not items:
            return

        seen_list = state.setdefault("ways_seen", {}).setdefault(str(supplier), [])
        seen = set(seen_list)

        # Сортируем по времени открытия (новые первыми)
        items_sorted = sorted(items, key=lambda it: _parse_dt_safe(it.get("open_dt")) or datetime.min, reverse=True)

        # Находим действительно новые путевые
        new_items = []
        for it in items_sorted:
            way_id = str(it.get("way_sheet_id"))
            if way_id not in seen:
                new_items.append(it)
        
        logger.info("User %s: found %d new waysheets", uid, len(new_items))
        
        if not new_items:
            return

        lastmile = await APIClient.fetch_api_last_mile(token, user_id=uid, bot=context.bot)

        # Собираем все ID новых путевых которые были отправлены
        sent_way_ids = set()

        for it in new_items:
            src = (it.get("src_office_name") or "").strip().lower()
            way_id = str(it.get("way_sheet_id"))
            
            # Проверяем, отслеживается ли этот город в ДАННОМ чате
            city_tracked = any(
                (tr.get("city") or "").strip().lower() == src 
                for tr in tracked_for_chat
            )
            
            if not city_tracked:
                continue  # Пропускаем, если город не отслеживается в этом чате
                
            route_obj = None
            # Ищем номер парковки в данных lastmile
            parking_number = "—"
            for office in lastmile or []:
                if (office.get("office_name") or "").strip().lower() == src:
                    for r in office.get("routes", []):
                        if str(r.get("route_car_id")) == str(it.get("route_car_id")):
                            route_obj = r
                            parking_number = r.get("parking", "—")
                            break
                    break

            msg = format_way(it, route_obj, parking_number)

            # Проверяем лимиты перед отправкой
            if can_send_to_chat(chat_id):
                try:
                    async with telegram_semaphore:
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
                        mark_chat_request(chat_id)
                        logger.info("Новый путевой %s (%s) -> chat %s", way_id, src, chat_id)
                        sent_way_ids.add(way_id)
                except Exception as e:
                    logger.error("Error sending way message to %s: %s", chat_id, e)
                # Задержка между сообщениями
                await asyncio.sleep(Config.DELAY_BETWEEN_CHAT_REQUESTS)
            else:
                logger.warning("Rate limit for chat %s, skipping way notification", chat_id)

        # Обновляем seen только отправленными путевыми
        if sent_way_ids:
            seen.update(sent_way_ids)
            state.setdefault("ways_seen", {})[str(supplier)] = list(seen)
            save_state()
            logger.info("Added %d new ways to seen list for supplier %s", len(sent_way_ids), supplier)
            
    except Exception as e:
        logger.error("Error in process_user_chat_ways for user %s: %s", uid, e)

# ----------------- CLEANUP -----------------
async def cleanup_stale_parks_messages(context: ContextTypes.DEFAULT_TYPE):
    """Очистка сообщений парковок, которые долго не обновлялись успешно"""
    try:
        tracked = state.get("parks_tracked", [])
        if not tracked:
            return
            
        now = datetime.now()
        removed_count = 0
        
        for track in tracked[:]:  # Копируем для безопасного удаления
            try:
                last_time = datetime.fromisoformat(track.get("last_time", "2000-01-01"))
                # Если сообщение не обновлялось более 24 часов - удаляем
                if (now - last_time).total_seconds() > 86400:  # 24 часа
                    state["parks_tracked"].remove(track)
                    removed_count += 1
                    logger.info("Removed stale parks message: chat %s, msg %s, city %s", 
                               track["chat_id"], track["msg_id"], track.get("city"))
            except Exception as e:
                logger.error("Error processing parks track: %s", e)
                
        if removed_count > 0:
            save_state()
            logger.info("Cleaned up %d stale parks messages", removed_count)
            
    except Exception as e:
        logger.error("Parks cleanup error: %s", e)

# ----------------- STATE OPTIMIZATION -----------------
def cleanup_old_ways_seen():
    """Очищает старые путевые (оставляет только последние 1000 на поставщика)"""
    for supplier_id in list(state.get("ways_seen", {}).keys()):
        ways = state["ways_seen"][supplier_id]
        # Оставляем только последние 1000 записей на поставщика
        if len(ways) > 1000:
            state["ways_seen"][supplier_id] = ways[-1000:]
            logger.info("Trimmed ways_seen for supplier %s: %d -> 1000", supplier_id, len(ways))

def optimize_parks_tracked():
    """Удаляет дубликаты и оптимизирует структуру"""
    seen = set()
    optimized = []
    
    for track in state.get("parks_tracked", []):
        key = (track["chat_id"], track["msg_id"], track["uid"])
        if key not in seen:
            seen.add(key)
            # Удаляем лишние поля которые можно перегенерировать
            track.pop("last_text", None)
            optimized.append(track)
    
    if len(optimized) < len(state.get("parks_tracked", [])):
        logger.info("Optimized parks_tracked: %d -> %d entries", 
                   len(state.get("parks_tracked", [])), len(optimized))
    
    state["parks_tracked"] = optimized

def cleanup_unused_users():
    """Удаляет пользователей без активных отслеживаний"""
    active_uids = set()
    
    # Собираем активных пользователей
    for track in state.get("parks_tracked", []):
        active_uids.add(track["uid"])
    for track in state.get("ways_tracked", []):
        active_uids.add(track["uid"])
    
    # Удаляем неактивных
    removed_count = 0
    for uid in list(state.get("users", {}).keys()):
        if uid not in active_uids:
            del state["users"][uid]
            removed_count += 1
    
    if removed_count > 0:
        logger.info("Removed %d unused users", removed_count)

def log_state_size():
    """Логирует размер state.json"""
    if Config.STATE_FILE.exists():
        size_kb = Config.STATE_FILE.stat().st_size / 1024
        items_count = (
            len(state.get("users", {})) +
            len(state.get("parks_tracked", [])) +
            len(state.get("ways_tracked", [])) +
            sum(len(v) for v in state.get("ways_seen", {}).values())
        )
        logger.info("State file: %.1f KB, %d total items", size_kb, items_count)

def optimize_state():
    """Полная оптимизация state.json"""
    logger.info("Starting state optimization...")
    
    initial_size = Config.STATE_FILE.stat().st_size if Config.STATE_FILE.exists() else 0
    
    # 1. Очистка устаревших путевых
    cleanup_old_ways_seen()
    
    # 2. Оптимизация парковок
    optimize_parks_tracked()
    
    # 3. Очистка неиспользуемых пользователей
    cleanup_unused_users()
    
    # 4. Сохраняем оптимизированное состояние
    save_state()
    
    # 5. Логируем результат
    if Config.STATE_FILE.exists():
        final_size = Config.STATE_FILE.stat().st_size
        if initial_size > 0:
            reduction = (initial_size - final_size) / initial_size * 100
            logger.info("State optimization completed: %.1f%% size reduction", reduction)
        log_state_size()

# ----------------- BACKGROUND RUNNER (для режима без JobQueue) -----------------
async def _background_runner(app):
    """Фоновый раннер для режима без JobQueue"""
    while True:
        try:
            # Запускаем обновление парковок
            await parks_updater(app)
            await asyncio.sleep(Config.UPDATE_INTERVAL_PARKS)
        except Exception as e:
            logger.error("Background parks runner error: %s", e)
            await asyncio.sleep(60)

async def background_cleanup():
    """Фоновая очистка для режима без JobQueue"""
    while True:
        await asyncio.sleep(21600)  # 6 часов
        await cleanup_stale_parks_messages(None)
        optimize_state()

# ----------------- MAIN -----------------
def main():
    if not Config.BOT_TOKEN or Config.BOT_TOKEN == "7835216446:AAGj8c9aSrAoLhNgoU0ZJ_UJWRmuRg2Xfec":
        logger.warning("Using default token - please set BOT_TOKEN environment variable for production!")
    
    load_state()
    
    # Логируем начальный размер state
    log_state_size()
    
    app = Application.builder().token(Config.BOT_TOKEN).build()

    # Conversation for token
    conv = ConversationHandler(
        entry_points=[CommandHandler("token", cmd_token_start)],
        states={Config.WAITING_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_token)]},
        fallbacks=[CommandHandler("cancel", cancel_token)]
    )
    app.add_handler(conv)

    # basic commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("all", cmd_all))

    # parks commands
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("dell", cmd_dell))

    # ways commands
    app.add_handler(CommandHandler("put", cmd_put))
    app.add_handler(CommandHandler("dellput", cmd_dellput))
    app.add_handler(CommandHandler("dellall", cmd_dellall))

    # schedule jobs
    if app.job_queue:
        app.job_queue.run_repeating(parks_updater, interval=Config.UPDATE_INTERVAL_PARKS, first=5, name="parks_updater")
        app.job_queue.run_repeating(ways_poller, interval=Config.UPDATE_INTERVAL_WAYS, first=10, name="ways_poller")
        # Добавляем очистку ТОЛЬКО для парковок каждые 6 часов
        app.job_queue.run_repeating(cleanup_stale_parks_messages, interval=21600, first=60, name="cleanup_parks")
        # Добавляем оптимизацию state каждые 24 часа
        app.job_queue.run_repeating(
            lambda context: optimize_state(), 
            interval=86400,
            first=300,
            name="state_optimizer"
        )       
        logger.info("Using JobQueue for scheduling")
    else:
        logger.warning("JobQueue not available - using built-in background runner")
        async def start_bg_after_startup(_):
            asyncio.create_task(_background_runner(app))
            asyncio.create_task(background_cleanup())
            # Запускаем ways_poller в фоне
            async def background_ways_poller():
                while True:
                    try:
                        await ways_poller(app)
                        await asyncio.sleep(Config.UPDATE_INTERVAL_WAYS)
                    except Exception as e:
                        logger.error("Background ways poller error: %s", e)
                        await asyncio.sleep(60)
            asyncio.create_task(background_ways_poller())
        app.post_init = [start_bg_after_startup]

    logger.info("WB Bot запущен с группировкой парковок по городам")
    logger.info("Интервалы: парковки %dс, путевые %dс", Config.UPDATE_INTERVAL_PARKS, Config.UPDATE_INTERVAL_WAYS)
    logger.info("Лимиты: %d сообщений/мин на чат, задержка %dс", Config.MAX_MESSAGES_PER_CHAT_PER_MINUTE, Config.DELAY_BETWEEN_CHAT_REQUESTS)
    app.run_polling(allowed_updates=None)

if __name__ == "__main__":
    main()