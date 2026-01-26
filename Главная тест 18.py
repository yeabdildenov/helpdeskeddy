import sys
import os
import re
import time
import threading
from datetime import datetime
from html import unescape
import base64
import requests
import json
import customtkinter as ctk
from tkinter import messagebox, ttk
import sqlite3

try:
    from pynput import keyboard
except ImportError:
    messagebox.showerror("Ошибка", "Не установлена библиотека pynput.\nВыполните: pip install pynput")
    sys.exit()


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


try:
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# --- КОНСТАНТЫ И СЛОВАРИ ---
EMAIL = "a.marchenko@arbuz.kz"
API_KEY = "d7e1fce9-305b-43e4-a272-7d26e10c9c43"
BASE_URL = "https://arbuz.helpdeskeddy.com/api/v2"

credentials = f"{EMAIL}:{API_KEY}"
auth_header = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
HEADERS = {
    "Authorization": f"Basic {auth_header}",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json"
}

FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSdxcu0jMVe3knll8goTB1lOfrqXZUz2EKbmmVinihT7SCs44w/viewform"
WAIT_TIMEOUT = 10

MANAGER_DATA = {
    129692: ("Нуржамал Мукаева (Качество)", "Алматы 1"),
    27: ("Айдос Багланулы (ОКЗ Алматы 2)", "Алматы 2"),
    28: ("Владимир Толстоухов (ОКЗ Астана)", "Астана"),
    20: ("Бормисов Иван (логистика Алматы)", "Алматы 1"),
    21: ("Елена Сокол (логистика Астана)", "Астана"),
    19: ("Руслан Курмангалиев (СБ Алматы)", "Алматы 1"),
    104: ("Альфия Шохаева (СБ Астана)", "Астана"),
}

AUDIT_MANAGERS = {
    "Нуржамал Мукаева": ("Нуржамал Мукаева (Качество)", "Алматы 1"),
    "Жанара Жусупова": ("Жанара Жусупова (ОКЗ Алматы 1)", "Алматы 1"),
    "Айдос Багланулы": ("Айдос Багланулы (ОКЗ Алматы 2)", "Алматы 2"),
    "Владимир Толстоухов": ("Владимир Толстоухов (ОКЗ Астана)", "Астана"),
    "Иван Бормисов": ("Бормисов Иван (логистика Алматы)", "Алматы 1"),
    "Елена Сокол": ("Елена Сокол (логистика Астана)", "Астана"),
    "Руслан Курмангалиев": ("Руслан Курмангалиев (СБ Алматы)", "Алматы 1"),
    "Альфия Шохаева": ("Альфия Шохаева (СБ Астана)", "Астана"),
    "Руфины Мамедовы": ("Руфины Мамедовы (логистика Караганды)", "Караганда"),
    "Нуржана Борибекова": ("Нуржана Борибекова (СБ Караганда)", "Караганда"),
    "Ерке Жанкелова": ("Ерке Жанкелова (ОКЗ Астаны)", "Астана")
}

WAREHOUSE_MAP = {
    "Муратбаева 23/1": "Алматы 1",
    "Таусаралы 73": "Алматы 2",
    "101-ая, 43/1": "Астана",
}

TAG_MAPPING = {
    "Транспортировка": [
        "растаял", "потек", "протек", "мятый", "раздавл", "бой", "разби", "теплый", "каша",
        "температур", "доставк", "переверн", "всмятку", "при доставке", "помял", "давлени",
        "поврежденка", "жидкость", "лед", "морожен"
    ],
    "Недовоз": [
        "недовоз", "не довезли", "не положили", "отсутствует", "не было в пакете", "нет товара",
        "забыли положить", "не привезли", "минус", "позици", "не хватает"
    ],
    "Поврежденная упаковка": [
        "порван", "упаковк", "вскрыт", "дырк", "открыт", "нарушен", "герметичн"
    ],
    "Упаковка": [
        "упаковк"
    ],
    "Не тот товар": [
        "не тот товар", "другой товар", "перепутал", "не то", "чужой", "заказывал другой"
    ],
    "Лишний товар": [
        "лишний", "не заказывал", "подарок", "лишние"
    ],
    "Опоздание курьера": [
        "опозда", "задерж", "время", "долго ехал", "не успел", "интервал", "с опозданием"
    ],
    "Грубый курьер": [
        "груб", "хам", "неадекват", "кричал", "ругался", "вежливост"
    ],
    "Ошибка курьера": [
        "не поднял", "не позвонил", "ушел", "не нашел", "адрес", "домофон", "курьер"
    ],
    "Товарное соседство": [
        "соседство", "бытовая химия", "химия с едой", "в одном пакете"
    ],
    "Много пластика": [
        "пластик", "пакет", "экологи"
    ],
    "Перевес/недовес": [
        "перевес", "недовес", "вес", "грамм", "кг"
    ],
    "Нет в наличии": [
        "наличи", "закончился"
    ],
    "Пересорт": [
        "пересорт", "артикул"
    ],
    "Не учли комментарий": [
        "комментари", "просил", "указал"
    ],
    "Ошибка склада": [
        "сборк", "сборщик", "ошибка склада"
    ],
    "Не устроила замена": [
        "замена", "аналог", "не согласовали"
    ],
    "Описание по сайту": [
        "ошибка в описании", "фото на сайте", "картинка на сайте", "состав не тот", "описание на сайте"
    ],
    "Регламент логистики": [
        "регламент", "форма", "вид курьера"
    ]
}

RESOLVED_OPTIONS_MAPPING = {
    "Довоз": ["довоз", "достав", "доез", "повторная доставка", "довезли"],
    "Бонусы": ["бонус", "промокод", "скидка", "прмокодом", "возврат бонусом", "бонусами", "промо",
               "компенсация в виде промо"],
    "Возврат денег": ["возврат денег", "возврат средств", "рефанд", "refund", "вернули деньги",
                      "компенсация деньгами", "возврат", "оформлен возврат"],
    "Объяснили": ["объяснил", "проинформировал", "сообщил", "разъяснил"],
    "Замена": ["замен"],
}

AUTO_CLOSE_KEYWORDS = {"поставщику", "производителю", "нарушений нет"}
STOP_WORDS_AFTER_NAME = {"прошу", "удержать", "назначить", "считаю", "является", "предоставлены"}
INVALID_NAME_STARTS = {"камера", "видео", "фото", "нет", "прошу", "заказ", "данный", "тикет", "уволен", "свет",
                       "не было","Город","Астана","Караганда","установить","камерам","неполадкам","технический","Отдел","ГМЗ"}

RE_ORDER = re.compile(r"(?:заказ[^\d]{0,3}|№|#)\s*(\d{6,})", re.IGNORECASE)
RE_HOW_RESOLVED = re.compile(r"как\s+реш[её]н?\s+вопрос\??\s*[:\-]?\s*\(*\s*([^)]+)\)*", re.IGNORECASE)
RE_NUMBER = re.compile(r"\d+")
RE_TEMPLATE_TEXT = re.compile(
    r"Если\s+недовоз\s+подтвердится.*?Марченко\s+Аркадия",
    re.IGNORECASE | re.DOTALL
)

KAZ_CYRILLIC_UPPER = "А-ЯЁӘҒҚҢӨҰҮҺІ"
KAZ_CYRILLIC_LOWER = "а-яёәғқңөұүһі"
RE_KAZ_NAME_FLEX = fr"[{KAZ_CYRILLIC_UPPER}][{KAZ_CYRILLIC_LOWER}]+(?:\s+[{KAZ_CYRILLIC_UPPER}][{KAZ_CYRILLIC_LOWER}]+){{0,2}}"

ESSENTIAL_FIELDS = [
    "Руководитель", "Склад", "Номер заказа", "Тег жалобы", "Суть жалобы", "Как решен вопрос"
]
DEDUCTION_FIELDS = [
    "Ответственный сотрудник", "Пункт удержания", "Сумма удержания"
]

STANDARD_DECISIONS = {
    "Объяснительная",
    "Беседа",
    "Удержание",
    "Увольнение",
    "Жалоба поставщику",
    "Вина секции"
}

KEYWORDS_OTHER_NO_PROCESS = [
    "без фото", "не просматривается", "не видно", "аутсорс", "outsourc", "вопрос к секци",
    "камера не", "невозможно увидеть", "не вазможно увидеть", "нет видео записи", "нет видео",
    "прошу закрыть тикет без штрафа", "камера выдали", "камера вдали", "нет возможности",
    "вдали от", "секция фреш", "не было света", "отключили свет", "сбой камеры",
    "весовые товары фасуют",
    "стикеруют секция фреш", "уволен", "уволена", "будет взята объяснительная",
    "просмотреть сборку", "нет возможности просмотреть"
]

KEYWORDS_BESEDA = ["беседа", "проведена беседа", "проведем беседу"]


# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БД ---
def setup_database():
    conn = sqlite3.connect('statistics.db')
    cursor = conn.cursor()
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS tickets
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY
                       AUTOINCREMENT,
                       ticket_id
                       INTEGER
                       UNIQUE,
                       data_peredachi
                       TEXT,
                       rukovoditel
                       TEXT,
                       sklad
                       TEXT,
                       ssylka
                       TEXT,
                       data_ispolneniya
                       TEXT,
                       prosrochka
                       TEXT,
                       nomer_zakaza
                       TEXT,
                       tag_zhaloby
                       TEXT,
                       sut_zhaloby
                       TEXT,
                       otvetstvennyy_sotrudnik
                       TEXT,
                       reshenie_tiketa
                       TEXT,
                       punkt_uderzhaniya
                       TEXT,
                       summa_uderzhaniya
                       TEXT,
                       kak_reshen_vopros
                       TEXT,
                       summa_poter
                       TEXT,
                       processed_at
                       TIMESTAMP
                       DEFAULT
                       CURRENT_TIMESTAMP
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS skipped_tickets
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY
                       AUTOINCREMENT,
                       ticket_id
                       INTEGER,
                       reason
                       TEXT,
                       skipped_at
                       TIMESTAMP
                       DEFAULT
                       CURRENT_TIMESTAMP
                   )
                   ''')
    conn.commit()
    conn.close()


def log_skipped_ticket_to_db(ticket_id: int, reason: str):
    try:
        conn = sqlite3.connect('statistics.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO skipped_tickets (ticket_id, reason) VALUES (?, ?)", (ticket_id, reason))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        if 'app' in globals() and isinstance(app, App):
            app.log(f"❌ Ошибка сохранения пропуска для тикета ID={ticket_id} в БД: {e}", "error")


def save_ticket_data_to_db(fields: dict, ticket_id: int):
    try:
        conn = sqlite3.connect('statistics.db')
        cursor = conn.cursor()
        cursor.execute('''
                       INSERT INTO tickets (ticket_id, data_peredachi, rukovoditel, sklad, ssylka, data_ispolneniya,
                                            prosrochka, nomer_zakaza, tag_zhaloby, sut_zhaloby, otvetstvennyy_sotrudnik,
                                            reshenie_tiketa, punkt_uderzhaniya, summa_uderzhaniya, kak_reshen_vopros,
                                            summa_poter)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(ticket_id) DO NOTHING
                       ''', (
                           ticket_id,
                           fields.get("Дата передачи", ""), fields.get("Руководитель", ""),
                           fields.get("Склад", ""), fields.get("Ссылка", ""),
                           fields.get("Дата исполнения", ""), fields.get("Просрочка", ""),
                           fields.get("Номер заказа", ""), fields.get("Тег жалобы", ""),
                           fields.get("Суть жалобы", ""), fields.get("Ответственный сотрудник", ""),
                           fields.get("Решение тикета", ""), fields.get("Пункт удержания", ""),
                           fields.get("Сумма удержания", ""), fields.get("Как решен вопрос", ""),
                           fields.get("Сумма потерь", "")
                       ))
        conn.commit()
        conn.close()
        if 'app' in globals() and isinstance(app, App):
            app.log(f"📊 Статистика по тикету ID={ticket_id} сохранена в БД.", "info")
    except sqlite3.Error as e:
        if 'app' in globals() and isinstance(app, App):
            app.log(f"❌ Ошибка сохранения в БД для тикета ID={ticket_id}: {e}", "error")


def calculate_sum_from_string(s: str) -> str:
    if not isinstance(s, str) or '+' not in s:
        return s
    if not re.match(r"^[ \d+]+$", s):
        return s
    try:
        return str(eval(s))
    except Exception:
        return s


def parse_manager_comment_for_deduction(text: str) -> tuple[str, str, str]:
    employee, point, amount = "", "", ""
    employee_pattern_keyword = re.compile(
        r"(?:сотрудник\w*|оператор\w*|курьер\w*|винов\w*)\s+(" + RE_KAZ_NAME_FLEX + r")", re.IGNORECASE)
    match = employee_pattern_keyword.search(text)
    if not match:
        employee_pattern_fallback = re.compile(r"^(" + RE_KAZ_NAME_FLEX + r")", re.IGNORECASE)
        match = employee_pattern_fallback.search(text)
    if match:
        full_name_match = match.group(1).strip()
        words = full_name_match.split()
        first_word = words[0].lower()
        if first_word not in INVALID_NAME_STARTS:
            if words and words[-1].lower() in STOP_WORDS_AFTER_NAME:
                employee = " ".join(words[:-1])
            else:
                employee = full_name_match

    point_pattern = re.compile(r"(?:пункт\w*|п\.?)\s*(\d+(?:[.,]\d+)*)", re.IGNORECASE)
    match = point_pattern.search(text)
    if match:
        point = match.group(1).replace(",", ".").strip()

    amount_pattern_main = re.compile(
        r"(?:удержать|удержание|штраф)\s(?:с\s.*?)?\s*([\d\s+]{2,})\s*(?:тг|тенге)?", re.IGNORECASE)
    match = amount_pattern_main.search(text)
    if match:
        amount = match.group(1).strip().replace(" ", "")

    if not amount:
        amount_pattern_fallback = re.compile(r"сумма\s*([\d\s+]{2,})\s*(?:тг|тенге)?", re.IGNORECASE)
        match = amount_pattern_fallback.search(text)
        if match:
            amount = match.group(1).strip().replace(" ", "")

    return employee, point, amount


def strip_html(html_text: str) -> str:
    if not html_text: return ""
    text = re.sub(r"<[^>]+>", " ", html_text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_api_datetime(s: str) -> datetime | None:
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S %d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            continue
    return None


def find_last_manager_from_audit(audit_payload: dict) -> tuple:
    audit_data = audit_payload.get("data", {})
    if not audit_data: return None, None, None
    sorted_events = sorted(audit_data.values(), key=lambda x: parse_api_datetime(
        x.get("date_created", "1970-01-01 00:00:00")) or datetime.min, reverse=True)
    for event in sorted_events:
        user_name = event.get("user_name")
        event_type = event.get("event")
        if user_name in AUDIT_MANAGERS and event_type != "ticket_view":
            manager_info = AUDIT_MANAGERS[user_name]
            event_date = parse_api_datetime(event.get("date_created", ""))
            return user_name, manager_info[1], event_date
    return None, None, None


def find_manager_decision_comment(comments: list) -> str:
    for comment in reversed(comments):
        user_id = comment.get('user_id')
        if user_id is None: continue
        try:
            if int(user_id) in MANAGER_DATA:
                return strip_html(comment.get("text", ""))
        except (ValueError, TypeError):
            continue
    return ""


def guess_how_resolved(text: str) -> str:
    match = RE_HOW_RESOLVED.search(text)
    if match:
        raw_text = match.group(1).lower()
        for official_option, keywords in RESOLVED_OPTIONS_MAPPING.items():
            if any(keyword in raw_text for keyword in keywords):
                return official_option
    lower_text = text.lower()
    for official_option, keywords in RESOLVED_OPTIONS_MAPPING.items():
        if any(keyword in lower_text for keyword in keywords):
            return official_option
    return ""


def calculate_total_loss(text: str) -> str:
    match = re.search(r"Сумма\s*[:\s]*(.*)", text, re.IGNORECASE)
    if not match: return ""
    line_with_sums = match.group(1)
    line_clean = line_with_sums.replace(" ", "").replace("\xa0", "")
    numbers_found = re.findall(r'\d+', line_clean)
    if not numbers_found: return ""
    total_loss = sum(int(num) for num in numbers_found)
    return str(total_loss) if total_loss > 0 else ""


def pick_order_number(text: str) -> str:
    m = RE_ORDER.search(text)
    return m.group(1) if m else ""


def get_warehouse_from_custom_fields(ticket: dict) -> str:
    for cf in ticket.get("custom_fields", []):
        if cf.get("id") == 9 and cf.get("field_type") == "select":
            field_value = cf.get("field_value") or {};
            name_dict = field_value.get("name") or {}
            address = name_dict.get("ru")
            if address and address in WAREHOUSE_MAP:
                return WAREHOUSE_MAP[address]
    return ""


def extract_complaint_text(cleaned_full_text: str, comments_plain: list[str]) -> str:
    flags = re.IGNORECASE | re.DOTALL
    stop_phrase = r"(Вопрос\s+с\s+клиентом\s+реш[её]н\?|Сумма:|Если\s+недовоз\s+подтвердится)"
    pattern1 = rf"Полное\s+описание\s+жалобы\s*:\s*(.+?)\s*{stop_phrase}"
    m1 = re.search(pattern1, cleaned_full_text, flags)
    if m1: return re.sub(r"\s+", " ", m1.group(1)).strip()
    pattern2 = rf"жалоба\s+на\s*(.+?)\s*(фото\s+прилагается|{stop_phrase})"
    m2 = re.search(pattern2, cleaned_full_text, flags)
    if m2: return re.sub(r"\s+", " ", m2.group(1)).strip()
    pattern3 = rf"Заказ\s+№\s*\d+.*?,\s*(.+?)\s*{stop_phrase}"
    m3 = re.search(pattern3, cleaned_full_text, flags)
    if m3: return re.sub(r"\s+", " ", m3.group(1)).strip()
    for text in comments_plain:
        cleaned_comment = RE_TEMPLATE_TEXT.sub("", text)
        if any(keyword in cleaned_comment.lower() for keyword in
               ["заказ №", "жалоба", "курьер", "недовоз", "качество"]):
            if len(cleaned_comment.strip()) > 20 and "решён?" not in cleaned_comment.lower():
                return cleaned_comment.strip()[:500]
    return ""


def determine_complaint_tag(ticket: dict, full_text: str) -> str:
    text_lower = full_text.lower()

    # 1. Сначала ищем по ключевым словам в тексте
    for tag_name, keywords in TAG_MAPPING.items():
        for kw in keywords:
            if kw in text_lower:
                return tag_name

    # 2. Если явных ключей нет, смотрим на контекст шаблонов
    if "не довезли позицию" in text_lower:
        return "Недовоз"

    if "курьер" in text_lower and ("доставка" in text_lower or "привез" in text_lower):
        return "Транспортировка"

    if "жалоба на" in text_lower:
        return "Транспортировка"

    # 3. Проверка по всем кастомным полям (fallback)
    for cf in ticket.get("custom_fields", []):
        if cf.get("field_type") == "select":
            val_obj = cf.get("field_value")
            if isinstance(val_obj, dict):
                val_name = (val_obj.get("name") or {}).get("ru", "").lower()
                if val_name:
                    for tag_name, keywords in TAG_MAPPING.items():
                        if val_name == tag_name.lower():
                            return tag_name
                        for kw in keywords:
                            if kw in val_name:
                                return tag_name
    return ""


def extract_deduction_custom_fields(ticket: dict) -> tuple[str, str, str]:
    responsible_employee, penalty_point, deduction_amount = "", "", ""
    custom_fields = ticket.get("custom_fields", [])
    if not custom_fields: return "", "", ""
    for cf in custom_fields:
        field_id, field_value = cf.get("id"), cf.get("field_value", "")
        if field_id == 22 and field_value:
            responsible_employee = str(field_value)
        elif field_id == 26 and field_value:
            penalty_point = str(field_value)
        elif field_id == 27 and field_value:
            deduction_amount = str(field_value)
    return responsible_employee, penalty_point, deduction_amount


def api_get_tickets(owner_id: int = 1, status_list: str = "v-processe", page: int = 1) -> dict:
    url, params = f"{BASE_URL}/tickets", {"status_list": status_list, "owner_list": str(owner_id), "page": page,
                                          "limit": 50}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status();
    return r.json()


def api_get_comments(ticket_id: int) -> dict:
    url, params = f"{BASE_URL}/tickets/{ticket_id}/comments", {"page": 1}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status();
    return r.json()


def api_get_ticket_audit(ticket_id: int) -> dict:
    url = f"{BASE_URL}/tickets/{ticket_id}/audit"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status();
    return r.json()


def api_update_ticket_status(ticket_id: int, status: str) -> dict:
    url, payload = f"{BASE_URL}/tickets/{ticket_id}/", {"status_id": status}
    r = requests.put(url, json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status();
    return r.json()


def build_fields_for_ticket(ticket: dict, comments_payload: dict, audit_payload: dict) -> dict:
    ticket_id = ticket.get("id")
    last_manager_name, manager_warehouse, manager_comment_date = find_last_manager_from_audit(audit_payload)
    rucl_form_name = (AUDIT_MANAGERS.get(last_manager_name) or [""])[0]

    comments_data = comments_payload.get("data", [])
    all_comments = list(comments_data.values()) if isinstance(comments_data, dict) else (
        comments_data if isinstance(comments_data, list) else [])
    comments = [c for c in all_comments if c.get('user_id') != 1]
    comments.sort(key=lambda c: parse_api_datetime(c.get("date_created", "")) or datetime.min)
    comments_plain = [strip_html(c.get("text", "") or "") for c in comments]
    full_text = "\n".join(comments_plain)
    cleaned_full_text = RE_TEMPLATE_TEXT.sub("", full_text)

    sklad = get_warehouse_from_custom_fields(ticket) or manager_warehouse
    dt_upd = parse_api_datetime(ticket.get("date_updated", ""))
    date_isp = (manager_comment_date.strftime("%Y-%m-%d") if manager_comment_date
                else (dt_upd.strftime("%Y-%m-%d") if dt_upd else ""))
    dt_created = parse_api_datetime(ticket.get("date_created", "")) or datetime.now()
    date_peredachi = dt_created.strftime("%Y-%m-%d")
    link = f"https://arbuz.helpdeskeddy.com/ru/ticket/list/filter/id/34/ticket/{ticket_id}"
    prosr = "Нет"
    try:
        if date_peredachi and date_isp:
            prosr = "Да" if (datetime.strptime(date_isp, "%Y-%m-%d") - datetime.strptime(date_peredachi,
                                                                                         "%Y-%m-%d")).days > 3 else "Нет"
    except Exception:
        pass

    status_t = "Тикет закрыт согласно процессу"
    order_num = pick_order_number(cleaned_full_text)

    sut = extract_complaint_text(cleaned_full_text, comments_plain)
    tag = determine_complaint_tag(ticket, cleaned_full_text)
    how_resolved = guess_how_resolved(cleaned_full_text)

    otv_sotr, punkt, sum_ud = extract_deduction_custom_fields(ticket)
    manager_comment_text = find_manager_decision_comment(comments)

    parsed_sotr, parsed_punkt, parsed_sum = "", "", ""
    if manager_comment_text:
        parsed_sotr, parsed_punkt, parsed_sum = parse_manager_comment_for_deduction(manager_comment_text)

    if not otv_sotr: otv_sotr = parsed_sotr
    if not punkt: punkt = parsed_punkt
    if not sum_ud: sum_ud = parsed_sum

    sum_ud = calculate_sum_from_string(sum_ud)

    if otv_sotr and sum_ud and not punkt:
        punkt = "3.16"

    res_tiketa = ""
    manager_comment_lower = manager_comment_text.lower()

    if any(kw in manager_comment_lower for kw in KEYWORDS_OTHER_NO_PROCESS):
        res_tiketa = manager_comment_text
        status_t = "Нет процесса по закрытию тикета"
        otv_sotr = ""
        punkt = ""
        sum_ud = ""
    elif all([otv_sotr, punkt, sum_ud]):
        res_tiketa = "Удержание"
    elif any(kw in manager_comment_lower for kw in KEYWORDS_BESEDA):
        res_tiketa = "Беседа"
        status_t = "Тикет закрыт согласно процессу"
        punkt = ""
        sum_ud = ""
    elif any([otv_sotr, punkt, sum_ud]):
        res_tiketa = "Удержание"

    if not res_tiketa:
        if how_resolved:
            res_tiketa = f"Решено: {how_resolved}"
        elif manager_comment_text:
            res_tiketa = manager_comment_text

    sum_poter = calculate_total_loss(cleaned_full_text)

    return {"Дата передачи": date_peredachi, "Руководитель": rucl_form_name or "", "Склад": sklad or "", "Ссылка": link,
            "Дата исполнения": date_isp or "", "Просрочка": prosr, "Статус тикета": status_t,
            "Номер заказа": order_num, "Тег жалобы": tag, "Суть жалобы": sut,
            "Ответственный сотрудник": otv_sotr, "Решение тикета": res_tiketa, "Пункт удержания": punkt,
            "Сумма удержания": sum_ud, "Как решен вопрос": how_resolved, "Сумма потерь": sum_poter}


driver = None


# селениум
def start_driver():
    global driver
    if driver is not None:
        try:
            _ = driver.window_handles
        except Exception:
            driver = None
    if driver is None:
        options = Options()
        profile_path = os.path.join(os.path.expanduser("~"), "SeleniumChromeProfiles", "HDE_Filler_Profile")
        options.add_argument(f"user-data-dir={profile_path}")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        if sys.platform == "win32":
            driver_filename = "chromedriver.exe"
        else:
            driver_filename = "chromedriver"

        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))

        chrome_driver_path = os.path.join(application_path, driver_filename)

        if not os.path.exists(chrome_driver_path):
            error_message = (f"Ошибка: {driver_filename} не найден!\n\n"
                             f"Ожидаемый путь:\n{chrome_driver_path}\n\n"
                             f"Пожалуйста, поместите {driver_filename} в ту же папку, что и программу.")
            messagebox.showerror("Файл не найден", error_message)
            sys.exit(error_message)

        service = Service(executable_path=chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    return driver


def _wait():
    time.sleep(0.2)


def _js_set_value_and_dispatch(el, value):
    try:
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            el, value)
    except Exception:
        try:
            el.clear();
            el.send_keys(value)
        except Exception:
            pass


def _fill_date_field_by_position(position: int, ymd: str):
    if not ymd: return
    try:
        xpath = "//div[@role='listitem' and (.//input[@type='date'] or .//input[@aria-label='Год' or @aria-label='Year'])]"
        WebDriverWait(driver, WAIT_TIMEOUT).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        containers = driver.find_elements(By.XPATH, xpath)
        if len(containers) <= position: return
        container = containers[position]
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", container);
        _wait()
        try:
            date_input = container.find_element(By.CSS_SELECTOR, "input[type='date']");
            _js_set_value_and_dispatch(date_input, ymd);
            return
        except Exception:
            pass
        try:
            y, m, d = ymd.split('-');
            day = container.find_element(By.XPATH, ".//input[@aria-label='День' or @aria-label='Day']")
            month = container.find_element(By.XPATH, ".//input[@aria-label='Месяц' or @aria-label='Month']")
            year = container.find_element(By.XPATH, ".//input[@aria-label='Год' or @aria-label='Year']")
            _js_set_value_and_dispatch(day, d);
            _js_set_value_and_dispatch(month, m);
            _js_set_value_and_dispatch(year, y);
            return
        except Exception:
            pass
        try:
            text_input = container.find_element(By.CSS_SELECTOR, "input[type='text']");
            y, m, d = ymd.split('-');
            _js_set_value_and_dispatch(text_input, f"{d}.{m}.{y}");
            return
        except Exception:
            pass
    except Exception as e:
        print(f"!!! Ошибка при поиске/заполнении поля даты #{position + 1}: {e}")


def _fill_field_by_label(label_text: str, value: str):
    if not value: return
    try:
        xpath = f"//div[@role='listitem' and .//*[contains(text(), '{label_text}')]]"
        container = WebDriverWait(driver, WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, xpath)))
        field = container.find_element(By.CSS_SELECTOR, "input[type='text'], textarea")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", container);
        _wait()
        field.clear();
        field.send_keys(value)
    except Exception as e:
        print(f"!!! ОШИБКА: Не удалось заполнить поле '{label_text}'. Ошибка: {e}")


def _fill_special_text_field(label_text: str, value: str):
    if not value: return
    try:
        xpath = f"//div[@jsname='WsjYwc' and .//*[contains(text(), '{label_text}')]]"
        container = WebDriverWait(driver, WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, xpath)))
        field = container.find_element(By.XPATH, ".//input[@type='text']")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", container);
        _wait()
        field.clear();
        field.send_keys(value)
    except Exception as e:
        print(f"!!! ОШИБКА: Не удалось заполнить специальное поле '{label_text}'. Ошибка: {e}")


def _click_option_by_label_and_text(label_text: str, option_text: str):
    if not option_text: return
    try:
        container_xpath = f"//div[(@role='listitem' or @jsname='WsjYwc') and .//*[contains(text(), '{label_text}')]]"
        container = WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, container_xpath)))
        option_xpath = f".//div[@role='radio' or @role='checkbox' or contains(@class, 'exportOption')][.//*[normalize-space(text())='{option_text}']] | .//span[normalize-space(text())='{option_text}']"
        option_element = WebDriverWait(container, 5).until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
        driver.execute_script("arguments[0].click();", option_element)
    except Exception as e:
        log_message = f"!!! ОШИБКА: Не удалось кликнуть опцию '{option_text}' для вопроса '{label_text}'. Ошибка: {e}"
        print(log_message)
        if 'app' in globals() and isinstance(app, ctk.CTk): app.log(log_message, 'error')


def _fill_decision_other(text_value: str):
    if not text_value: return
    try:
        xpath_container = "//div[@role='listitem' and .//*[contains(text(), 'Решение тикета')]]"
        container = WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, xpath_container))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", container)
        _wait()
        xpath_input = ".//div[.//span[contains(text(), 'Другое')]]//input[@type='text']"
        input_element = container.find_element(By.XPATH, xpath_input)
        driver.execute_script("arguments[0].click();", input_element)
        _wait()
        input_element.clear()
        input_element.send_keys(text_value)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            input_element
        )
    except Exception as e:
        err = f"!!! ОШИБКА при заполнении поля 'Другое' в Решении тикета: {e}"
        print(err)
        if 'app' in globals() and isinstance(app, ctk.CTk): app.log(err, 'error')


def fill_form_fields(fields: dict):
    _fill_date_field_by_position(0, fields.get("Дата передачи", ""));
    _fill_date_field_by_position(1, fields.get("Дата исполнения", ""))
    _fill_field_by_label("Ссылка", fields.get("Ссылка", ""));
    _fill_field_by_label("Номер заказа", fields.get("Номер заказа", ""))
    _fill_field_by_label("Суть жалобы", fields.get("Суть жалобы", ""));
    _click_option_by_label_and_text("Ответственный руководитель", fields.get("Руководитель", ""))
    _click_option_by_label_and_text("Склад", fields.get("Склад", ""));
    _click_option_by_label_and_text("Просрочка", fields.get("Просрочка", ""))
    _click_option_by_label_and_text("Статус тикета", fields.get("Статус тикета", ""));
    _click_option_by_label_and_text("Тег жалобы", fields.get("Тег жалобы", ""))
    _fill_special_text_field("Ответственный сотрудник за жалобу", fields.get("Ответственный сотрудник", ""));

    decision = fields.get("Решение тикета", "")
    if decision in STANDARD_DECISIONS:
        _click_option_by_label_and_text("Решение тикета", decision)
    else:
        _fill_decision_other(decision)

    time.sleep(0.3)
    _fill_field_by_label("Пункт удержания", fields.get("Пункт удержания", ""));
    _fill_special_text_field("Сумма удержания", fields.get("Сумма удержания", ""))
    _click_option_by_label_and_text("Как был решен вопрос", fields.get("Как решен вопрос", ""));
    _fill_field_by_label("Сумма потерь", fields.get("Сумма потерь", ""))


# --- ОСНОВНОЙ КЛАСС ПРИЛОЖЕНИЯ ---
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("HDE → Google Form Filler");
        self.geometry("1200x720")
        ctk.set_appearance_mode("dark");
        ctk.set_default_color_theme("dark-blue")

        setup_database()

        self.grid_columnconfigure(1, weight=1);
        self.grid_rowconfigure(0, weight=1)
        self.sidebar_frame = ctk.CTkFrame(self, width=280, corner_radius=0);
        self.sidebar_frame.grid(row=0, column=0, sticky="nsew");
        self.sidebar_frame.grid_rowconfigure(4, weight=1)
        self.logo_label = ctk.CTkLabel(self.sidebar_frame, text="HDE Filler", font=ctk.CTkFont(size=20, weight="bold"));
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))

        load_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent");
        load_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(load_frame, text="📥 Загрузка тикетов", font=ctk.CTkFont(size=16, weight="bold"), anchor="w").pack(
            pady=(0, 10), fill="x")
        self.btn_load = ctk.CTkButton(load_frame, text="Загрузить 'В процессе'", command=self.load_tickets_open);
        self.btn_load.pack(fill="x", pady=5)
        self.btn_load_1000 = ctk.CTkButton(load_frame, text="Загрузить 1000 тикетов", command=self.load_tickets_bulk);
        self.btn_load_1000.pack(fill="x", pady=5)

        manual_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent");
        manual_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(manual_frame, text="🕹️ Управление и отчеты", font=ctk.CTkFont(size=16, weight="bold"),
                     anchor="w").pack(pady=(0, 10), fill="x")
        self.btn_stats = ctk.CTkButton(manual_frame, text="📊 Показать статистику", command=self.show_statistics_window);
        self.btn_stats.pack(fill="x", pady=5)
        self.btn_list = ctk.CTkButton(manual_frame, text="📋 Показать список", command=self.show_full_list_window)
        self.btn_list.pack(fill="x", pady=5)
        self.btn_fill = ctk.CTkButton(manual_frame, text="Заполнить следующий (Cmd+Opt+N)", state="disabled",
                                      command=self.fill_next_manual);
        self.btn_fill.pack(fill="x", pady=5)
        self.btn_submit_and_close = ctk.CTkButton(manual_frame, text="Отправить и Закрыть (Cmd+Opt+S)",
                                                  state="disabled",
                                                  command=self.submit_and_close_manual);
        self.btn_submit_and_close.pack(fill="x", pady=5)
        self.btn_skip = ctk.CTkButton(manual_frame, text="Пропустить текущий", state="disabled",
                                      command=self.skip_current);
        self.btn_skip.pack(fill="x", pady=5)

        tickonator_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent");
        tickonator_frame.grid(row=3, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(tickonator_frame, text="🤖 Автоматизация", font=ctk.CTkFont(size=16, weight="bold"),
                     anchor="w").pack(pady=(0, 10), fill="x")
        self.btn_tickonator_virgin = ctk.CTkButton(tickonator_frame, text="💎 Тикетонатор (Девственница)",
                                                   state="disabled", command=self.start_tickonator_virgin,
                                                   fg_color="#00BCD4", hover_color="#0097A7", height=35,
                                                   font=ctk.CTkFont(size=13, weight="bold"));
        self.btn_tickonator_virgin.pack(fill="x", pady=5)
        self.btn_tickonator_forgiving = ctk.CTkButton(tickonator_frame, text="🔥 Тикетонатор Давалка", state="disabled",
                                                      command=self.start_tickonator_forgiving, fg_color="#E91E63",
                                                      hover_color="#C2185B", height=35,
                                                      font=ctk.CTkFont(size=13, weight="bold"));
        self.btn_tickonator_forgiving.pack(fill="x", pady=5)
        self.btn_tickonator = ctk.CTkButton(tickonator_frame, text="⚡ Тикетонатор (Строгий)", state="disabled",
                                            command=self.start_tickonator, fg_color="#673AB7", hover_color="#512DA8",
                                            height=35, font=ctk.CTkFont(size=13, weight="bold"));
        self.btn_tickonator.pack(fill="x", pady=5)

        self.btn_interrupt = ctk.CTkButton(self.sidebar_frame, text="🛑 Прервать цикл", state="disabled",
                                           command=self.interrupt_tickonator, fg_color="#D32F2F",
                                           hover_color="#B71C1C");
        self.btn_interrupt.grid(row=5, column=0, padx=20, pady=(10, 5), sticky="sew")
        self.btn_quit = ctk.CTkButton(self.sidebar_frame, text="Выход", command=self.on_quit);
        self.btn_quit.grid(row=6, column=0, padx=20, pady=(5, 20), sticky="ew")

        self.log_textbox = ctk.CTkTextbox(self, wrap="word", font=("Consolas", 13), border_width=0);
        self.log_textbox.grid(row=0, column=1, padx=(10, 20), pady=(20, 20), sticky="nsew")
        for tag, color in {'success': '#4CAF50', 'error': '#F44336', 'warning': '#FFC107', 'info': '#2196F3',
                           'forgiving': '#E91E63', 'virgin': '#00BCD4'}.items():
            self.log_textbox.tag_config(tag, foreground=color)

        self.tickets_queue, self.current_idx = [], -1
        self.ticket_window_handle, self.form_window_handle, self.tickonator_running = None, None, False
        self.protocol("WM_DELETE_WINDOW", self.on_quit)

        self.setup_global_hotkeys()

#горячие клавиши (которые не работают )
    def setup_global_hotkeys(self):
        """Запускает слушатель клавиатуры в отдельном потоке."""
        self.hotkey_map = {
            '<ctrl>+<alt>+n': self._hotkey_fill_next,
            '<ctrl>+<alt>+s': self._hotkey_submit
        }

        def start_listener():
            try:
                with keyboard.GlobalHotKeys(self.hotkey_map) as h:
                    h.join()
            except Exception as e:
                print(f"Ошибка горячих клавиш: {e}")

        self.hotkey_thread = threading.Thread(target=start_listener, daemon=True)
        self.hotkey_thread.start()

        self.log("⌨️ Горячие клавиши (macOS):\nCmd+Opt+N = Заполнить след.\nCmd+Opt+S = Отправить и закрыть", "info")
        self.log("⚠️ Убедитесь, что у терминала есть доступ к 'Мониторингу ввода' в Системных настройках.", "warning")

    def _hotkey_fill_next(self):
        """Безопасная обертка для вызова из потока клавиатуры"""
        if self.btn_fill.cget("state") != "disabled":
            self.after(0, lambda: self.log("⌨️ [Hotkey] Cmd+Opt+N -> Заполняю...", "info"))
            self.after(0, self.fill_next_manual)

    def _hotkey_submit(self):
        """Безопасная обертка для вызова из потока клавиатуры"""
        if self.btn_submit_and_close.cget("state") != "disabled":
            self.after(0, lambda: self.log("⌨️ [Hotkey] Cmd+Opt+S -> Отправляю...", "info"))
            self.after(0, self.submit_and_close_manual)

    def log(self, msg: str, tag: str | None = None):
        formatted_msg = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"
        self.log_textbox.insert("end", formatted_msg, tag);
        self.log_textbox.see("end");
        self.update_idletasks()

    def _set_buttons_state_after_load(self, state: str):
        buttons = [self.btn_fill, self.btn_skip, self.btn_submit_and_close,
                   self.btn_tickonator, self.btn_tickonator_forgiving, self.btn_tickonator_virgin]
        for btn in buttons: btn.configure(state=state)

    def _set_buttons_state_during_run(self, is_running: bool):
        action_state, load_state, interrupt_state = ("disabled" if is_running else "normal",
                                                     "disabled" if is_running else "normal",
                                                     "normal" if is_running else "disabled")
        action_buttons = [self.btn_fill, self.btn_skip, self.btn_submit_and_close, self.btn_tickonator,
                          self.btn_tickonator_forgiving, self.btn_tickonator_virgin]
        for btn in action_buttons: btn.configure(state=action_state)
        for btn in [self.btn_load, self.btn_load_1000]: btn.configure(state=load_state)
        self.btn_interrupt.configure(state=interrupt_state)
        if not is_running and not self.tickets_queue:
            for btn in action_buttons: btn.configure(state="disabled")

    def load_tickets_open(self):
        self._load_tickets_generic(status="v-processe", limit=100)

    def load_tickets_bulk(self):
        self._load_tickets_generic(status="v-processe", limit=1000)

    def _load_tickets_generic(self, status: str, limit: int):
        try:
            self.log_textbox.delete('1.0', ctk.END);
            self.log(f"Загрузка до {limit} тикетов со статусом '{status}'...", "info")
            all_tickets, page = [], 1
            while len(all_tickets) < limit:
                self.log(f"Запрос страницы {page}...");
                raw = api_get_tickets(owner_id=1, status_list=status, page=page)
                if 'data' not in raw or not raw['data']: self.log("Больше тикетов не найдено.", "warning"); break
                new_tickets = [(int(v.get("id") or k), v) for k, v in raw['data'].items() if
                               str(v.get("id") or k).isdigit()]
                if not new_tickets: break
                all_tickets.extend(new_tickets);
                self.log(f"Загружено {len(all_tickets)} из ~{limit} тикетов...");
                page += 1;
                time.sleep(0.5)
            self.tickets_queue = sorted(all_tickets[:limit], key=lambda x: parse_api_datetime(
                x[1].get("date_created", "")) or datetime.now())
            self.current_idx = -1;
            self.log(f"✅ Итого загружено тикетов: {len(self.tickets_queue)}", "success")
            if self.tickets_queue:
                self._set_buttons_state_after_load("normal");
                messagebox.showinfo("Готово",
                                    f"Загружено {len(self.tickets_queue)} тикетов.")
            else:
                self._set_buttons_state_after_load("disabled");
                messagebox.showinfo("Пусто",
                                    "Нет тикетов для обработки.")
        except Exception as e:
            self.log(f"❌ Ошибка API: {e}", "error");
            messagebox.showerror("Ошибка API",
                                 f"Не удалось загрузить тикеты.\n{e}")

    def auto_close_and_skip(self, ticket_id, reason: str):
        self.log(f"Тикет ID={ticket_id} обрабатывается по особому правилу. Причина: {reason}.", "warning")
        log_skipped_ticket_to_db(ticket_id, reason)
        try:
            api_update_ticket_status(ticket_id, status="closed");
            self.log(f"✅ Заявка ID={ticket_id} успешно закрыта.",
                     "success");
            return True
        except Exception as e:
            self.log(f"❌ Не удалось автоматически закрыть заявку ID={ticket_id}. Ошибка: {e}", "error");
            return False

    def fill_next_manual(self):
        self.current_idx += 1
        if self._is_queue_finished(): return
        self._process_one_ticket(self.tickets_queue[self.current_idx])

    def _is_queue_finished(self) -> bool:
        if self.current_idx >= len(self.tickets_queue):
            messagebox.showinfo("Готово", "Все тикеты обработаны.")
            self._set_buttons_state_after_load("disabled");
            self.log("🎉 Все тикеты обработаны.", "success")
            self.interrupt_tickonator();
            return True
        return False

    def _process_one_ticket(self, ticket_tuple, is_auto_mode=False) -> dict | None:
        tid, ticket = ticket_tuple
        self.log("\n" + "=" * 80);
        self.log(
            f"▶️  Обработка тикета [{self.current_idx + 1}/{len(self.tickets_queue)}] ID={tid}, {ticket.get('unique_id', '')}, title='{ticket.get('title', '')}'")
        try:
            audit_payload = api_get_ticket_audit(tid)
        except Exception as e:
            self.log(f"⚠️ Ошибка получения аудита: {e}", "error");
            audit_payload = {"data": {}}
        last_manager_name, _, _ = find_last_manager_from_audit(audit_payload)
        if last_manager_name == "Нуржамал Мукаева":
            if self.auto_close_and_skip(tid, "Правило 'Нуржамал Мукаева'"):
                self.tickets_queue.pop(self.current_idx);
                self.current_idx -= 1
            return None
        try:
            comments_payload = api_get_comments(tid)
        except Exception as e:
            self.log(f"⚠️ Ошибка получения комментариев: {e}", "error");
            comments_payload = {"data": []}
        fields = build_fields_for_ticket(ticket, comments_payload, audit_payload)

        self.log("Сформированные поля для проверки:")
        missing_keys = self._validate_fields(fields)

        for k, v in fields.items():
            if v:
                marker, tag = ("[✓]", "success")
            elif k in missing_keys:
                marker, tag = ("[✗]", "error")
            else:
                marker, tag = ("[•]", "warning")

            self.log_textbox.insert("end", f"  {marker} ", tag);
            self.log_textbox.insert("end", f"{k:<25}: {v}\n")
        self.log_textbox.see("end")

        try:
            self._update_browser_windows(fields);
            fill_form_fields(fields)
            if not is_auto_mode: self.log(
                "✅ Окна обновлены, форма заполнена. Проверьте данные и нажмите 'Отправить и Закрыть' (Cmd+Opt+S).",
                "success")
        except Exception as e:
            self.log(f"❌ Ошибка Selenium: {e}", "error");
            messagebox.showerror("Ошибка Selenium", f"Не удалось управлять браузером.\n\nОшибка: {e}")
            self.ticket_window_handle, self.form_window_handle = None, None;
            self.interrupt_tickonator()
            return None
        return fields

    def _update_browser_windows(self, fields):
        drv = start_driver();
        current_handles = set(drv.window_handles)
        if self.ticket_window_handle not in current_handles or self.form_window_handle not in current_handles:
            self.ticket_window_handle, self.form_window_handle = None, None
        if not self.ticket_window_handle:
            self.log("Создаю новые окна браузера...");
            main_handle = drv.current_window_handle
            for handle in [h for h in current_handles if h != main_handle]:
                try:
                    drv.switch_to.window(handle);
                    drv.close()
                except:
                    pass
            drv.switch_to.window(main_handle);
            drv.get(fields.get("Ссылка"));
            self.ticket_window_handle = drv.current_window_handle
            drv.switch_to.new_window('window');
            drv.get(FORM_URL);
            self.form_window_handle = drv.current_window_handle
            sw, sh = drv.execute_script("return [window.screen.width, window.screen.height];");
            hw = sw // 2
            drv.switch_to.window(self.ticket_window_handle);
            drv.set_window_position(0, 0);
            drv.set_window_size(hw, sh)
            drv.switch_to.window(self.form_window_handle);
            drv.set_window_position(hw, 0);
            drv.set_window_size(hw, sh)
        else:
            self.log("Обновляю существующие окна...");
            drv.switch_to.window(self.ticket_window_handle);
            drv.get(fields.get("Ссылка"))
            drv.switch_to.window(self.form_window_handle);
            drv.get(FORM_URL)
        drv.switch_to.window(self.form_window_handle);
        time.sleep(2)

    def _submit_google_form(self) -> bool:
        self.log("Шаг 1/2: Отправка Google Формы...")
        try:
            driver.switch_to.window(self.form_window_handle)

            submit_button_xpath = "//div[@role='button'][.//span[text()='Отправить']]"
            submit_button = WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, submit_button_xpath)))

            driver.execute_script("arguments[0].click();", submit_button)

            try:
                WebDriverWait(driver, 10).until(
                    lambda d:
                    d.find_elements(By.XPATH,
                                    "//*[contains(text(), 'Ответ записан') or contains(text(), 'Your response has been recorded')]")
                    or
                    d.find_elements(By.XPATH,
                                    "//*[contains(text(), 'Отправить ещё один ответ') or contains(text(), 'Submit another response')]")
                    or
                    "formResponse" in d.current_url
                    or
                    d.find_elements(By.XPATH,
                                    "//*[contains(text(), 'Обязательный вопрос') or contains(text(), 'This is a required question')]")
                )

                errors = driver.find_elements(By.XPATH,
                                              "//*[contains(text(), 'Обязательный вопрос') or contains(text(), 'This is a required question')]")
                if errors:
                    visible_errors = [e for e in errors if e.is_displayed()]
                    if visible_errors:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", visible_errors[0])
                        self.log(f"❌ Ошибка валидации Google Forms! Не заполнены обязательные поля.", "error")
                        messagebox.showwarning("Ошибка формы",
                                               "Google Форма не отправлена!\nЕсть незаполненные обязательные поля.")
                        return False

                if ("formResponse" in driver.current_url
                        or driver.find_elements(By.XPATH, "//*[contains(text(), 'Ответ записан')]")
                        or driver.find_elements(By.XPATH, "//*[contains(text(), 'Отправить ещё один ответ')]")):
                    self.log("✅ Форма успешно отправлена (подтверждено).", "success")
                    time.sleep(1)
                    return True

                self.log("⚠️ Не удалось подтвердить отправку формы (страница не изменилась).", "warning")
                return False

            except Exception:
                self.log("⚠️ Тайм-аут ожидания ответа от Google Forms. Проверьте браузер.", "warning")
                return False

        except Exception as e:
            self.log(f"❌ Критическая ошибка Selenium: {e}", "error")
            messagebox.showerror("Ошибка Selenium", f"Сбой при работе с формой.\n\nОшибка: {e}")
            return False

    def _close_current_ticket_in_hde(self) -> bool:
        tid, _ = self.tickets_queue[self.current_idx]
        self.log(f"Шаг 2/2: Закрытие заявки ID={tid} в HelpDeskEddy...")
        try:
            api_update_ticket_status(tid, status="closed");
            self.log(f"✅ Заявка ID={tid} успешно закрыта.",
                     "success");
            return True
        except Exception as e:
            self.log(f"❌ Не удалось закрыть заявку ID={tid}. Ошибка API: {e}", "error");
            messagebox.showerror(
                "Ошибка API", f"Не удалось закрыть заявку ID={tid}.\n\n{e}");
            return False

    def submit_and_close_manual(self):
        if self.current_idx < 0 or self.current_idx >= len(self.tickets_queue): return

        if self._submit_google_form():
            if self._close_current_ticket_in_hde():
                tid, ticket = self.tickets_queue[self.current_idx]
                try:
                    comments_payload = api_get_comments(tid);
                    audit_payload = api_get_ticket_audit(tid)
                    fields_for_db = build_fields_for_ticket(ticket, comments_payload, audit_payload)
                    save_ticket_data_to_db(fields_for_db, tid)
                except Exception as e:
                    self.log(f"❌ Не удалось получить данные для сохранения в БД для тикета {tid}: {e}", "error")
                self.tickets_queue.pop(self.current_idx);
                self.current_idx -= 1;
                self.fill_next_manual()
        else:
            self.log("🛑 Операция прервана: Форма не была отправлена.", "error")

    def skip_current(self):
        if 0 <= self.current_idx < len(self.tickets_queue):
            tid, _ = self.tickets_queue[self.current_idx]
            self.log(f"⏩ Тикет id={tid} пропущен вручную.", "warning")
            log_skipped_ticket_to_db(tid, "Пропущено пользователем вручную")
        self.fill_next_manual()

    def start_tickonator(self):
        self._start_tickonator_base(self.tickonator_strict_loop, "Тикетонатор (Строгий)")

    def start_tickonator_forgiving(self):
        self._start_tickonator_base(self.tickonator_forgiving_loop, "Тикетонатор Давалка")

    def start_tickonator_virgin(self):
        self._start_tickonator_base(self.tickonator_virgin_loop, "Тикетонатор (Девственница)")

    def _start_tickonator_base(self, loop_function, mode_name):
        if self.tickonator_running: return
        self.log("\n" + "#" * 80, "info");
        self.log(f"🚀 Запуск '{mode_name}'! Начинаю автоматическую обработку.", "info");
        self.log("#" * 80 + "\n", "info")
        self.tickonator_running = True;
        self._set_buttons_state_during_run(True);
        self.after(100, loop_function)

    def interrupt_tickonator(self):
        if not self.tickonator_running: return
        self.tickonator_running = False
        self.log("\n" + "#" * 80, "warning");
        self.log("🛑 Цикл 'Тикетонатора' прерван пользователем.", "warning");
        self.log("#" * 80 + "\n", "warning")
        self._set_buttons_state_during_run(False)

    def tickonator_strict_loop(self):
        self._tickonator_generic_loop(is_forgiving=False)

    def tickonator_forgiving_loop(self):
        self._tickonator_generic_loop(is_forgiving=True)

    def _validate_fields(self, fields: dict) -> list:
        required_fields = ESSENTIAL_FIELDS.copy()
        if fields.get("Тег жалобы") == "Лишний товар":
            if "Как решен вопрос" in required_fields:
                required_fields.remove("Как решен вопрос")
        missing = [key for key in required_fields if not fields.get(key)]
        if fields.get("Решение тикета") == "Удержание":
            missing.extend(key for key in DEDUCTION_FIELDS if not fields.get(key))
        return missing

    def _tickonator_generic_loop(self, is_forgiving: bool):
        if not self.tickonator_running: return
        self.current_idx += 1
        if self._is_queue_finished(): return
        current_ticket = self.tickets_queue[self.current_idx]
        tid, _ = current_ticket
        fields = self._process_one_ticket(current_ticket, is_auto_mode=True)
        if fields is None: self.after(100,
                                      self.tickonator_forgiving_loop if is_forgiving else self.tickonator_strict_loop); return
        missing_fields = self._validate_fields(fields)
        if missing_fields:
            reason = f"Неполные данные: {', '.join(missing_fields)}"
            log_skipped_ticket_to_db(tid, reason)

            if is_forgiving:
                self.log(f"⏩ ДАВАЛКА-ПРОПУСК. Тикет ID={tid} пропущен ({reason})", "forgiving")
                self.after(100, self.tickonator_forgiving_loop)
            else:
                self.log(f"❗️ АВТОМАТИЗАЦИЯ ОСТАНОВЛЕНА. {reason}", "error")
                messagebox.showwarning("Тикетонатор остановлен",
                                       f"Не удалось заполнить все поля для тикета ID={tid}.\n\nПропущены: {', '.join(missing_fields)}")
                self.interrupt_tickonator()
            return

        if self._submit_google_form():
            if self._close_current_ticket_in_hde():
                save_ticket_data_to_db(fields, current_ticket[0])
                self.tickets_queue.pop(self.current_idx);
                self.current_idx -= 1
                self.after(600, self.tickonator_forgiving_loop if is_forgiving else self.tickonator_strict_loop)
            else:
                self.log("❌ Ошибка при закрытии тикета (API). Цикл остановлен.", "error")
                self.interrupt_tickonator()
        else:
            self.log("❌ Ошибка при отправке формы (Валидация). Цикл остановлен.", "error")
            self.interrupt_tickonator()

    def tickonator_virgin_loop(self):
        if not self.tickonator_running: return
        self.current_idx += 1
        if self._is_queue_finished(): return
        tid, ticket = current_ticket = self.tickets_queue[self.current_idx]
        self.log("\n" + "=" * 80);
        self.log(f"▶️  ПРЕДПРОВЕРКА [{self.current_idx + 1}/{len(self.tickets_queue)}] ID={tid}", 'virgin')

        reason = ""
        try:
            audit_payload = api_get_ticket_audit(tid)
        except Exception:
            reason = "Ошибка получения аудита"
            log_skipped_ticket_to_db(tid, reason)
            self.log(f"⏩ ДЕВСТВЕННИЦА-ПРОПУСК. Тикет ID={tid} пропущен ({reason}).", "forgiving");
            self.after(100, self.tickonator_virgin_loop);
            return

        last_manager_name, _, _ = find_last_manager_from_audit(audit_payload)
        if last_manager_name == "Нуржамал Мукаева":
            if self.auto_close_and_skip(tid, "Правило 'Нуржамал Мукаева'"): self.tickets_queue.pop(
                self.current_idx); self.current_idx -= 1
            self.after(100, self.tickonator_virgin_loop);
            return

        try:
            comments_payload = api_get_comments(tid)
        except Exception:
            reason = "Ошибка получения комментариев"
            log_skipped_ticket_to_db(tid, reason)
            self.log(f"⏩ ДЕВСТВЕННИЦА-ПРОПУСК. Тикет ID={tid} пропущен ({reason}).", "forgiving");
            self.after(100, self.tickonator_virgin_loop);
            return

        fields = build_fields_for_ticket(ticket, comments_payload, audit_payload)
        if missing_fields := self._validate_fields(fields):
            reason = f"Неполные данные: {', '.join(missing_fields)}"
            log_skipped_ticket_to_db(tid, reason)
            self.log(f"⏩ ДЕВСТВЕННИЦА-ПРОПУСК. Тикет ID={tid} пропущен ({reason})", "forgiving");
            self.after(100, self.tickonator_virgin_loop);
            return

        self.log(f"✅ Данные для тикета ID={tid} идеальны. Начинаю полное заполнение...", "success")
        try:
            self._update_browser_windows(fields);
            fill_form_fields(fields)
        except Exception as e:
            self.log(f"❌ Ошибка Selenium при заполнении идеального тикета ID={tid}: {e}", "error")
            self.interrupt_tickonator();
            return

        if self._submit_google_form():
            if self._close_current_ticket_in_hde():
                save_ticket_data_to_db(fields, tid)
                self.tickets_queue.pop(self.current_idx);
                self.current_idx -= 1;
                self.after(600, self.tickonator_virgin_loop)
            else:
                self.log("❌ Ошибка при закрытии тикета (API). Цикл остановлен.", "error")
                self.interrupt_tickonator()
        else:
            self.log("❌ Ошибка при отправке формы (Валидация). Цикл остановлен.", "error")
            self.interrupt_tickonator()

    def show_full_list_window(self):
        list_window = ctk.CTkToplevel(self)
        list_window.title("Полный список обработанных тикетов")
        list_window.geometry("900x600")
        list_window.transient(self)

        try:
            conn = sqlite3.connect('statistics.db')
            cursor = conn.cursor()
            cursor.execute(
                "SELECT nomer_zakaza, tag_zhaloby, otvetstvennyy_sotrudnik, summa_poter FROM tickets ORDER BY processed_at DESC")
            records = cursor.fetchall()
            conn.close()
        except Exception as e:
            messagebox.showerror("Ошибка БД", f"Не удалось загрузить данные из базы: {e}", parent=list_window)
            return

        frame = ctk.CTkFrame(list_window)
        frame.pack(expand=True, fill="both", padx=10, pady=10)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background="#2a2d2e", foreground="white", rowheight=25, fieldbackground="#343638",
                        bordercolor="#343638", borderwidth=0)
        style.map('Treeview', background=[('selected', '#22559b')])
        style.configure("Treeview.Heading", background="#565b5e", foreground="white", relief="flat")
        style.map("Treeview.Heading", background=[('active', '#3484F0')])

        tree = ttk.Treeview(frame, columns=("order", "tag", "employee", "loss"), show='headings')
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        tree.heading("order", text="Номер заказа")
        tree.heading("tag", text="Тег жалобы")
        tree.heading("employee", text="Ответственный сотрудник")
        tree.heading("loss", text="Сумма потерь")
        tree.column("order", width=100, anchor='center')
        tree.column("tag", width=200)
        tree.column("employee", width=250)
        tree.column("loss", width=100, anchor='center')

        for record in records:
            tree.insert("", "end", values=record)

        list_window.focus()

    def show_statistics_window(self):
        stats_window = ctk.CTkToplevel(self)
        stats_window.title("Статистика обработки")
        stats_window.geometry("800x600")
        stats_window.transient(self)

        main_frame = ctk.CTkFrame(stats_window)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        if not MATPLOTLIB_AVAILABLE:
            ctk.CTkLabel(main_frame, text="Для отображения графиков установите: pip install matplotlib",
                         text_color="orange").pack(pady=20)
            return

        try:
            conn = sqlite3.connect('statistics.db')
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM tickets")
            processed_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM skipped_tickets")
            skipped_count = cursor.fetchone()[0]

            cursor.execute("SELECT reason, COUNT(*) as cnt FROM skipped_tickets GROUP BY reason ORDER BY cnt DESC")
            skip_reasons = cursor.fetchall()

            conn.close()

        except Exception as e:
            ctk.CTkLabel(main_frame, text=f"Ошибка загрузки данных из БД:\n{e}", text_color="red").pack(pady=20)
            return

        chart_frame = ctk.CTkFrame(main_frame)
        chart_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        fig, ax = plt.subplots(figsize=(6, 4), dpi=100)
        plt.style.use('dark_background')
        fig.patch.set_alpha(0)
        ax.set_facecolor('#2B2B2B')

        labels = ['Обработано', 'Пропущено']
        counts = [processed_count, skipped_count]
        colors = ['#4CAF50', '#F44336']

        bars = ax.bar(labels, counts, color=colors)
        ax.set_title('Соотношение обработанных и пропущенных тикетов', color='white', fontsize=12)
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')
        ax.set_ylabel('Количество тикетов', color='white')

        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval + 0.1, int(yval), ha='center', va='bottom', color='white')

        plt.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(side=ctk.TOP, fill=ctk.BOTH, expand=True)

        report_frame = ctk.CTkFrame(main_frame)
        report_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

        textbox = ctk.CTkTextbox(report_frame, wrap="word", font=("Consolas", 12))
        textbox.pack(expand=True, fill="both", padx=5, pady=5)

        report_lines = ["АНАЛИЗ ПРОПУЩЕННЫХ ЗАЯВОК\n" + "=" * 40 + "\n"]
        if not skip_reasons:
            report_lines.append("Пропущенных заявок не найдено.")
        else:
            for reason, count in skip_reasons:
                report_lines.append(f"- {reason}: {count} раз")

        textbox.insert("1.0", "\n".join(report_lines))

        stats_window.focus()

    def on_quit(self):
        global driver
        if messagebox.askokcancel("Выход", "Вы уверены, что хотите выйти?"):
            try:
                if driver: driver.quit()
            except Exception:
                pass
            self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()