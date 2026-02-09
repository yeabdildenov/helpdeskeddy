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
EMAIL = "-------"
API_KEY = "-------"
BASE_URL = "https://arbuz.helpdeskeddy.com/api/v2"

credentials = f"{EMAIL}:{API_KEY}"
auth_header = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
HEADERS = {
    "Authorization": f"Basic {auth_header}",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json"
}

FORM_URL = "---------"
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
            res_tiketa = manager_comment_text, "HDE_Filler_Profile")
        options.add_argument(f"user-data-dir={profile_path}")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
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
                if not new_tickets: break\
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
            self.interrupt_tickonator(

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


        if self._submit_google_form():
            if self._close_current_ticket_in_hde():
                save_ticket_data_to_db(fields, tid)
                self.tickets_queue.pop(self.current_idx);
                self.current_idx -= 1;
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


        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval + 0.1, int(yval)

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
