from __future__ import annotations

import io
import json
import os
import re
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.scraper import ParseFilters, TORGET_SUBCATEGORIES, fetch_listing_detail, recheck_rows, scrape_search_pages


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
MAX_ITEMS_PER_CATEGORY = 5000
MAX_PAGES_PER_CATEGORY = 80
ACCESS_KEY = os.getenv("BOT_ACCESS_KEY", "Ivanshurpato12")
OWNER_FILE = Path(__file__).resolve().parent / ".bot_owner.json"
OWNER_USER_ID: Optional[int] = None

CATEGORY_OPTIONS: List[Dict[str, str]] = [
    {
        "id": url.split("category=")[-1],
        "name": name,
        "url": url,
    }
    for name, url in TORGET_SUBCATEGORIES
]
CATEGORY_BY_ID: Dict[str, Dict[str, str]] = {item["id"]: item for item in CATEGORY_OPTIONS}


def _init_user_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    await_access_key = bool(context.user_data.get("await_access_key", False))
    context.user_data["selected_categories"] = []
    context.user_data["filters"] = {
        "fiks_ferdig": False,
        "published_today": False,
        "price_from": None,
        "price_to": None,
    }
    context.user_data["await_price_from"] = False
    context.user_data["await_price_to"] = False
    context.user_data["await_recheck"] = False
    context.user_data["await_link_check"] = False
    context.user_data["await_access_key"] = await_access_key


def _load_owner_user_id() -> Optional[int]:
    global OWNER_USER_ID
    if OWNER_USER_ID is not None:
        return OWNER_USER_ID
    if not OWNER_FILE.exists():
        return None
    try:
        data = json.loads(OWNER_FILE.read_text(encoding="utf-8"))
        user_id = int(data.get("user_id"))
        OWNER_USER_ID = user_id
        return user_id
    except Exception:
        return None


def _save_owner(user_id: int) -> None:
    global OWNER_USER_ID
    OWNER_USER_ID = int(user_id)
    OWNER_FILE.write_text(json.dumps({"user_id": OWNER_USER_ID}, ensure_ascii=False), encoding="utf-8")


def _is_owner(update: Update) -> bool:
    user = update.effective_user
    owner_id = _load_owner_user_id()
    return bool(user and owner_id and user.id == owner_id)


async def _reply_any(
    update: Update,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
        return
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup)


async def _ensure_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass

    owner_id = _load_owner_user_id()
    if owner_id is None:
        context.user_data["await_access_key"] = True
        await _reply_any(update, "Введите ключ доступа:")
        return False

    if _is_owner(update):
        return True

    await _reply_any(update, "Доступ запрещен. Бот привязан к другому пользователю.")
    return False


async def _try_handle_access_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.message
    if not message:
        return False

    text = (message.text or "").strip()
    if not text:
        return False

    owner_id = _load_owner_user_id()
    if owner_id is not None:
        if _is_owner(update):
            return False
        await message.reply_text("Доступ запрещен. Бот привязан к другому пользователю.")
        return True

    if text == ACCESS_KEY:
        user = update.effective_user
        if not user:
            await message.reply_text("Ошибка авторизации.")
            return True
        _save_owner(user.id)
        context.user_data["await_access_key"] = False
        _init_user_state(context)
        context.user_data["await_access_key"] = False
        await message.reply_text("Ключ принят. Доступ открыт.", reply_markup=_main_keyboard())
        return True

    context.user_data["await_access_key"] = True
    await message.reply_text("Неверный ключ. Попробуй еще раз.")
    return True


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Парсить", callback_data="main:parse")],
            [InlineKeyboardButton("Ре-чек", callback_data="main:recheck")],
            [InlineKeyboardButton("Чек ссылки", callback_data="main:linkcheck")],
        ]
    )


def _category_keyboard(selected_ids: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for option in CATEGORY_OPTIONS:
        prefix = "✅" if option["id"] in selected_ids else "⬜"
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {option['name']}",
                callback_data=f"cat:toggle:{option['id']}",
            )
        ])

    rows.append([InlineKeyboardButton("Сбросить категории", callback_data="cat:reset")])
    rows.append([InlineKeyboardButton("Дальше к фильтрам", callback_data="cat:next")])
    rows.append([InlineKeyboardButton("Назад", callback_data="cat:back")])
    return InlineKeyboardMarkup(rows)


def _filters_keyboard(filters_state: dict) -> InlineKeyboardMarkup:
    fiks = "ON" if filters_state.get("fiks_ferdig") else "OFF"
    published = "ON" if filters_state.get("published_today") else "OFF"
    price_from = filters_state.get("price_from")
    price_to = filters_state.get("price_to")

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"Fiks ferdig: {fiks}", callback_data="filter:toggle_fiks")],
            [InlineKeyboardButton(f"Опубликовано сегодня: {published}", callback_data="filter:toggle_published")],
            [
                InlineKeyboardButton(f"Цена от: {price_from if price_from is not None else '-'}", callback_data="filter:set_from"),
                InlineKeyboardButton(f"Цена до: {price_to if price_to is not None else '-'}", callback_data="filter:set_to"),
            ],
            [InlineKeyboardButton("Сбросить фильтры", callback_data="filter:reset")],
            [InlineKeyboardButton("Парс", callback_data="filter:run")],
            [InlineKeyboardButton("Назад к категориям", callback_data="filter:back")],
        ]
    )


def _build_parse_xlsx(rows: List[dict]) -> io.BytesIO:
    ordered_columns = ["категория", "название", "ссылка", "цена", "дата парса"]
    df = pd.DataFrame(rows, columns=ordered_columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
        ws = writer.sheets["data"]

        widths = {
            "A": 28,
            "B": 60,
            "C": 60,
            "D": 14,
            "E": 22,
        }
        for col, width in widths.items():
            ws.column_dimensions[col].width = width

    output.seek(0)
    return output


def _build_recheck_xlsx(rows: List[dict]) -> io.BytesIO:
    ordered_columns = ["категория", "название", "ссылка", "количество лайков", "дата", "статус", "фото"]
    df = pd.DataFrame(rows, columns=ordered_columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="recheck")
        ws = writer.sheets["recheck"]

        widths = {
            "A": 28,
            "B": 52,
            "C": 58,
            "D": 20,
            "E": 24,
            "F": 14,
            "G": 58,
        }
        for col, width in widths.items():
            ws.column_dimensions[col].width = width

    output.seek(0)
    return output


def _filters_summary(filters_state: dict) -> str:
    return (
        f"Fiks ferdig: {'ON' if filters_state.get('fiks_ferdig') else 'OFF'}\n"
        f"Опубликовано сегодня: {'ON' if filters_state.get('published_today') else 'OFF'}\n"
        f"Цена: {filters_state.get('price_from') if filters_state.get('price_from') is not None else '-'}"
        f" - {filters_state.get('price_to') if filters_state.get('price_to') is not None else '-'}"
    )


def _parse_int_value(text: str) -> Optional[int]:
    text = (text or "").strip()
    if not text:
        return None
    if text in {"-", "none", "null", "нет"}:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None


def _extract_first_url(text: str) -> str:
    match = re.search(r"https?://\S+", text or "")
    return match.group(0).rstrip(").,;!?") if match else ""


def _progress_bar(done: int, total: int, width: int = 14) -> str:
    if total <= 0:
        return "[--------------] 0%"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(round(ratio * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + f"] {int(ratio * 100)}%"


def _extract_input_rows(df: pd.DataFrame) -> List[dict]:
    mapped_rows = []

    for _, row in df.fillna("").iterrows():
        mapped_rows.append(
            {
                "category": row.get("категория") or row.get("category") or "",
                "title": row.get("название") or row.get("title") or "",
                "url": row.get("ссылка") or row.get("url") or row.get("link") or "",
            }
        )

    return [r for r in mapped_rows if r.get("url")]


async def _safe_query_answer(update: Update) -> None:
    query = update.callback_query
    if not query:
        return
    try:
        await query.answer()
    except Exception:
        return


async def _safe_edit_or_send(
    update: Update,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    query = update.callback_query
    if not query:
        return

    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
        return
    except BadRequest as exc:
        message = str(exc)
        if "Message is not modified" in message:
            return

    if query.message:
        await query.message.reply_text(text=text, reply_markup=reply_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    _init_user_state(context)

    if update.message:
        await update.message.reply_text("Выбери действие:", reply_markup=_main_keyboard())
    elif update.callback_query:
        await _safe_query_answer(update)
        await _safe_edit_or_send(update, "Выбери действие:", _main_keyboard())


async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    _init_user_state(context)
    context.user_data["await_recheck"] = False
    if update.message:
        await update.message.reply_text(
            "Выбери категории для парса (можно несколько):",
            reply_markup=_category_keyboard([]),
        )


async def recheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    _init_user_state(context)
    context.user_data["await_recheck"] = True
    if update.message:
        await update.message.reply_text("Пришли XLSX файл для ре-чека.")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    _init_user_state(context)
    context.user_data["await_link_check"] = True
    if update.message:
        await update.message.reply_text("Пришли одну ссылку на объявление FINN. Верну статус и лайки.")


async def handle_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    await _safe_query_answer(update)

    action = (update.callback_query.data or "").split(":", 1)[1]
    if action == "parse":
        _init_user_state(context)
        await _safe_edit_or_send(
            update,
            "Выбери категории для парса (можно несколько):",
            _category_keyboard([]),
        )
        return

    if action == "recheck":
        _init_user_state(context)
        context.user_data["await_recheck"] = True
        await _safe_edit_or_send(update, "Пришли XLSX файл для ре-чека.")
        return

    if action == "linkcheck":
        _init_user_state(context)
        context.user_data["await_link_check"] = True
        await _safe_edit_or_send(update, "Пришли одну ссылку на объявление FINN. Верну статус и лайки.")


async def handle_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    await _safe_query_answer(update)

    selected: List[str] = context.user_data.get("selected_categories", [])
    data = update.callback_query.data or ""

    if data == "cat:reset":
        selected.clear()
        context.user_data["selected_categories"] = selected
        await _safe_edit_or_send(
            update,
            "Категории сброшены. Выбери заново:",
            _category_keyboard(selected),
        )
        return

    if data == "cat:back":
        await _safe_edit_or_send(update, "Выбери действие:", _main_keyboard())
        return

    if data == "cat:next":
        if not selected:
            await _safe_edit_or_send(
                update,
                "Выбери минимум одну категорию.",
                _category_keyboard(selected),
            )
            return

        filters_state = context.user_data.get("filters", {})
        await _safe_edit_or_send(
            update,
            "Настрой фильтры и нажми «Парс».\n\n" + _filters_summary(filters_state),
            _filters_keyboard(filters_state),
        )
        return

    parts = data.split(":", 2)
    if len(parts) == 3 and parts[1] == "toggle":
        category_id = parts[2]
        if category_id not in CATEGORY_BY_ID:
            return

        if category_id in selected:
            selected.remove(category_id)
        else:
            selected.append(category_id)

        context.user_data["selected_categories"] = selected
        label = ", ".join(CATEGORY_BY_ID[item]["name"] for item in selected) if selected else "ничего"
        await _safe_edit_or_send(
            update,
            f"Выбранные категории: {label}",
            _category_keyboard(selected),
        )


async def _run_parse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    selected: List[str] = context.user_data.get("selected_categories", [])
    if not selected:
        await message.reply_text("Категории не выбраны. Нажми /parse")
        return

    filters_state = context.user_data.get("filters", {})
    parse_filters = ParseFilters(
        fiks_ferdig=bool(filters_state.get("fiks_ferdig")),
        price_from=filters_state.get("price_from"),
        price_to=filters_state.get("price_to"),
        published_today=bool(filters_state.get("published_today")),
    )

    await message.reply_text("Запускаю парс. Это может занять несколько минут.")
    await message.chat.send_action(ChatAction.TYPING)
    progress_message = await message.reply_text("Прогресс парса: [--------------] 0%")

    parse_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")

    result_rows: List[dict] = []
    total_found = 0
    total_categories = len(selected)
    last_edit_ts = 0.0

    for category_index, category_id in enumerate(selected, start=1):
        option = CATEGORY_BY_ID[category_id]
        await message.reply_text(f"Парс категории: {option['name']} ({category_index}/{total_categories})")

        async def _on_progress(payload: dict) -> None:
            nonlocal last_edit_ts
            now = time.monotonic()
            is_done_event = bool(payload.get("done"))
            if not is_done_event and now - last_edit_ts < 1.0:
                return
            last_edit_ts = now

            page = int(payload.get("page") or 0)
            in_category = int(payload.get("total") or 0)
            completed_categories = category_index - (0 if is_done_event else 1)
            bar = _progress_bar(completed_categories, total_categories)

            text = (
                f"Прогресс парса: {bar}\n"
                f"Категория: {option['name']} ({category_index}/{total_categories})\n"
                f"Страница: {page}\n"
                f"Найдено в категории: {in_category}\n"
                f"Всего найдено: {total_found + in_category}"
            )
            try:
                await progress_message.edit_text(text)
            except Exception:
                return

        listings = await scrape_search_pages(
            category_name=option["name"],
            category_url=option["url"],
            filters=parse_filters,
            max_items=MAX_ITEMS_PER_CATEGORY,
            max_pages=MAX_PAGES_PER_CATEGORY,
            progress_cb=_on_progress,
        )

        total_found += len(listings)
        try:
            done_bar = _progress_bar(category_index, total_categories)
            await progress_message.edit_text(
                f"Прогресс парса: {done_bar}\n"
                f"Категория завершена: {option['name']}\n"
                f"Найдено в категории: {len(listings)}\n"
                f"Всего найдено: {total_found}"
            )
        except Exception:
            pass

        for listing in listings:
            listing_dict = asdict(listing)
            result_rows.append(
                {
                    "категория": listing_dict.get("subcategory") or option["name"],
                    "название": listing_dict.get("title", ""),
                    "ссылка": listing_dict.get("url", ""),
                    "цена": listing_dict.get("price", ""),
                    "дата парса": parse_date,
                }
            )

    if not result_rows:
        await message.reply_text("Объявления не найдены по текущим фильтрам.")
        return

    # Final dedupe across selected categories.
    deduped = []
    seen = set()
    for row in result_rows:
        url = row.get("ссылка", "")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(row)

    output = _build_parse_xlsx(deduped)

    await message.chat.send_action(ChatAction.UPLOAD_DOCUMENT)
    await message.reply_document(document=output, filename="finn_parse.xlsx")
    try:
        await progress_message.edit_text(
            f"Прогресс парса: {_progress_bar(total_categories, total_categories)}\n"
            f"Готово. Собрано: {len(deduped)}"
        )
    except Exception:
        pass
    await message.reply_text(
        f"Готово. Собрано: {len(deduped)} объявлений.\n"
        f"Категорий: {len(selected)}\n"
        f"Всего до дедупликации: {total_found}"
    )


async def handle_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return
    await _safe_query_answer(update)

    data = update.callback_query.data or ""
    filters_state = context.user_data.get("filters", {})

    if data == "filter:toggle_fiks":
        filters_state["fiks_ferdig"] = not bool(filters_state.get("fiks_ferdig"))
    elif data == "filter:toggle_published":
        filters_state["published_today"] = not bool(filters_state.get("published_today"))
    elif data == "filter:reset":
        filters_state["fiks_ferdig"] = False
        filters_state["published_today"] = False
        filters_state["price_from"] = None
        filters_state["price_to"] = None
    elif data == "filter:set_from":
        context.user_data["await_price_from"] = True
        context.user_data["await_price_to"] = False
        if update.callback_query.message:
            await update.callback_query.message.reply_text(
                "Введи `цена от` числом (например 1000). Для очистки отправь `-`.",
                parse_mode=ParseMode.MARKDOWN,
            )
        return
    elif data == "filter:set_to":
        context.user_data["await_price_to"] = True
        context.user_data["await_price_from"] = False
        if update.callback_query.message:
            await update.callback_query.message.reply_text(
                "Введи `цена до` числом (например 20000). Для очистки отправь `-`.",
                parse_mode=ParseMode.MARKDOWN,
            )
        return
    elif data == "filter:back":
        selected = context.user_data.get("selected_categories", [])
        await _safe_edit_or_send(
            update,
            "Выбери категории для парса (можно несколько):",
            _category_keyboard(selected),
        )
        return
    elif data == "filter:run":
        if update.callback_query.message:
            await _safe_edit_or_send(update, "Запускаю парс...")
            await _run_parse(update, context)
            await update.callback_query.message.reply_text("Выбери действие:", reply_markup=_main_keyboard())
        return

    context.user_data["filters"] = filters_state
    await _safe_edit_or_send(
        update,
        "Настрой фильтры и нажми «Парс».\n\n" + _filters_summary(filters_state),
        _filters_keyboard(filters_state),
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _try_handle_access_key(update, context):
        return
    if not await _ensure_authorized(update, context):
        return

    message = update.message
    if not message:
        return

    text = (message.text or "").strip()
    filters_state = context.user_data.get("filters", {})

    if context.user_data.get("await_price_from"):
        context.user_data["await_price_from"] = False
        filters_state["price_from"] = _parse_int_value(text)
        context.user_data["filters"] = filters_state
        await message.reply_text(
            "Цена от обновлена.\n\n" + _filters_summary(filters_state),
            reply_markup=_filters_keyboard(filters_state),
        )
        return

    if context.user_data.get("await_price_to"):
        context.user_data["await_price_to"] = False
        filters_state["price_to"] = _parse_int_value(text)
        context.user_data["filters"] = filters_state
        await message.reply_text(
            "Цена до обновлена.\n\n" + _filters_summary(filters_state),
            reply_markup=_filters_keyboard(filters_state),
        )
        return

    if context.user_data.get("await_link_check"):
        url = _extract_first_url(text)
        if not url:
            await message.reply_text("Нужна ссылка вида https://www.finn.no/recommerce/forsale/item/...")
            return
        await message.reply_text("Проверяю ссылку...")
        details = await fetch_listing_detail(url)
        await message.reply_text(
            f"Статус: {details.get('status', '')}\n"
            f"Лайки: {details.get('likes', 0)}\n"
            f"Ссылка: {url}"
        )
        context.user_data["await_link_check"] = False
        await message.reply_text("Выбери действие:", reply_markup=_main_keyboard())
        return

    if "finn.no/recommerce/forsale/item/" in text:
        url = _extract_first_url(text)
        if url:
            await message.reply_text("Проверяю ссылку...")
            details = await fetch_listing_detail(url)
            await message.reply_text(
                f"Статус: {details.get('status', '')}\n"
                f"Лайки: {details.get('likes', 0)}\n"
                f"Ссылка: {url}"
            )
            return


async def _send_recheck_alert(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    row: dict,
) -> None:
    caption = (
        f"Статус: {row.get('статус', '')}\n"
        f"Название: {row.get('название', '')}\n"
        f"Лайки: {row.get('количество лайков', 0)}\n"
        f"Ссылка: {row.get('ссылка', '')}"
    )
    caption = caption[:1000]

    photo = row.get("фото", "")
    if photo:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=photo, caption=caption)
            return
        except Exception:
            pass

    await context.bot.send_message(chat_id=chat_id, text=caption)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_authorized(update, context):
        return

    message = update.message
    if not message:
        return

    mode = None
    if context.user_data.get("await_recheck"):
        mode = "recheck"

    if mode is None:
        await message.reply_text("Сейчас не ожидаю файл. Нажми «Ре-чек».")
        return

    doc = message.document
    if not doc or not (doc.file_name or "").lower().endswith(".xlsx"):
        await message.reply_text("Нужен XLSX файл.")
        return

    context.user_data["await_recheck"] = False

    await message.chat.send_action(ChatAction.TYPING)
    telegram_file = await doc.get_file()
    file_bytes = await telegram_file.download_as_bytearray()

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception:
        await message.reply_text("Файл не читается как XLSX.")
        return

    sheet_name = "data" if "data" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=sheet_name)
    input_rows = _extract_input_rows(df)

    if not input_rows:
        await message.reply_text("В файле нет ссылок для проверки.")
        return

    total = len(input_rows)
    started_text = "Запускаю ре-чек"
    await message.reply_text(f"{started_text}: {total} ссылок.")
    progress_message = await message.reply_text(
        f"{started_text}: {_progress_bar(0, total)}\nПроверено: 0/{total}"
    )
    last_edit_ts = 0.0

    async def _on_progress(payload: dict) -> None:
        nonlocal last_edit_ts
        done = int(payload.get("done") or 0)
        status = str(payload.get("status") or "")
        now = time.monotonic()
        if done < total and now - last_edit_ts < 0.9:
            return
        last_edit_ts = now
        try:
            await progress_message.edit_text(
                f"{started_text}: {_progress_bar(done, total)}\n"
                f"Проверено: {done}/{total}\n"
                f"Последний статус: {status or '-'}"
            )
        except Exception:
            return

    results = await recheck_rows(
        input_rows,
        concurrency=5,
        include_active=False,
        progress_cb=_on_progress,
    )
    if not results:
        try:
            await progress_message.edit_text(
                f"{started_text}: {_progress_bar(total, total)}\nПроверено: {total}/{total}\nГотово."
            )
        except Exception:
            pass
        await message.reply_text("Проданных/инактивных/404 объявлений не найдено.")
        await message.reply_text("Выбери действие:", reply_markup=_main_keyboard())
        return

    recheck_table_rows: List[dict] = []
    for item in results:
        row = {
            "категория": item.category,
            "название": item.title,
            "ссылка": item.url,
            "количество лайков": item.likes,
            "дата": item.date,
            "статус": item.status,
            "фото": item.photo,
        }
        recheck_table_rows.append(row)
        await _send_recheck_alert(message.chat_id, context, row)

    output = _build_recheck_xlsx(recheck_table_rows)

    await message.chat.send_action(ChatAction.UPLOAD_DOCUMENT)
    await message.reply_document(document=output, filename="finn_recheck.xlsx")
    try:
        await progress_message.edit_text(
            f"{started_text}: {_progress_bar(total, total)}\nПроверено: {total}/{total}\nГотово."
        )
    except Exception:
        pass
    await message.reply_text(f"Ре-чек завершен. Найдено: {len(recheck_table_rows)}")
    await message.reply_text("Выбери действие:", reply_markup=_main_keyboard())


def build_app(token: str) -> Application:
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parse_command))
    app.add_handler(CommandHandler("recheck", recheck_command))
    app.add_handler(CommandHandler("info", info_command))

    app.add_handler(CallbackQueryHandler(handle_main, pattern=r"^main:"))
    app.add_handler(CallbackQueryHandler(handle_category, pattern=r"^cat:"))
    app.add_handler(CallbackQueryHandler(handle_filter, pattern=r"^filter:"))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    return app


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise SystemExit("BOT_TOKEN is not set")

    app = build_app(token)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
