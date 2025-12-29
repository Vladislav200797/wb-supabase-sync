import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil.parser import isoparse

# ====== Конфиг ======
WB_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service role
WB_API_KEY = os.environ["WB_API_KEY"]

# Разово можно указать день для перезаливки, потом убрать/оставить пустым
# Формат: "YYYY-MM-DD" (день в МСК)
RELOAD_DAY = os.getenv("RELOAD_DAY", "").strip() or None

# Нахлёст назад по last_change_date (чтобы не терять заказы на границе)
OVERLAP_MINUTES = int(os.getenv("OVERLAP_MINUTES", "120"))

# Пакет для upsert
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# WB лимит: 1 запрос в минуту
WB_SLEEP_SECONDS = int(os.getenv("WB_SLEEP_SECONDS", "65"))

MSK = ZoneInfo("Europe/Moscow")


# ====== HTTP headers ======
def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def wb_headers():
    return {
        "Authorization": WB_API_KEY,
        "Accept": "application/json",
        "Accept-Encoding": "gzip,deflate,compress",
    }


# ====== Time helpers ======
def parse_wb_dt_to_utc(dt_str: str) -> datetime:
    """
    WB часто отдаёт дату/время без timezone => считаем это МСК.
    Возвращаем aware datetime в UTC.
    """
    dt = isoparse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(timezone.utc)


def format_dt_for_wb(date_utc: datetime) -> str:
    """
    WB ожидает dateFrom в МСК (в их доке).
    Отдаём строку в МСК без Z, секундная точность.
    """
    dt_msk = date_utc.astimezone(MSK)
    return dt_msk.strftime("%Y-%m-%dT%H:%M:%S")


# ====== WB request with retries ======
def wb_request(date_from: str, flag: int = 0, max_retries: int = 6):
    """
    WB лимит 1 запрос/мин. Делаем ретраи на 429/5xx/таймаут.
    """
    params = {"dateFrom": date_from, "flag": flag}

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(
                WB_API_URL,
                headers=wb_headers(),
                params=params,
                timeout=60
            )

            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []

            if r.status_code == 429:
                # слишком часто
                sleep_s = max(WB_SLEEP_SECONDS, 65)
                print(f"[WB] 429 rate limit. Sleep {sleep_s}s...")
                time.sleep(sleep_s)
                continue

            if 500 <= r.status_code < 600:
                # временно на стороне WB
                sleep_s = 10 * attempt
                print(f"[WB] {r.status_code} server error. Sleep {sleep_s}s...")
                time.sleep(sleep_s)
                continue

            print(f"[WB] HTTP {r.status_code}: {r.text}")
            return []

        except requests.RequestException as e:
            sleep_s = 10 * attempt
            print(f"[WB] Network error: {e}. Sleep {sleep_s}s...")
            time.sleep(sleep_s)

    return []


# ====== Supabase helpers ======
def get_db_max_last_change_date_utc() -> datetime:
    """
    Без sync_state: курсор берём из самой таблицы wb_orders.
    """
    url = f"{SUPABASE_URL}/rest/v1/wb_orders"
    params = {
        "select": "last_change_date",
        "order": "last_change_date.desc",
        "limit": "1",
    }
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=60)
    r.raise_for_status()

    rows = r.json()
    if not rows:
        # если таблица пустая
        start_msk = datetime(2023, 1, 1, 0, 0, 0, tzinfo=MSK)
        return start_msk.astimezone(timezone.utc)

    return isoparse(rows[0]["last_change_date"]).astimezone(timezone.utc)


def delete_msk_day_from_db(day_yyyy_mm_dd: str):
    """
    Удаляем из Supabase только один день (по МСК).
    Фильтруем по UTC-диапазону, чтобы PostgREST понял.
    """
    day = datetime.fromisoformat(day_yyyy_mm_dd).date()

    start_msk = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=MSK)
    end_msk = start_msk + timedelta(days=1)

    start_utc = start_msk.astimezone(timezone.utc).isoformat()
    end_utc = end_msk.astimezone(timezone.utc).isoformat()

    url = f"{SUPABASE_URL}/rest/v1/wb_orders"
    params = [
        ("date", f"gte.{start_utc}"),
        ("date", f"lt.{end_utc}"),
    ]

    r = requests.delete(url, headers=supabase_headers(), params=params, timeout=120)
    r.raise_for_status()
    print(f"[DB] Deleted rows for MSK day {day_yyyy_mm_dd} (UTC {start_utc} .. {end_utc})")


def upsert_orders_to_db(rows: list[dict]):
    """
    Upsert по srid => не плодим дубли, а обновляем отмены/изменения.
    """
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/wb_orders?on_conflict=srid"
    headers = supabase_headers()
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        r = requests.post(url, headers=headers, data=json.dumps(batch), timeout=180)
        r.raise_for_status()
        print(f"[DB] Upserted batch {i}..{i + len(batch)}")


# ====== Transform WB -> DB rows ======
def transform_wb_orders(wb_orders: list[dict]) -> list[dict]:
    out = []

    for o in wb_orders:
        # ВАЖНО: date и lastChangeDate у WB считаем МСК, приводим в UTC ISO
        date_utc = parse_wb_dt_to_utc(o["date"]).isoformat()
        lcd_utc = parse_wb_dt_to_utc(o["lastChangeDate"]).isoformat()

        cancel_raw = o.get("cancelDate")
        cancel_utc = None
        if cancel_raw and cancel_raw != "0001-01-01T00:00:00":
            cancel_utc = parse_wb_dt_to_utc(cancel_raw).isoformat()

        out.append({
            "date": date_utc,
            "last_change_date": lcd_utc,
            "warehouse_name": o.get("warehouseName"),
            "warehouse_type": o.get("warehouseType"),
            "country_name": o.get("countryName"),
            "oblast_okrug_name": o.get("oblastOkrugName"),
            "region_name": o.get("regionName"),
            "supplier_article": o.get("supplierArticle"),
            "nm_id": o.get("nmId"),
            "barcode": o.get("barcode"),
            "category": o.get("category"),
            "subject": o.get("subject"),
            "brand": o.get("brand"),
            "tech_size": o.get("techSize"),
            "income_id": o.get("incomeID"),
            "is_supply": o.get("isSupply"),
            "is_realization": o.get("isRealization"),
            "total_price": o.get("totalPrice"),
            "discount_percent": o.get("discountPercent"),
            "spp": o.get("spp"),
            "finished_price": o.get("finishedPrice"),
            "price_with_disc": o.get("priceWithDisc"),
            "is_cancel": o.get("isCancel"),
            "cancel_date": cancel_utc,
            "sticker": o.get("sticker"),
            "g_number": o.get("gNumber"),
            "srid": o.get("srid"),
        })

    return out


# ====== One-day reload (flag=1) ======
def full_reload_day_msk(day_yyyy_mm_dd: str):
    """
    1) удаляем день (по МСК)
    2) WB flag=1 выгружаем весь день
    3) upsert
    """
    print(f"=== FULL RELOAD MSK DAY {day_yyyy_mm_dd} ===")
    delete_msk_day_from_db(day_yyyy_mm_dd)

    wb_data = wb_request(day_yyyy_mm_dd, flag=1)
    print(f"[WB] flag=1 day={day_yyyy_mm_dd}: got {len(wb_data)} rows")

    transformed = transform_wb_orders(wb_data)
    upsert_orders_to_db(transformed)
    print("=== FULL RELOAD DONE ===")


# ====== Incremental sync (flag=0) ======
def incremental_sync():
    """
    Инкремент:
    - берём max(last_change_date) из БД
    - откатываемся назад на overlap
    - WB flag=0: грузим изменения с dateFrom
    - пагинация: dateFrom = lastChangeDate последней строки ответа (как WB рекомендует)
    - upsert по srid
    """
    db_max_lcd = get_db_max_last_change_date_utc()
    start_utc = db_max_lcd - timedelta(minutes=OVERLAP_MINUTES)
    cursor = format_dt_for_wb(start_utc)

    print("=== INCREMENTAL SYNC ===")
    print(f"[DB] max(last_change_date) UTC: {db_max_lcd.isoformat()}")
    print(f"[SYNC] overlap minutes: {OVERLAP_MINUTES}")
    print(f"[SYNC] start cursor for WB (MSK): {cursor}")

    pages = 0
    prev_cursor = None

    while True:
        pages += 1
        print(f"[WB] page {pages} dateFrom={cursor} flag=0")
        wb_data = wb_request(cursor, flag=0)

        if not wb_data:
            print("[WB] empty response => stop.")
            break

        transformed = transform_wb_orders(wb_data)
        upsert_orders_to_db(transformed)

        last_row_lcd = wb_data[-1].get("lastChangeDate")
        if not last_row_lcd:
            print("[WB] last row has no lastChangeDate => stop.")
            break

        # Защита от зацикливания на одинаковом курсоре
        if prev_cursor == last_row_lcd and len(wb_data) >= 80000:
            bumped = parse_wb_dt_to_utc(last_row_lcd) + timedelta(seconds=1)
            cursor = format_dt_for_wb(bumped)
            print(f"[WARN] cursor stuck at big page; bump +1s => {cursor}")
        else:
            cursor = last_row_lcd

        prev_cursor = last_row_lcd

        print(f"[SYNC] sleep {WB_SLEEP_SECONDS}s (WB limit 1 req/min)")
        time.sleep(WB_SLEEP_SECONDS)

    print("=== INCREMENTAL DONE ===")


def main():
    print("############################################")
    print("# WB Orders -> Supabase (Python sync)")
    print("############################################")

    if RELOAD_DAY:
        full_reload_day_msk(RELOAD_DAY)

    incremental_sync()
    print("=== ALL DONE ===")


if __name__ == "__main__":
    main()
