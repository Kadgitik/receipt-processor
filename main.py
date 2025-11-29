# -*- coding: utf-8 -*-
"""
Receipt processing service - исправленная версия с ожиданием batch job завершения
"""

import os
import json
import logging
import time
import re
import uuid
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple

from google.cloud import bigquery, storage, aiplatform, bigquery_storage_v1
from flask import Request, jsonify
import functions_framework
import pandas as pd

# -----------------------------------------------------------------------------
# Configuration and constants
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "datascience-417611")
LOCATION = os.getenv("LOCATION", "europe-central2")
DATASET = os.getenv("DATASET", "vlad")  # BigQuery dataset остается vlad
BUCKET_NAME = f"{PROJECT_ID}-batch-processing"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBJTQ1yNa9fZASgskN4IBXVgy-V8J931Mw")

# Initialize BigQuery client for country lookup
bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("receipt_processor")

# Маппинг стран на валюты
COUNTRY_TO_CURRENCY = {
    'PL': 'PLN',  # Польша - Zloty
    'GB': 'GBP',  # Великобритания - Pounds Sterling
    'RO': 'RON',  # Румыния - Romanian Leu
    'HU': 'HUF',  # Венгрия - Hungarian Forint
    'RS': 'RSD',  # Сербия - Serbian Dinar
    'IT': 'EUR',  # Италия - Euro
    'DE': 'EUR',  # Германия - Euro
    'FR': 'EUR',  # Франция - Euro
    'ES': 'EUR',  # Испания - Euro
    'PT': 'EUR',  # Португалия - Euro
    'NL': 'EUR',  # Нидерланды - Euro
    'BE': 'EUR',  # Бельгия - Euro
    'AT': 'EUR',  # Австрия - Euro
    'IE': 'EUR',  # Ирландия - Euro
    'FI': 'EUR',  # Финляндия - Euro
    'GR': 'EUR',  # Греция - Euro
    'SK': 'EUR',  # Словакия - Euro
    'SI': 'EUR',  # Словения - Euro
    'EE': 'EUR',  # Эстония - Euro
    'LV': 'EUR',  # Латвия - Euro
    'LT': 'EUR',  # Литва - Euro
    'LU': 'EUR',  # Люксембург - Euro
    'MT': 'EUR',  # Мальта - Euro
    'CY': 'EUR',  # Кипр - Euro
}

def get_currency_for_country(country: str) -> str:
    """Получает валюту для страны"""
    return COUNTRY_TO_CURRENCY.get(country, 'EUR')  # По умолчанию EUR

def get_country_from_gamification(gamification_id: str) -> Optional[str]:
    """Получает страну по gamification_id из BigQuery lookup таблицы"""
    if not gamification_id:
        return None
        
    try:
        query = f"""
        SELECT bill_country 
        FROM `{PROJECT_ID}.{DATASET}.gamification_lookup`
        WHERE gamification_id = @gamification_id
          AND is_active = TRUE
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("gamification_id", "STRING", gamification_id)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config, location=LOCATION)
        results = list(query_job.result())
        
        if results:
            return results[0].bill_country
        else:
            logger.warning(f"⚠️ No active gamification found for ID: {gamification_id}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error getting country for {gamification_id}: {e}")
        return None

def get_active_promotions() -> List[Dict[str, Any]]:
    """
    Получает список активных промо-акций из gamification_lookup
    
    ИЗМЕНЕНО: Убраны проверки дат (start_date, finish_date)
    Теперь используется только is_active = TRUE (который определяется как NOT is_off)
    Это согласуется с логикой gamification-sync и daily-export-to-gcs
    """
    try:
        query = f"""
        SELECT 
            gamification_id,
            bill_country,
            bill_name,
            company_id,
            start_date,
            finish_date,
            is_active,
            created_at
        FROM `{PROJECT_ID}.{DATASET}.gamification_lookup`
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        """
        
        query_job = bq_client.query(query, location=LOCATION)
        results = list(query_job.result())
        
        promotions = []
        for row in results:
            promotions.append({
                "gamification_id": row.gamification_id,
                "bill_country": row.bill_country,
                "bill_name": row.bill_name,
                "company_id": row.company_id,
                "start_date": row.start_date,
                "finish_date": row.finish_date,
                "is_active": row.is_active,
                "created_at": row.created_at
            })
        
        logger.info(f"Found {len(promotions)} active promotions")
        return promotions
        
    except Exception as e:
        logger.error(f"❌ Error getting active promotions: {e}")
        return []

def process_active_promotions_parallel(report_id: str, report_name: str, limit_per_promo: int = None) -> Dict[str, Any]:
    """
    Обрабатывает все активные промо-акции параллельно
    
    Args:
        report_id: ID отчета
        report_name: Название отчета  
        limit_per_promo: Лимит чеков на промо-акцию
        
    Returns:
        Результаты обработки всех промо-акций
    """
    try:
        # Получаем активные промо-акции
        active_promotions = get_active_promotions()
        
        if not active_promotions:
            logger.warning("No active promotions found")
            return {
                "status": "warning",
                "message": "No active promotions found",
                "promotions_processed": 0
            }
        
        logger.info(f"Processing {len(active_promotions)} active promotions")
        
        # Группируем промо по странам для оптимизации
        promotions_by_country = {}
        for promo in active_promotions:
            country = promo["bill_country"]
            if country not in promotions_by_country:
                promotions_by_country[country] = []
            promotions_by_country[country].append(promo)
        
        results = {
            "status": "success",
            "promotions_processed": len(active_promotions),
            "countries": list(promotions_by_country.keys()),
            "results_by_country": {}
        }
        
        # Обрабатываем каждую страну отдельно
        for country, country_promotions in promotions_by_country.items():
            logger.info(f"Processing {len(country_promotions)} promotions for country {country}")
            
            try:
                # Создаем процессор для батчевой обработки
                processor = BatchReceiptProcessor()
                
                # Обрабатываем чеки для этой страны
                country_result = processor.process_batch_receipts_complete(
                    report_id=report_id,
                    report_name=report_name,
                    countries=[country],
                    limit=limit_per_promo,  # Без лимита - обрабатываем все чеки
                    test_mode=False
                )
                
                results["results_by_country"][country] = {
                    "promotions_count": len(country_promotions),
                    "promotions": [p["gamification_id"] for p in country_promotions],
                    "processing_result": country_result
                }
                
                logger.info(f"Completed processing for country {country}: {country_result.get('status', 'unknown')}")
                
            except Exception as e:
                logger.error(f"Error processing country {country}: {e}")
                results["results_by_country"][country] = {
                    "promotions_count": len(country_promotions),
                    "promotions": [p["gamification_id"] for p in country_promotions],
                    "error": str(e)
                }
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error processing active promotions: {e}")
        return {
            "status": "error",
            "message": f"Failed to process active promotions: {str(e)}",
            "promotions_processed": 0
        }

def get_countries_from_data(target_date: str = None, limit: int = None) -> List[str]:
    """
    Получает список уникальных стран из данных для указанной даты
    
    is_success статусы:
    -5: server-side error
    -4: client-side error reported  
    -3: cancelled by user or aborted
    -2: rejected by moderator
    -1: rejected by automation (no retry)
    0: rejected by automation
    1: accepted by automation (НЕ используется для обработки)
    2: accepted by moderator ✅
    3: points proposition accepted (should change to -4 or 4)
    4: synchronized with CCA and points given ✅
    """
    try:
        # Формируем WHERE условие для даты
        date_filter = ""
        if target_date:
            date_filter = f"AND DATE(time_added_ts) = DATE('{target_date}')"
        
        query = f"""
        SELECT DISTINCT 
            gl.bill_country as country
        FROM `{PROJECT_ID}.{DATASET}.gamification_bills_flat` gbf
        LEFT JOIN `{PROJECT_ID}.{DATASET}.gamification_lookup` gl
            ON gbf.gamification_id = gl.gamification_id
            AND gl.is_active = TRUE
        WHERE gbf.is_success IN (2, 4)  -- ТОЛЬКО: 2=moderator, 4=synchronized (БЕЗ 1=automation!)
            AND gbf.is_finished = true
            AND gl.bill_country IS NOT NULL
            {date_filter}
        """
        
        query_job = bq_client.query(query, location=LOCATION)
        results = list(query_job.result())
        
        countries = [row.country for row in results if row.country]
        
        # Удаляем дубликаты и возвращаем
        unique_countries = list(set(countries))
        
        if not unique_countries:
            logger.warning(f"⚠️ No countries found in data for date: {target_date}")
            return []
            
        logger.info(f"🌍 Found countries in data: {unique_countries}")
        return unique_countries
        
    except Exception as e:
        logger.error(f"❌ Error getting countries from data: {e}")
        return []  # No fallback - return empty list on error

# Retry mechanism
try:
    import backoff
    BACKOFF_AVAILABLE = True
except ImportError:
    BACKOFF_AVAILABLE = False
    logger.warning("backoff library not available, retry mechanism disabled")

# Импорт JobState с fallback
try:
    from google.cloud.aiplatform_v1.types.job_state import JobState
except ImportError:
    try:
        from google.cloud.aiplatform.utils import JobState
    except ImportError:
        JobState = None
        logger.warning("JobState not available, using string comparison")

# Initialize clients
bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)
aiplatform.init(project=PROJECT_ID, location=LOCATION)

# Table names
PRODUCTS_TABLE = f"{PROJECT_ID}.{DATASET}.corrected_products"
VECTOR_READY_TABLE = f"{PROJECT_ID}.{DATASET}.products_vector_ready"
SHOP_TABLE = f"{PROJECT_ID}.{DATASET}.shop_directory"
ALL_DATA_TABLE = f"{PROJECT_ID}.{DATASET}.all_data"
GAMIFICATION_BILLS_FLAT = f"{PROJECT_ID}.{DATASET}.gamification_bills_flat"
FACT_SCAN_TABLE = f"{PROJECT_ID}.{DATASET}.fact_scan"
# Universal dictionaries for all countries
DICT_CITIES_ALL_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_all"
DICT_REGIONS_ALL_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_all"
# Legacy tables (kept for backward compatibility)
DICT_CITIES_PL_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_pl"
DICT_REGIONS_PL_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_pl"
DICT_CITIES_IT_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_it"
DICT_REGIONS_IT_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_it"
DICT_CITIES_GB_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_gb"
DICT_REGIONS_GB_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_gb"
DICT_CITIES_HU_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_hu"
DICT_REGIONS_HU_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_hu"
DICT_CITIES_PT_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_pt"
DICT_REGIONS_PT_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_pt"
DICT_CITIES_RO_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_ro"
DICT_REGIONS_RO_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_ro"
DICT_CITIES_FR_TABLE = f"{PROJECT_ID}.{DATASET}.dict_cities_fr"
DICT_REGIONS_FR_TABLE = f"{PROJECT_ID}.{DATASET}.dict_regions_fr"

SUPPORTED_COUNTRIES = ["IT", "PL", "DE", "FR", "GB", "HU", "PT", "RO", "RS", "ES", "AT", "EE", "GR", "IE", "NL", "BE", "FI", "SK", "SI", "LV", "LT", "LU", "MT", "CY"]

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------
def generate_report_id(report_name: Optional[str] = None) -> str:
    """Generate a unique report identifier."""
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    if report_name:
        clean = re.sub(r"[^A-Za-z0-9_-]", "_", report_name)[:50]
        return f"RPT_{ts}_{clean}_{short}"
    return f"RPT_{ts}_{short}"

def _now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.utcnow()

def _delete_existing_promo_fact_scan_data(gamification_id: str) -> None:
    """
    Удаляет существующие данные для конкретного gamification_id из fact_scan таблицы.
    """
    try:
        delete_sql = f"""
        DELETE FROM `{FACT_SCAN_TABLE}`
        WHERE gamification_id = '{gamification_id}'
        """
        job = bq_client.query(delete_sql, location=LOCATION)
        result = job.result()
        
        deleted_rows = job.num_dml_affected_rows if hasattr(job, 'num_dml_affected_rows') else 0
        logger.info(f"🗑️ Deleted {deleted_rows} existing rows from fact_scan for gamification_id: {gamification_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to delete existing fact_scan data for gamification_id {gamification_id}: {e}")
        raise

def _insert_rows(table_id: str, rows: List[Dict[str, Any]]) -> None:
    """Insert multiple rows into a BigQuery table with error handling."""
    if not rows:
        return
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insertion errors for {table_id}: {errors}")
    logger.info("Inserted %s rows into %s", len(rows), table_id)

def _storage_write_api_load(table_id: str, rows: List[Dict[str, Any]], report_id: str, report_name: str) -> None:
    """Load data to BigQuery using optimized batch load (5-10x faster than streaming)."""
    if not rows:
        return
    
    start_time = time.time()
    logger.info("🚀 STARTING FAST BATCH LOAD: %s rows to %s", len(rows), table_id)
    
    # Оптимизация: батчинг для больших объемов
    BATCH_SIZE = 50000  # Максимум 50K строк за раз для оптимальной скорости
    
    if len(rows) <= BATCH_SIZE:
        # Малый объем - загружаем сразу
        logger.info("📊 SMALL DATASET: Loading %s rows in single batch", len(rows))
        _load_single_batch(table_id, rows, report_id, report_name)
    else:
        # Большой объем - разбиваем на батчи
        logger.info("📊 LARGE DATASET: %s rows, splitting into batches of %s", len(rows), BATCH_SIZE)
        
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info("🔄 PROCESSING BATCH %s/%s (%s rows)", batch_num, total_batches, len(batch))
            _load_single_batch(table_id, batch, report_id, f"{report_name}_batch_{batch_num}")
    
    elapsed_time = time.time() - start_time
    rows_per_sec = len(rows) / elapsed_time if elapsed_time > 0 else 0
    logger.info("✅ FAST BATCH LOAD COMPLETED: %s rows in %.2f seconds (%.0f rows/sec)", 
                len(rows), elapsed_time, rows_per_sec)

def _load_single_batch(table_id: str, rows: List[Dict[str, Any]], report_id: str, report_name: str) -> None:
    """Load a single batch to BigQuery using optimized batch load."""
    try:
        # Создаем временный файл в Cloud Storage
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Создаем уникальное имя файла
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")  # Микросекунды для уникальности
        filename = f"fast_load/{report_id}_{timestamp}.json"
        blob = bucket.blob(filename)
        
        # Конвертируем данные в JSON Lines формат (оптимизированно)
        json_lines = [json.dumps(row, ensure_ascii=False, default=str) for row in rows]
        
        # Загружаем в Cloud Storage одной операцией
        blob.upload_from_string('\n'.join(json_lines), content_type='application/json')
        logger.info("Uploaded %s rows to Cloud Storage: %s", len(rows), filename)
        
        # Настраиваем job configuration для batch load
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,  # Используем существующую схему
            ignore_unknown_values=True,
            max_bad_records=10,
            # Оптимизации для скорости
            job_timeout_ms=300000,  # 5 минут timeout
            use_avro_logical_types=True
        )
        
        # Создаем URI для Cloud Storage файла
        gcs_uri = f"gs://{BUCKET_NAME}/{filename}"
        
        # Запускаем batch load job
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        
        # Ждем завершения job
        load_job.result()
        if load_job.errors:
            logger.error("Fast batch load errors: %s", load_job.errors)
            raise RuntimeError(f"Fast batch load failed for {table_id}: {load_job.errors}")
        
        logger.info("Successfully fast loaded %s rows into %s", len(rows), table_id)
        
        # Удаляем временный файл из Cloud Storage
        try:
            blob.delete()
            logger.info("Deleted temporary file: %s", filename)
        except Exception as e:
            logger.warning("Failed to delete temporary file %s: %s", filename, e)
        
    except Exception as e:
        logger.error("❌ FAST BATCH LOAD FAILED: %s", e)
        logger.error("📊 FALLING BACK TO STANDARD BATCH LOAD for table: %s", table_id)
        # Fallback to standard batch load method
        try:
            _batch_load_to_bigquery(table_id, rows, report_id, report_name)
            logger.info("✅ STANDARD BATCH LOAD SUCCESSFUL")
        except Exception as fallback_error:
            logger.error("❌ STANDARD BATCH LOAD ALSO FAILED: %s", fallback_error)
            logger.error("📊 FALLING BACK TO STREAMING INSERTS for table: %s", table_id)
            try:
                _insert_rows(table_id, rows)
                logger.info("✅ STREAMING INSERTS SUCCESSFUL")
            except Exception as streaming_error:
                logger.error("❌ ALL LOAD METHODS FAILED for table %s: %s", table_id, streaming_error)
                raise RuntimeError(f"All BigQuery load methods failed for {table_id}: {streaming_error}")

def _batch_load_to_bigquery(table_id: str, rows: List[Dict[str, Any]], 
                           report_id: str, report_name: str) -> None:
    """Load data to BigQuery using batch load (Cloud Storage) instead of streaming insert."""
    if not rows:
        return
    
    # Создаем временный файл в Cloud Storage
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Создаем уникальное имя файла
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"batch_load/{report_id}_{timestamp}.json"
    blob = bucket.blob(filename)
    
    # Конвертируем данные в JSON Lines формат
    json_lines = []
    for row in rows:
        json_lines.append(json.dumps(row, ensure_ascii=False, default=str))
    
    # Загружаем в Cloud Storage
    blob.upload_from_string('\n'.join(json_lines), content_type='application/json')
    logger.info("Uploaded %s rows to Cloud Storage: %s", len(rows), filename)
    
    # Настраиваем job configuration для batch load
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,  # Используем существующую схему
        ignore_unknown_values=True,
        max_bad_records=10
    )
    
    # Создаем URI для Cloud Storage файла
    gcs_uri = f"gs://{BUCKET_NAME}/{filename}"
    
    # Запускаем batch load job
    load_job = bq_client.load_table_from_uri(
        gcs_uri, 
        table_id, 
        job_config=job_config
    )
    
    # Ждем завершения job
    load_job.result()
    
    if load_job.errors:
        logger.error("Batch load errors: %s", load_job.errors)
        raise RuntimeError(f"Batch load failed for {table_id}: {load_job.errors}")
    
    logger.info("Successfully batch loaded %s rows into %s", len(rows), table_id)
    
    # Удаляем временный файл из Cloud Storage
    try:
        blob.delete()
        logger.info("Deleted temporary file: %s", filename)
    except Exception as e:
        logger.warning("Failed to delete temporary file %s: %s", filename, e)


def create_combined_correction_prompt(api_id: str,
                                     products_json: list,
                                     total_price: Optional[float] = None,
                                     country_code: Optional[str] = None,
                                     nip: Optional[str] = None,
                                     shopnetwork: Optional[str] = None,
                                     raw_address: Optional[str] = None) -> str:
    """Build a combined prompt for product correction AND city determination."""
    current_total = 0.0
    lines = []
    for i, p in enumerate(products_json, 1):
        name = p.get("name", "UNKNOWN")
        qty = float(p.get("number") or p.get("qty") or 1)
        ps = float(p.get("price_single") or p.get("price") or 0)
        pt = float(p.get("price_total") or (qty * ps))
        current_total += pt
        lines.append(f"{i}. {name} | Qty: {qty} × {ps} = {pt}")

    currency = "PLN" if (country_code or "IT") == "PL" else "EUR"
    total_block = ""
    if total_price and total_price > 0:
        difference = abs(total_price - current_total)
        total_block = (
            f"\nRECEIPT TOTAL: {currency}{total_price:.2f}\n"
            f"CURRENT SUM: {currency}{current_total:.2f}\n"
            f"DIFFERENCE: {currency}{difference:.2f}"
        )

    total_receipt_str = f"{total_price:.2f}" if total_price and total_price > 0 else "null"

    # Собираем уникальные продукты для анализа города
    unique_products = []
    seen = set()
    for p in products_json[:15]:
        name = (p.get('name') or '').upper()
        if name and len(name) > 2 and name not in seen:
            unique_products.append(name)
            seen.add(name)
    products_text = "\n".join(f"- {p}" for p in unique_products[:10])

    # Извлекаем адреса, ZIP код и провинцию для промпта
    shop_address, headquarters_address = extract_addresses_from_receipt(raw_address) if raw_address else (None, None)
    
    # Извлекаем zip_code из адреса магазина (приоритет) или из общего адреса
    zip_code = None
    if shop_address:
        zip_code = extract_zip_code(shop_address, country_code or "PL")
    if not zip_code and raw_address:
        zip_code = extract_zip_code(raw_address, country_code or "PL")
    
    # Извлекаем код провинции для Италии
    province_code = None
    if country_code == "IT":
        if shop_address:
            province_code = extract_province_code(shop_address, country_code)
        if not province_code and raw_address:
            province_code = extract_province_code(raw_address, country_code)
        logger.info(f"Extracted for IT in batch: ZIP={zip_code}, Province={province_code} from address={raw_address}")

    # Разные промпты для разных стран
    if country_code == "PL":
        prompt = f"""
You are an expert in Polish retail geography and receipt data correction.

CRITICAL TASK: Fix product names AND prices, AND determine the city from receipt data.

RECEIPT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Sample products from receipt:
{products_text}

RECEIPT ITEMS:
{chr(10).join(lines)}{total_block}

YOUR TASKS:
1. FIX PRODUCT NAMES (IMPORTANT - KEEP POLISH LANGUAGE):
   - Expand abbreviations: 'SOK JABŁKOWY' → 'Sok Jabłkowy'
   - Fix typos: 'OGORKI' → 'Ogórki', 'BULKI' → 'Bułki'
   - Standardize units: '1L' → '1 L', '500G' → '500 g'
   - Fix OCR errors: 'M0KA' → 'Mąka', 'SZYNKA' → 'Szynka'
   - Keep Polish names: 'Mąka Basia' stays 'Mąka Basia' (NOT 'Basia Flour')
   - Keep Polish names: 'Szynka' stays 'Szynka' (NOT 'Ham')
   - Use proper Polish capitalization: 'SOK JABŁKOWY' → 'Sok Jabłkowy'
   - Fix Polish diacritics: 'ą', 'ć', 'ę', 'ł', 'ń', 'ó', 'ś', 'ź', 'ż'
   - Common Polish products: Chleb, Mleko, Masło, Ser, Wędlina, Owoce, Warzywa

2. FIX PRICES (CRITICAL):
   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)
   - Check if quantity × unit price = total price for each item
   - If not, adjust the unit price or quantity to make it consistent
   - All corrected prices must sum up to the receipt total (if provided)
   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4

3. DETERMINE CITY (CRITICAL - USE POLISH NAMES):
   - ALWAYS use Polish city names: Warszawa (not Warsaw), Kraków (not Cracow), Łódź (not Lodz)
   - PRIORITY #1: Use ZIP code if available ({zip_code or 'none'}) ⭐
   - PRIORITY #2: Use NIP first digits for regional hints:
     * 10-19: Mazowieckie (Warszawa area)
     * 20-29: Małopolskie (Kraków area)
     * 30-39: Lubelskie, Podkarpackie
     * 40-49: Śląskie (Katowice area)
     * 50-59: Dolnośląskie (Wrocław area)
     * 60-69: Wielkopolskie (Poznań area)
     * 70-79: Zachodniopomorskie (Szczecin area)
     * 80-89: Pomorskie (Gdańsk area)
     * 90-99: Warmińsko-mazurskie, Podlaskie
   - Use retail chain geographic presence knowledge
   - Analyze product types for regional preferences
   - Provide population estimates based on city size knowledge
   - Major Polish cities: Warszawa (~1.8M), Kraków (~780K), Łódź (~680K), Wrocław (~640K), Poznań (~540K), Gdańsk (~470K)

POLISH RETAIL CHAINS KNOWLEDGE:
- BIEDRONKA: Strong in all major cities, especially Warszawa, Kraków, Wrocław
- ŻABKA: Especially strong in Warszawa, Kraków, Wrocław, Poznań
- KAUFLAND: Major cities, especially Wrocław, Poznań, Warszawa
- LIDL: All major cities, strong presence everywhere
- CARREFOUR: Warszawa, Kraków, Poznań, Wrocław
- TESCO: Major cities, especially Warszawa, Kraków
- ALDI: Growing presence in major cities
- NETTO: Strong in northern regions

POLISH REGIONS AND MAJOR CITIES (USE STANDARD FORMS):
- MAZOWIECKIE: Warszawa, Radom, Płock, Siedlce, Ostrołęka
- MAŁOPOLSKIE: Kraków, Tarnów, Nowy Sącz, Oświęcim
- ŚLĄSKIE: Katowice, Częstochowa, Sosnowiec, Gliwice, Zabrze, Bytom
- WIELKOPOLSKIE: Poznań, Kalisz, Konin, Piła
- DOLNOŚLĄSKIE: Wrocław, Wałbrzych, Legnica, Jelenia Góra
- ŁÓDZKIE: Łódź, Piotrków Trybunalski, Pabianice, Tomaszów Mazowiecki
- POMORSKIE: Gdańsk, Gdynia, Sopot, Słupsk, Tczew
- ZACHODNIOPOMORSKIE: Szczecin, Koszalin, Stargard, Kołobrzeg
- LUBELSKIE: Lublin, Chełm, Zamość, Biała Podlaska
- PODKARPACKIE: Rzeszów, Przemyśl, Stalowa Wola, Mielec
- PODLASKIE: Białystok, Suwałki, Łomża, Augustów
- WARMIŃSKO-MAZURSKIE: Olsztyn, Elbląg, Ełk, Ostróda
- KUJAWSKO-POMORSKIE: Bydgoszcz, Toruń, Włocławek, Grudziądz
- ŚWIĘTOKRZYSKIE: Kielce, Ostrowiec Świętokrzyski, Starachowice
- LUBUSKIE: Zielona Góra, Gorzów Wielkopolski, Żary, Nowa Sól
- OPOLSKIE: Opole, Kędzierzyn-Koźle, Nysa, Brzeg

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "total_receipt": {total_receipt_str},
  "total_calculated": <sum of all corrected price_total>,
  "corrections_made": true/false,
  "products": [
    {{
      "name_original": "original name from receipt",
      "name_corrected": "fixed name",
      "quantity": integer,
      "price_single_original": number,
      "price_single_corrected": number,
      "price_total": number,
      "price_correction_reason": "OCR error: 3→8" or null
    }}
  ],
  "city_analysis": {{
    "city": "city name or UNKNOWN (use standard forms: Warszawa, Kraków, Gdańsk)",
    "region": "region name or UNKNOWN",
    "zip_code": "extracted or matched zip code or null",
    "city_population": integer,
    "match_method": "zip_code_match" | "address_match" | "name_match" | "nip_hint",
    "confidence": "HIGH/MEDIUM/LOW",
    "evidence": "brief explanation of how you identified the location, including which matching method was used"
  }}
}}

IMPORTANT: Ensure all arithmetic is correct, total matches, product names stay in Polish, and provide best city estimate!

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON.
"""
    elif country_code == "IT":
        prompt = f"""
You are an expert in Italian retail geography and receipt data correction.

CRITICAL TASK: Fix product names AND prices, AND determine the city from receipt data.

CRITICAL MATCHING PRIORITY for city determination (use in this exact order):
1. ZIP CODE MATCH (HIGHEST PRIORITY) ⭐⭐⭐
   - If zip_code is provided and not 'none', use it FIRST
   - Example: zip_code "80126" → NAPOLI (Campania)
   - Example: zip_code "00100" → ROMA (Lazio)
   
2. PROVINCE/REGION CODE MATCH ⭐⭐
   - If address contains (RM), (NA), (MI), etc. → filter by province
   - Example: "(RM)" → Roma province → filter cities in Lazio
   
3. STREET ADDRESS CONTEXT ⭐
   - Use street name to disambiguate between similar cities
   
4. NORMALIZED CITY NAME SIMILARITY
   - Use only if ZIP/province not available
   
5. NIP REGION HINTS
   - Use only as last resort

RECEIPT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Shop Address (if separated): {shop_address or 'none'}
- Headquarters Address (if separated): {headquarters_address or 'none'}
- Extracted ZIP Code: {zip_code or 'none'} {'⭐ USE THIS FIRST!' if zip_code and zip_code != 'none' else ''}
- Extracted Province Code: {province_code or 'none'} {'⭐ USE THIS SECOND!' if province_code and province_code != 'none' else ''}
- Sample products from receipt:
{products_text}

RECEIPT ITEMS:
{chr(10).join(lines)}{total_block}

YOUR TASKS:
1. FIX PRODUCT NAMES:
   - Expand abbreviations: "COCA" → "COCA COLA"
   - Fix typos: "PIANATA" → "PIADINA"
   - Standardize units: "1,5LT" → "1.5L"
   - Fix OCR errors: "C0CA C0LA" → "COCA COLA"

2. FIX PRICES (CRITICAL):
   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)
   - Check if quantity × unit price = total price for each item
   - If not, adjust the unit price or quantity to make it consistent
   - All corrected prices must sum up to the receipt total (if provided)
   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4

3. DETERMINE CITY (IMPORTANT):
   - PRIORITY #1: Use ZIP code if available ({zip_code or 'none'}) ⭐
   - PRIORITY #2: Use Province code if available ({province_code or 'none'}) ⭐
   - PRIORITY #3: Use NIP first digits for regional hints (Northern Italy: 01-19, Central: 20-59, Southern: 60-99)
   - Consider all Italian cities and towns (population 10,000+)
   - Use retail chain geographic presence knowledge
   - Analyze product types for regional preferences
   - Provide population estimates based on city size knowledge

ITALIAN RETAIL CHAINS KNOWLEDGE:
- CONAD: Cooperative stores, strong in Emilia-Romagna, Tuscany
- CARREFOUR: Major cities, especially Milano, Roma, Napoli
- ESSELUNGA: Lombardia, Tuscany, strong in Milano, Firenze
- COOP: Pan-Italian, especially strong in northern regions
- LIDL: All major cities, strong presence everywhere
- EUROSPIN: Very cheap, strong in southern regions
- PAM: Northern Italy, especially Lombardia
- DESPAR: Veneto, Lombardia regions
- PENNY MARKET: Growing presence, especially in central Italy
- MD DISCOUNT: Southern Italy, especially Campania, Puglia
- SELEX: Central Italy
- FAMILA: Veneto, Friuli-Venezia Giulia
- BENNET: Northern Italy, especially Lombardia
- CRAI: Southern Italy, especially Sicily, Calabria

ITALIAN REGIONS AND MAJOR CITIES (USE STANDARD FORMS):
- LAZIO: Roma, Latina, Frosinone, Viterbo, Rieti
- LOMBARDIA: Milano, Bergamo, Brescia, Monza, Como, Varese, Pavia, Cremona
- CAMPANIA: Napoli, Salerno, Caserta, Avellino, Benevento
- PIEMONTE: Torino, Alessandria, Novara, Cuneo, Asti
- SICILIA: Palermo, Catania, Messina, Siracusa, Agrigento
- VENETO: Venezia, Verona, Padova, Vicenza, Treviso
- EMILIA-ROMAGNA: Bologna, Modena, Parma, Reggio Emilia, Ravenna, Ferrara
- TOSCANA: Firenze, Pisa, Livorno, Prato, Siena, Arezzo
- PUGLIA: Bari, Taranto, Foggia, Lecce, Brindisi
- LIGURIA: Genova, La Spezia, Savona, Imperia
- CALABRIA: Reggio Calabria, Catanzaro, Cosenza
- MARCHE: Ancona, Pesaro, Macerata, Ascoli Piceno
- ABRUZZO: L'Aquila, Pescara, Chieti, Teramo
- UMBRIA: Perugia, Terni

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "total_receipt": {total_receipt_str},
  "total_calculated": <sum of all corrected price_total>,
  "corrections_made": true/false,
  "products": [
    {{
      "name_original": "original name from receipt",
      "name_corrected": "fixed name",
      "quantity": integer,
      "price_single_original": number,
      "price_single_corrected": number,
      "price_total": number,
      "price_correction_reason": "OCR error: 3→8" or null
    }}
  ],
  "city_analysis": {{
    "city": "city name or UNKNOWN (use standard forms: Roma, Milano, Napoli)",
    "region": "region name or UNKNOWN",
    "zip_code": "extracted or matched zip code or null",
    "province_code": "extracted province code (RM, NA, MI, etc.) or null",
    "city_population": integer,
    "match_method": "zip_code_match" | "province_match" | "address_match" | "name_match" | "nip_hint",
    "confidence": "HIGH/MEDIUM/LOW",
    "evidence": "brief explanation of how you identified the location, including which matching method was used"
  }}
}}

IMPORTANT: Ensure all arithmetic is correct, total matches, and provide best city estimate!

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON.
"""
    elif country_code == "DE":
        prompt = f"""
You are an expert in German retail geography and receipt data correction.

CRITICAL TASK: Fix product names AND prices, AND determine the city from receipt data.

RECEIPT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Sample products from receipt:
{products_text}

RECEIPT ITEMS:
{chr(10).join(lines)}{total_block}

YOUR TASKS:
1. FIX PRODUCT NAMES (IMPORTANT - KEEP GERMAN LANGUAGE):
   - Expand abbreviations: 'BROT' → 'Brot', 'MILCH' → 'Milch'
   - Fix typos: 'WURST' → 'Wurst', 'KASE' → 'Käse'
   - Standardize units: '1L' → '1 L', '500G' → '500 g'
   - Fix OCR errors: 'BROT' → 'Brot', 'BUTTER' → 'Butter'
   - Keep German names: 'Brot' stays 'Brot' (NOT 'Bread')
   - Keep German names: 'Wurst' stays 'Wurst' (NOT 'Sausage')
   - Use proper German capitalization: 'BROT' → 'Brot'
   - Fix German diacritics: 'ä', 'ö', 'ü', 'ß'
   - Common German products: Brot, Milch, Butter, Käse, Wurst, Obst, Gemüse

2. FIX PRICES (CRITICAL):
   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)
   - Check if quantity × unit price = total price for each item
   - If not, adjust the unit price or quantity to make it consistent
   - All corrected prices must sum up to the receipt total (if provided)
   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4

3. DETERMINE CITY (IMPORTANT):
   - Use retail chain geographic presence knowledge
   - Consider all German cities and towns (population 50,000+)
   - Analyze product types for regional preferences
   - Provide population estimates based on city size knowledge

GERMAN RETAIL CHAINS KNOWLEDGE:
- ALDI: Strong nationwide, especially in rural areas
- LIDL: Pan-German presence, strong in all regions
- REWE: Strong in western and southern Germany
- EDEKA: Strong regional presence, especially in northern Germany
- KAUFLAND: Major cities, especially in eastern Germany
- REAL: Large cities, especially in urban centers
- NETTO: Strong in northern and eastern Germany
- PENNY: Growing presence, especially in urban areas
- NORMA: Southern Germany, especially Bavaria
- TEGUT: Hesse, Thuringia regions
- SPAR: Various regions, especially rural areas

GERMAN MAJOR CITIES (USE STANDARD FORMS):
- NORTH RHINE-WESTPHALIA: Köln, Düsseldorf, Dortmund, Essen, Duisburg, Bochum, Wuppertal, Bielefeld, Bonn, Münster
- BAVARIA: München, Nürnberg, Augsburg, Regensburg, Ingolstadt, Würzburg, Fürth, Erlangen, Bayreuth
- BADEN-WÜRTTEMBERG: Stuttgart, Mannheim, Karlsruhe, Freiburg, Heidelberg, Heilbronn, Ulm, Pforzheim, Reutlingen
- LOWER SAXONY: Hannover, Braunschweig, Osnabrück, Oldenburg, Göttingen, Wolfsburg, Hildesheim, Salzgitter
- HESSE: Frankfurt, Wiesbaden, Kassel, Darmstadt, Offenbach, Hanau, Marburg, Gießen
- SAXONY: Dresden, Leipzig, Chemnitz, Zwickau, Plauen, Görlitz, Freiberg, Bautzen
- RHINELAND-PALATINATE: Mainz, Ludwigshafen, Koblenz, Trier, Kaiserslautern, Worms, Neuwied
- BERLIN: Berlin (city-state)
- HAMBURG: Hamburg (city-state)
- BREMEN: Bremen, Bremerhaven (city-state)
- SCHLESWIG-HOLSTEIN: Kiel, Lübeck, Flensburg, Neumünster, Norderstedt
- MECKLENBURG-WESTERN POMERANIA: Schwerin, Rostock, Neubrandenburg, Stralsund, Greifswald
- BRANDENBURG: Potsdam, Cottbus, Brandenburg, Frankfurt (Oder), Oranienburg
- SAXONY-ANHALT: Magdeburg, Halle, Dessau-Roßlau, Wittenberg, Stendal
- THURINGIA: Erfurt, Jena, Gera, Weimar, Gotha, Eisenach, Nordhausen

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "total_receipt": {total_receipt_str},
  "total_calculated": <sum of all corrected price_total>,
  "corrections_made": true/false,
  "products": [
    {{
      "name_original": "original name from receipt",
      "name_corrected": "fixed name",
      "quantity": integer,
      "price_single_original": number,
      "price_single_corrected": number,
      "price_total": number,
      "price_correction_reason": "OCR error: 3→8" or null
    }}
  ],
  "city_analysis": {{
    "city": "city name or UNKNOWN (use standard forms: Berlin, München, Hamburg)",
    "region": "region/province name or UNKNOWN",
    "city_population": integer,
    "confidence": "HIGH/MEDIUM/LOW",
    "evidence": "brief explanation of how you identified the location"
  }}
}}

IMPORTANT: Ensure all arithmetic is correct, total matches, product names stay in German, and provide best city estimate!

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON.
"""
    elif country_code == "FR":
        prompt = f"""
You are an expert in French retail geography and receipt data correction.

CRITICAL TASK: Fix product names AND prices, AND determine the city from receipt data.

RECEIPT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Sample products from receipt:
{products_text}

RECEIPT ITEMS:
{chr(10).join(lines)}{total_block}

YOUR TASKS:
1. FIX PRODUCT NAMES (IMPORTANT - KEEP FRENCH LANGUAGE):
   - Expand abbreviations: 'PAIN' → 'Pain', 'LAIT' → 'Lait'
   - Fix typos: 'FROMAGE' → 'Fromage', 'JAMBON' → 'Jambon'
   - Standardize units: '1L' → '1 L', '500G' → '500 g'
   - Fix OCR errors: 'PAIN' → 'Pain', 'BEURRE' → 'Beurre'
   - Keep French names: 'Pain' stays 'Pain' (NOT 'Bread')
   - Keep French names: 'Jambon' stays 'Jambon' (NOT 'Ham')
   - Use proper French capitalization: 'PAIN' → 'Pain'
   - Fix French diacritics: 'à', 'â', 'ä', 'ç', 'é', 'è', 'ê', 'ë', 'î', 'ï', 'ô', 'ö', 'ù', 'û', 'ü', 'ÿ'
   - Common French products: Pain, Lait, Beurre, Fromage, Jambon, Fruits, Légumes

2. FIX PRICES (CRITICAL):
   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)
   - Check if quantity × unit price = total price for each item
   - If not, adjust the unit price or quantity to make it consistent
   - All corrected prices must sum up to the receipt total (if provided)
   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4

3. DETERMINE CITY (IMPORTANT):
   - Use retail chain geographic presence knowledge
   - Consider all French cities and towns (population 50,000+)
   - Analyze product types for regional preferences
   - Provide population estimates based on city size knowledge

FRENCH RETAIL CHAINS KNOWLEDGE:
- CARREFOUR: Pan-French presence, strong in major cities
- LECLERC: Strong nationwide, especially in rural and suburban areas
- AUCHAN: Major cities, especially in northern and eastern France
- CASINO: Southern France, especially in Provence and Languedoc
- INTERMARCHE: Strong in rural areas and small towns
- SUPER U / SYSTEME U: Cooperative stores, strong in western France
- LIDL: Growing presence, especially in urban areas
- ALDI: Growing presence, especially in eastern France
- MONOPRIX: Urban centers, especially Paris and major cities
- FRANPRIX: Paris and urban centers
- SPAR: Various regions, especially rural areas

FRENCH MAJOR CITIES (USE STANDARD FORMS):
- ILE-DE-FRANCE: Paris, Boulogne-Billancourt, Saint-Denis, Argenteuil, Montreuil, Créteil, Nanterre, Vitry-sur-Seine, Courbevoie, Versailles
- PROVENCE-ALPES-COTE D'AZUR: Marseille, Nice, Toulon, Nîmes, Aix-en-Provence, Montpellier, Avignon, Cannes, Antibes, La Seyne-sur-Mer
- AUVERGNE-RHONE-ALPES: Lyon, Saint-Étienne, Grenoble, Villeurbanne, Clermont-Ferrand, Annecy, Valence, Chambéry, Bourg-en-Bresse, Saint-Priest
- HAUTS-DE-FRANCE: Lille, Amiens, Roubaix, Tourcoing, Dunkerque, Calais, Villeneuve-d'Ascq, Saint-Quentin, Beauvais, Abbeville
- GRAND EST: Strasbourg, Mulhouse, Reims, Metz, Colmar, Troyes, Charleville-Mézières, Châlons-en-Champagne, Épinal, Haguenau
- OCCITANIE: Toulouse, Montpellier, Nîmes, Perpignan, Béziers, Montauban, Narbonne, Albi, Carcassonne, Sète
- NOUVELLE-AQUITAINE: Bordeaux, Limoges, Poitiers, La Rochelle, Angoulême, Agen, Périgueux, Bayonne, Pau, Mont-de-Marsan
- PAYS DE LA LOIRE: Nantes, Le Mans, Angers, Saint-Nazaire, Cholet, La Roche-sur-Yon, Laval, Saumur, Saint-Herblain, Orvault
- BRETAGNE: Rennes, Brest, Quimper, Lorient, Vannes, Saint-Malo, Fougères, Saint-Brieuc, Lannion, Concarneau
- NORMANDIE: Rouen, Le Havre, Caen, Cherbourg, Évreux, Dieppe, Saint-Étienne-du-Rouvray, Sotteville-lès-Rouen, Le Grand-Quevilly, Vernon
- CENTRE-VAL DE LOIRE: Tours, Orléans, Blois, Châteauroux, Bourges, Chartres, Dreux, Vierzon, Olivet, Saint-Jean-de-Braye
- BOURGOGNE-FRANCHE-COMTE: Dijon, Besançon, Belfort, Chalon-sur-Saône, Auxerre, Nevers, Mâcon, Sens, Montbéliard, Beaune
- CORSE: Ajaccio, Bastia, Porto-Vecchio, Corte, Sartène, Calvi, L'Île-Rousse, Propriano, Bonifacio, Aléria

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "total_receipt": {total_receipt_str},
  "total_calculated": <sum of all corrected price_total>,
  "corrections_made": true/false,
  "products": [
    {{
      "name_original": "original name from receipt",
      "name_corrected": "fixed name",
      "quantity": integer,
      "price_single_original": number,
      "price_single_corrected": number,
      "price_total": number,
      "price_correction_reason": "OCR error: 3→8" or null
    }}
  ],
  "city_analysis": {{
    "city": "city name or UNKNOWN (use standard forms: Paris, Lyon, Marseille)",
    "region": "region name or UNKNOWN",
    "city_population": integer,
    "confidence": "HIGH/MEDIUM/LOW",
    "evidence": "brief explanation of how you identified the location"
  }}
}}

IMPORTANT: Ensure all arithmetic is correct, total matches, product names stay in French, and provide best city estimate!

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON.
"""
    elif country_code == "GB":
        # Use universal prompt for GB
        pass
    elif country_code == "HU":
        # Use universal prompt for HU
        pass
    elif country_code == "PT":
        # Use universal prompt for PT
        pass
    elif country_code == "RO":
        # Use universal prompt for RO
        pass
    # Old prompts removed - using universal prompt for all countries
    if False:
        prompt = (
            "You are an expert in fixing Hungarian receipt data with OCR errors.\n\n"
            "CRITICAL TASK: Fix product names AND correct prices that have OCR errors.\n"
            "The receipt total is usually correct, but individual prices often have OCR mistakes.\n\n"
            "RECEIPT DATA:\n"
            f"API_ID: {api_id}\n"
            + "\n".join(lines)
            + total_block
            + "\n\nYOUR TASKS:\n"
              "1. FIX PRODUCT NAMES (IMPORTANT - KEEP HUNGARIAN LANGUAGE):\n"
              "   - Expand abbreviations: 'KENYÉR' → 'Kenyér', 'TEJ' → 'Tej'\n"
              "   - Fix typos: 'VAGON' → 'Vajon', 'SAJT' → 'Sajt'\n"
              "   - Standardize units: '1L' → '1 L', '500G' → '500 g'\n"
              "   - Fix OCR errors: 'KENYÉR' → 'Kenyér', 'VAGON' → 'Vajon'\n"
              "   - Keep Hungarian names: 'Kenyér' stays 'Kenyér' (NOT 'Bread')\n"
              "   - Use proper Hungarian capitalization: 'KENYÉR' → 'Kenyér'\n"
              "   - Fix Hungarian diacritics: 'á', 'é', 'í', 'ó', 'ö', 'ő', 'ú', 'ü', 'ű'\n"
              "   - Common Hungarian products: Kenyér, Tej, Vaj, Sajt, Sonka, Gyümölcs, Zöldség\n\n"
              "2. FIX PRICES (CRITICAL):\n"
              "   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)\n"
              "   - Check if quantity × unit price = total price for each item\n"
              "   - If not, adjust the unit price or quantity to make it consistent\n"
              "   - All corrected prices must sum up to the receipt total (if provided)\n"
              "   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4\n\n"
              "3. VALIDATION RULES:\n"
              "   - Each line: quantity × price_single MUST equal price_total\n"
              "   - Sum of all price_total MUST equal receipt total (if provided)\n"
              "   - Prices must be realistic for Hungarian supermarkets (100 - 50000 HUF per item)\n"
              "   - KEEP PRODUCT NAMES IN HUNGARIAN - DO NOT TRANSLATE TO ENGLISH\n\n"
              "HUNGARIAN PRODUCT EXAMPLES:\n"
              "   - 'Kenyér 500g' → 'Kenyér 500 g' (NOT 'Bread 500 g')\n"
              "   - 'Sonka 200g' → 'Sonka 200 g' (NOT 'Ham 200 g')\n"
              "   - 'Tej 1L' → 'Tej 1 L'\n"
              "   - 'Sajt 300g' → 'Sajt 300 g'\n\n"
              "OUTPUT FORMAT (strict JSON):\n"
            + "JSON schema not needed - using universal prompt"
            + "\n\nIMPORTANT: Ensure all arithmetic is correct, total matches, and product names stay in Hungarian!"
        )
    elif country_code == "PT":
        # Portuguese prompt
        prompt = (
            "You are an expert in fixing Portuguese receipt data with OCR errors.\n\n"
            "CRITICAL TASK: Fix product names AND correct prices that have OCR errors.\n"
            "The receipt total is usually correct, but individual prices often have OCR mistakes.\n\n"
            "RECEIPT DATA:\n"
            f"API_ID: {api_id}\n"
            + "\n".join(lines)
            + total_block
            + "\n\nYOUR TASKS:\n"
              "1. FIX PRODUCT NAMES (IMPORTANT - KEEP PORTUGUESE LANGUAGE):\n"
              "   - Expand abbreviations: 'PÃO' → 'Pão', 'LEITE' → 'Leite'\n"
              "   - Fix typos: 'MANTEIGA' → 'Manteiga', 'QUEIJO' → 'Queijo'\n"
              "   - Standardize units: '1L' → '1 L', '500G' → '500 g'\n"
              "   - Fix OCR errors: 'PÃO' → 'Pão', 'MANTEIGA' → 'Manteiga'\n"
              "   - Keep Portuguese names: 'Pão' stays 'Pão' (NOT 'Bread')\n"
              "   - Use proper Portuguese capitalization: 'PÃO' → 'Pão'\n"
              "   - Fix Portuguese diacritics: 'á', 'à', 'â', 'ã', 'é', 'ê', 'í', 'ó', 'ô', 'õ', 'ú', 'ç'\n"
              "   - Common Portuguese products: Pão, Leite, Manteiga, Queijo, Fiambre, Fruta, Legumes\n\n"
              "2. FIX PRICES (CRITICAL):\n"
              "   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)\n"
              "   - Check if quantity × unit price = total price for each item\n"
              "   - If not, adjust the unit price or quantity to make it consistent\n"
              "   - All corrected prices must sum up to the receipt total (if provided)\n"
              "   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4\n\n"
              "3. VALIDATION RULES:\n"
              "   - Each line: quantity × price_single MUST equal price_total\n"
              "   - Sum of all price_total MUST equal receipt total (if provided)\n"
              "   - Prices must be realistic for Portuguese supermarkets (€0.10 - €50 per item)\n"
              "   - KEEP PRODUCT NAMES IN PORTUGUESE - DO NOT TRANSLATE TO ENGLISH\n\n"
              "PORTUGUESE PRODUCT EXAMPLES:\n"
              "   - 'Pão 500g' → 'Pão 500 g' (NOT 'Bread 500 g')\n"
              "   - 'Fiambre 200g' → 'Fiambre 200 g' (NOT 'Ham 200 g')\n"
              "   - 'Leite 1L' → 'Leite 1 L'\n"
              "   - 'Queijo 300g' → 'Queijo 300 g'\n\n"
              "OUTPUT FORMAT (strict JSON):\n"
            + "JSON schema not needed - using universal prompt"
            + "\n\nIMPORTANT: Ensure all arithmetic is correct, total matches, and product names stay in Portuguese!"
        )
    elif country_code == "RO":
        # Romanian prompt
        prompt = (
            "You are an expert in fixing Romanian receipt data with OCR errors.\n\n"
            "CRITICAL TASK: Fix product names AND correct prices that have OCR errors.\n"
            "The receipt total is usually correct, but individual prices often have OCR mistakes.\n\n"
            "RECEIPT DATA:\n"
            f"API_ID: {api_id}\n"
            + "\n".join(lines)
            + total_block
            + "\n\nYOUR TASKS:\n"
              "1. FIX PRODUCT NAMES (IMPORTANT - KEEP ROMANIAN LANGUAGE):\n"
              "   - Expand abbreviations: 'PÂINE' → 'Pâine', 'LAPT' → 'Lapte'\n"
              "   - Fix typos: 'UNTE' → 'Unt', 'BRÂNZĂ' → 'Brânză'\n"
              "   - Standardize units: '1L' → '1 L', '500G' → '500 g'\n"
              "   - Fix OCR errors: 'PÂINE' → 'Pâine', 'UNTE' → 'Unt'\n"
              "   - Keep Romanian names: 'Pâine' stays 'Pâine' (NOT 'Bread')\n"
              "   - Use proper Romanian capitalization: 'PÂINE' → 'Pâine'\n"
              "   - Fix Romanian diacritics: 'ă', 'â', 'î', 'ș', 'ț'\n"
              "   - Common Romanian products: Pâine, Lapte, Unt, Brânză, Șuncă, Fructe, Legume\n\n"
              "2. FIX PRICES (CRITICAL):\n"
              "   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)\n"
              "   - Check if quantity × unit price = total price for each item\n"
              "   - If not, adjust the unit price or quantity to make it consistent\n"
              "   - All corrected prices must sum up to the receipt total (if provided)\n"
              "   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4\n\n"
              "3. VALIDATION RULES:\n"
              "   - Each line: quantity × price_single MUST equal price_total\n"
              "   - Sum of all price_total MUST equal receipt total (if provided)\n"
              "   - Prices must be realistic for Romanian supermarkets (0.50 - 250 RON per item)\n"
              "   - KEEP PRODUCT NAMES IN ROMANIAN - DO NOT TRANSLATE TO ENGLISH\n\n"
              "ROMANIAN PRODUCT EXAMPLES:\n"
              "   - 'Pâine 500g' → 'Pâine 500 g' (NOT 'Bread 500 g')\n"
              "   - 'Șuncă 200g' → 'Șuncă 200 g' (NOT 'Ham 200 g')\n"
              "   - 'Lapte 1L' → 'Lapte 1 L'\n"
              "   - 'Brânză 300g' → 'Brânză 300 g'\n\n"
              "OUTPUT FORMAT (strict JSON):\n"
            + "JSON schema not needed - using universal prompt"
            + "\n\nIMPORTANT: Ensure all arithmetic is correct, total matches, and product names stay in Romanian!"
        )
    else:
        # Универсальный промпт для других стран
        prompt = f"""
You are an expert in {country_code} retail geography and receipt data correction.

CRITICAL TASK: Fix product names AND prices, AND determine the city from receipt data.

RECEIPT DATA:
- API_ID: {api_id}
- Country: {country_code}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Sample products from receipt:
{products_text}

RECEIPT ITEMS:
{chr(10).join(lines)}{total_block}

YOUR TASKS:
1. FIX PRODUCT NAMES:
   - Fix product names (brand first, title-case, ASCII where possible)
   - Standardize units, expand common abbreviations
   - Fix OCR errors and typos

2. FIX PRICES (CRITICAL):
   - OCR often misreads prices (e.g., 1.89 might be 1.39, 3.09 might be 8.09)
   - Check if quantity × unit price = total price for each item
   - If not, adjust the unit price or quantity to make it consistent
   - All corrected prices must sum up to the receipt total (if provided)
   - Common OCR errors: 3→8, 5→6, 1→7, 0→8, 9→4

3. DETERMINE CITY (CRITICAL - USE NATIONAL NAMES):
   - Use VAT/NIP first digits for regional hints if available
   - Consider retail chain geographic presence knowledge
   - Analyze product types for regional preferences
   - Use general European geographic knowledge
   - Consider population centers and economic hubs
   - ALWAYS use national city names: Warszawa (not Warsaw), Roma (not Rome), München (not Munich)
   - Check the country-specific city list below for correct national names

EUROPEAN RETAIL CHAINS KNOWLEDGE:
- ALDI: Strong in Germany, Netherlands, Belgium, France, Austria, Switzerland
- LIDL: Pan-European presence, strong in Germany, France, Italy, Spain, Poland
- CARREFOUR: Major cities in France, Spain, Italy, Belgium, Romania
- TESCO: UK, Ireland, Czech Republic, Hungary, Slovakia, Poland
- REWE: Germany, Austria, Czech Republic, Hungary
- SPAR: Pan-European, especially Austria, Netherlands, Germany, Italy
- EDEKA: Germany, strong regional presence
- AUCHAN: France, Poland, Romania, Russia, Ukraine
- INTERMARCHE: France, Belgium, Luxembourg, Portugal
- PENNY: Germany, Austria, Italy, Czech Republic

MAJOR EUROPEAN CITIES BY COUNTRY (USE NATIONAL NAMES):
- POLAND: Warszawa, Kraków, Łódź, Wrocław, Poznań, Gdańsk, Szczecin, Bydgoszcz, Lublin, Katowice
- GERMANY: Berlin, Hamburg, München, Köln, Frankfurt, Stuttgart, Düsseldorf, Dortmund, Essen, Leipzig
- FRANCE: Paris, Marseille, Lyon, Toulouse, Nice, Nantes, Montpellier, Strasbourg, Bordeaux, Lille
- ITALY: Roma, Milano, Napoli, Torino, Palermo, Genova, Bologna, Firenze, Bari, Catania
- SPAIN: Madrid, Barcelona, Valencia, Sevilla, Zaragoza, Málaga, Murcia, Palma, Las Palmas, Bilbao
- NETHERLANDS: Amsterdam, Rotterdam, Den Haag, Utrecht, Eindhoven, Tilburg, Groningen, Almere, Breda, Nijmegen
- BELGIUM: Brussels, Antwerp, Ghent, Charleroi, Liège, Bruges, Namur, Leuven, Mons, Aalst
- AUSTRIA: Vienna, Graz, Linz, Salzburg, Innsbruck, Klagenfurt, Villach, Wels, Sankt Pölten, Dornbirn
- SWITZERLAND: Zürich, Geneva, Basel, Bern, Lausanne, Winterthur, Lucerne, St. Gallen, Lugano, Biel
- ROMANIA: București, Cluj-Napoca, Timișoara, Iași, Constanța, Craiova, Galați, Ploiești, Brașov, Brăila
- PORTUGAL: Lisboa, Porto, Braga, Setúbal, Coimbra, Queluz, Funchal, Cacém, Vila Nova de Gaia, Loures
- HUNGARY: Budapest, Debrecen, Szeged, Miskolc, Pécs, Győr, Nyíregyháza, Kecskemét, Székesfehérvár, Szombathely
- UNITED KINGDOM: London, Birmingham, Manchester, Glasgow, Liverpool, Leeds, Sheffield, Edinburgh, Bristol, Leicester

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "total_receipt": {total_receipt_str},
  "total_calculated": <sum of all corrected price_total>,
  "corrections_made": true/false,
  "products": [
    {{
      "name_original": "original name from receipt",
      "name_corrected": "fixed name",
      "quantity": integer,
      "price_single_original": number,
      "price_single_corrected": number,
      "price_total": number,
      "price_correction_reason": "OCR error: 3→8" or null
    }}
  ],
  "city_analysis": {{
    "city": "city name or UNKNOWN",
    "region": "region/province name or UNKNOWN",
    "city_population": "population number if known",
    "confidence": "HIGH/MEDIUM/LOW",
    "evidence": "brief explanation of how you identified the location"
  }}
}}

IMPORTANT: Ensure all arithmetic is correct, total matches, and provide best city estimate!

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON.
"""
    return prompt

def call_gemini_api(prompt: str) -> str:
    """Call the Gemini 2.0 Flash API with the given prompt."""
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not set; cannot call Gemini API")
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        "gemini-2.0-flash:generateContent?key=" + GEMINI_API_KEY
    )
    headers = {"Content-Type": "application/json"}
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 8192,
            "candidateCount": 1,
            "responseMimeType": "application/json"
        },
    }
    response = requests.post(url, headers=headers, json=data, timeout=60)
    response.raise_for_status()
    j = response.json()
    return j.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")

def call_gemini_api_with_retry(prompt: str) -> str:
    """Call the Gemini API with retry mechanism for failed requests."""
    if BACKOFF_AVAILABLE:
        @backoff.on_exception(
            backoff.expo,
            (requests.RequestException, requests.HTTPError),
            max_tries=3,
            base=2,
            max_value=10,
            logger=logger
        )
        def _call_with_retry():
            return call_gemini_api(prompt)
        
        try:
            return _call_with_retry()
        except Exception as e:
            logger.error(f"Gemini API call failed after retries: {e}")
            raise
    else:
        # Fallback without retry
        return call_gemini_api(prompt)

def clean_and_parse_json(response_text: str) -> Dict[str, Any]:
    """Attempt to parse JSON from the Gemini response."""
    try:
        txt = response_text.strip()
        if "```json" in txt:
            txt = txt.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in txt:
            txt = txt.split("```", 1)[1].split("```", 1)[0].strip()
        try:
            parsed = json.loads(txt)
            # Ensure we return a dict, not a list
            if isinstance(parsed, list):
                logger.warning("Gemini returned a list instead of dict, wrapping in products key")
                return {"products": parsed}
            return parsed
        except json.JSONDecodeError:
            txt2 = re.sub(r"(\w+):", r'"\1":', txt)
            txt2 = re.sub(r",(\s*[}\]])", r"\1", txt2)
            txt2 = re.sub(r"\\(?![\"\\/bfnrt])", r"\\\\", txt2)
            parsed = json.loads(txt2)
            # Ensure we return a dict, not a list
            if isinstance(parsed, list):
                logger.warning("Gemini returned a list instead of dict (after fix), wrapping in products key")
                return {"products": parsed}
            return parsed
    except Exception as e:
        logger.warning("JSON parsing error: %s", e)
    return {"products": []}

def normalise_text(s: str) -> str:
    """Normalise a string for matching."""
    # Добавляем польские диакритики
    replacements = str.maketrans(
        "àáâäãåçèéêëìíîïñòóôöõùúûüýžÀÁÂÄÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝŽąćęłńóśźżĄĆĘŁŃÓŚŹŻ",
        "aaaaaaceeeeiiiinooooouuuuyzAAAAAACEEEEIIIINOOOOOUUUUYZacelnoszzACELNOSZZ",
    )
    s2 = s.translate(replacements)
    return re.sub(r"[^a-z0-9]+", "", s2.lower())

def extract_zip_code(address: str, country: str = "PL") -> Optional[str]:
    """
    Извлекает почтовый индекс из адреса.
    Поддерживает различные форматы для разных стран.
    """
    if not address:
        return None
    
    # Паттерны для разных стран
    zip_patterns = {
        "PL": r"\b\d{2}-\d{3}\b",  # Польша: 00-000
        "IT": r"\b\d{5}\b",  # Италия: 00000
        "GB": r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b",  # Великобритания: SW1A 1AA
        "FR": r"\b\d{5}\b",  # Франция: 00000
        "DE": r"\b\d{5}\b",  # Германия: 00000
        "ES": r"\b\d{5}\b",  # Испания: 00000
        "PT": r"\b\d{4}-\d{3}\b",  # Португалия: 0000-000
        "RO": r"\b\d{6}\b",  # Румыния: 000000
        "HU": r"\b\d{4}\b",  # Венгрия: 0000
        "AT": r"\b\d{4}\b",  # Австрия: 0000
        "GR": r"\b\d{3}\s?\d{2}\b",  # Греция: 000 00
        "IE": r"\b[A-Z]\d{2}\s?[A-Z0-9]{4}\b",  # Ирландия: D02 AF30
        "NL": r"\b\d{4}\s?[A-Z]{2}\b",  # Нидерланды: 0000 AA
        "BE": r"\b\d{4}\b",  # Бельгия: 0000
        "FI": r"\b\d{5}\b",  # Финляндия: 00000
        "SK": r"\b\d{3}\s?\d{2}\b",  # Словакия: 000 00
        "SI": r"\b\d{4}\b",  # Словения: 0000
        "EE": r"\b\d{5}\b",  # Эстония: 00000
        "LV": r"\b[LV]-?\d{4}\b",  # Латвия: LV-0000
        "LT": r"\b[LT]-?\d{5}\b",  # Литва: LT-00000
        "LU": r"\b\d{4}\b",  # Люксембург: 0000
        "MT": r"\b[A-Z]{3}\s?\d{4}\b",  # Мальта: AAA 0000
        "CY": r"\b\d{4}\b",  # Кипр: 0000
        "RS": r"\b\d{5,6}\b",  # Сербия: 00000 или 000000
    }
    
    pattern = zip_patterns.get(country.upper(), r"\b\d{4,6}\b")  # По умолчанию ищем 4-6 цифр
    
    matches = re.findall(pattern, address.upper())
    if matches:
        # Возвращаем первый найденный индекс, очищенный от пробелов
        zip_code = matches[0].replace(" ", "").replace("-", "")
        # Для некоторых стран нужно сохранить формат
        if country.upper() in ["PL", "PT"]:
            # Восстанавливаем дефис для польских и португальских индексов
            if len(zip_code) == 5:
                zip_code = f"{zip_code[:2]}-{zip_code[2:]}"
            elif len(zip_code) == 7:
                zip_code = f"{zip_code[:4]}-{zip_code[4:]}"
        return zip_code
    
    return None

def extract_province_code(address: str, country: str = "IT") -> Optional[str]:
    """
    Извлекает код провинции из адреса (для Италии: RM, NA, MI и т.д.)
    
    Args:
        address: Адрес из чека
        country: Код страны (по умолчанию IT)
    
    Returns:
        Код провинции (например, "RM", "NA") или None
    """
    if country != "IT" or not address:
        return None
    
    # Паттерн для Италии: (RM), (NA), (MI) и т.д.
    pattern = r'\(([A-Z]{2})\)'
    match = re.search(pattern, address.upper())
    if match:
        province_code = match.group(1)
        logger.info(f"Extracted province code: {province_code} from address: {address}")
        return province_code
    return None

def extract_addresses_from_receipt(raw_address: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Извлекает два адреса из чека: адрес магазина и адрес главного офиса.
    Обычно они разделены запятыми, точкой с запятой или переносами строк.
    
    Returns:
        Tuple[shop_address, headquarters_address]
    """
    if not raw_address:
        return None, None
    
    # Разделяем адреса по различным разделителям
    # Обычно первый адрес - это адрес магазина, второй - главный офис
    separators = ["\n", ";", "|", "//"]
    
    addresses = [raw_address]
    for sep in separators:
        if sep in raw_address:
            addresses = [a.strip() for a in raw_address.split(sep, 1)]
            break
    
    shop_address = addresses[0] if len(addresses) > 0 else None
    headquarters_address = addresses[1] if len(addresses) > 1 else None
    
    return shop_address, headquarters_address

def parse_city_population(population_str: str) -> Optional[int]:
    """Parse city population from various formats like '1.79 million', '1790000', '1,790,000'."""
    if not population_str or population_str == "UNKNOWN":
        return None
    
    try:
        # Remove common suffixes and convert to lowercase
        text = str(population_str).lower().strip()
        
        # Handle "million" format
        if 'million' in text:
            # Extract number before "million"
            num_part = text.split('million')[0].strip()
            # Remove commas and convert to float
            num_part = num_part.replace(',', '').replace('.', '')
            if num_part.isdigit():
                return int(num_part) * 1000000
        
        # Handle "thousand" format  
        elif 'thousand' in text or 'k' in text:
            num_part = text.replace('thousand', '').replace('k', '').strip()
            num_part = num_part.replace(',', '').replace('.', '')
            if num_part.isdigit():
                return int(num_part) * 1000
        
        # Handle direct number format
        else:
            # Remove commas and try to convert
            clean_num = text.replace(',', '').replace('.', '')
            if clean_num.isdigit():
                return int(clean_num)
        
        return None
    except Exception as e:
        logger.warning(f"Failed to parse city population '{population_str}': {e}")
        return None

def normalize_network_name_by_country(network: str, country_code: str = "PL") -> str:
    """Normalize retail network names by country."""
    if not network or network == 'UNKNOWN':
        return 'UNKNOWN'
    
    if country_code == "PL":
        network_mapping = {
            # Major Polish chains
            'BIEDRONKA': 'BIEDRONKA', 'biedronka': 'BIEDRONKA', 'Biedronka': 'BIEDRONKA',
            'ŻABKA': 'ŻABKA', 'zabka': 'ŻABKA', 'Zabka': 'ŻABKA', 'ZABKA': 'ŻABKA',
            'KAUFLAND': 'KAUFLAND', 'kaufland': 'KAUFLAND', 'Kaufland': 'KAUFLAND',
            'LIDL': 'LIDL', 'lidl': 'LIDL', 'Lidl': 'LIDL',
            'CARREFOUR': 'CARREFOUR', 'carrefour': 'CARREFOUR', 'Carrefour': 'CARREFOUR',
            'TESCO': 'TESCO', 'tesco': 'TESCO', 'Tesco': 'TESCO',
            'ALDI': 'ALDI', 'aldi': 'ALDI', 'Aldi': 'ALDI',
            'NETTO': 'NETTO', 'netto': 'NETTO', 'Netto': 'NETTO',
            'PENNY': 'PENNY MARKET', 'penny': 'PENNY MARKET', 'Penny': 'PENNY MARKET',
            'REAL': 'REAL', 'real': 'REAL', 'Real': 'REAL',
            'INTERMARCHE': 'INTERMARCHE', 'intermarche': 'INTERMARCHE', 'Intermarche': 'INTERMARCHE',
            'SPAR': 'SPAR', 'spar': 'SPAR', 'Spar': 'SPAR',
            'POLOMARKET': 'POLOMARKET', 'polomarket': 'POLOMARKET', 'Polomarket': 'POLOMARKET',
            'STOKROTKA': 'STOKROTKA', 'stokrotka': 'STOKROTKA', 'Stokrotka': 'STOKROTKA',
            'LEWIATAN': 'LEWIATAN', 'lewiatan': 'LEWIATAN', 'Lewiatan': 'LEWIATAN',
            'ABC': 'ABC', 'abc': 'ABC', 'Abc': 'ABC',
            'DELIKATESY': 'DELIKATESY', 'delikatesy': 'DELIKATESY', 'Delikatesy': 'DELIKATESY',
            'FRAC': 'FRAC', 'frac': 'FRAC', 'Frac': 'FRAC',
            'GROSZEK': 'GROSZEK', 'groszek': 'GROSZEK', 'Groszek': 'GROSZEK',
            'MILA': 'MILA', 'mila': 'MILA', 'Mila': 'MILA',
            'POLSKI': 'POLSKI', 'polski': 'POLSKI', 'Polski': 'POLSKI',
            'SKLEP': 'SKLEP', 'sklep': 'SKLEP', 'Sklep': 'SKLEP'
        }
    elif country_code == "IT":
        network_mapping = {
            # Major Italian chains (from old code)
            'CONAD': 'CONAD', 'conad': 'CONAD', 'Conad': 'CONAD',
            'COOP': 'COOP', 'coop': 'COOP', 'Coop': 'COOP',
            'ESSELUNGA': 'ESSELUNGA', 'esselunga': 'ESSELUNGA', 'Esselunga': 'ESSELUNGA',
            'CARREFOUR': 'CARREFOUR', 'carrefour': 'CARREFOUR', 'Carrefour': 'CARREFOUR',
            'LIDL': 'LIDL', 'lidl': 'LIDL', 'Lidl': 'LIDL',
            'EUROSPIN': 'EUROSPIN', 'eurospin': 'EUROSPIN', 'Eurospin': 'EUROSPIN',
            'PAM': 'PAM', 'pam': 'PAM', 'Pam': 'PAM',
            'DESPAR': 'DESPAR', 'despar': 'DESPAR', 'Despar': 'DESPAR',
            'SELEX': 'SELEX', 'selex': 'SELEX', 'Selex': 'SELEX',
            'FAMILA': 'FAMILA', 'famila': 'FAMILA', 'Famila': 'FAMILA',
            'BENNET': 'BENNET', 'bennet': 'BENNET', 'Bennet': 'BENNET',
            'CRAI': 'CRAI', 'crai': 'CRAI', 'Crai': 'CRAI',
            'MD DISCOUNT': 'MD DISCOUNT', 'md discount': 'MD DISCOUNT', 'MD': 'MD DISCOUNT',
            'PENNY MARKET': 'PENNY MARKET', 'penny market': 'PENNY MARKET', 'PENNY': 'PENNY MARKET'
        }
    elif country_code == "DE":
        network_mapping = {
            # Major German chains
            'ALDI': 'ALDI', 'aldi': 'ALDI', 'Aldi': 'ALDI',
            'LIDL': 'LIDL', 'lidl': 'LIDL', 'Lidl': 'LIDL',
            'REWE': 'REWE', 'rewe': 'REWE', 'Rewe': 'REWE',
            'EDEKA': 'EDEKA', 'edeka': 'EDEKA', 'Edeka': 'EDEKA',
            'KAUFLAND': 'KAUFLAND', 'kaufland': 'KAUFLAND', 'Kaufland': 'KAUFLAND',
            'REAL': 'REAL', 'real': 'REAL', 'Real': 'REAL',
            'NETTO': 'NETTO', 'netto': 'NETTO', 'Netto': 'NETTO',
            'PENNY': 'PENNY', 'penny': 'PENNY', 'Penny': 'PENNY',
            'NORMA': 'NORMA', 'norma': 'NORMA', 'Norma': 'NORMA',
            'TEGUT': 'TEGUT', 'tegut': 'TEGUT', 'Tegut': 'TEGUT',
            'SPAR': 'SPAR', 'spar': 'SPAR', 'Spar': 'SPAR',
            'DM': 'DM', 'dm': 'DM', 'Dm': 'DM',
            'ROSSMANN': 'ROSSMANN', 'rossmann': 'ROSSMANN', 'Rossmann': 'ROSSMANN'
        }
    elif country_code == "FR":
        network_mapping = {
            # Major French chains
            'CARREFOUR': 'CARREFOUR', 'carrefour': 'CARREFOUR', 'Carrefour': 'CARREFOUR',
            'LECLERC': 'LECLERC', 'leclerc': 'LECLERC', 'Leclerc': 'LECLERC',
            'AUCHAN': 'AUCHAN', 'auchan': 'AUCHAN', 'Auchan': 'AUCHAN',
            'CASINO': 'CASINO', 'casino': 'CASINO', 'Casino': 'CASINO',
            'INTERMARCHE': 'INTERMARCHE', 'intermarche': 'INTERMARCHE', 'Intermarché': 'INTERMARCHE',
            'SUPER U': 'SUPER U', 'super u': 'SUPER U', 'Super U': 'SUPER U',
            'SYSTEME U': 'SYSTEME U', 'systeme u': 'SYSTEME U', 'Système U': 'SYSTEME U',
            'LIDL': 'LIDL', 'lidl': 'LIDL', 'Lidl': 'LIDL',
            'ALDI': 'ALDI', 'aldi': 'ALDI', 'Aldi': 'ALDI',
            'MONOPRIX': 'MONOPRIX', 'monoprix': 'MONOPRIX', 'Monoprix': 'MONOPRIX',
            'FRANPRIX': 'FRANPRIX', 'franprix': 'FRANPRIX', 'Franprix': 'FRANPRIX',
            'SPAR': 'SPAR', 'spar': 'SPAR', 'Spar': 'SPAR'
        }
    else:
        # Universal mapping for other countries
        network_mapping = {
            'LIDL': 'LIDL', 'lidl': 'LIDL', 'Lidl': 'LIDL',
            'ALDI': 'ALDI', 'aldi': 'ALDI', 'Aldi': 'ALDI',
            'CARREFOUR': 'CARREFOUR', 'carrefour': 'CARREFOUR', 'Carrefour': 'CARREFOUR',
            'SPAR': 'SPAR', 'spar': 'SPAR', 'Spar': 'SPAR'
        }
    
    # ИСПРАВЛЕНО: Безопасное преобразование в строку
    try:
        network_str = str(network) if network else 'UNKNOWN'
        network_upper = network_str.upper()
        if network_upper in [k.upper() for k in network_mapping]:
            for k, v in network_mapping.items():
                if k.upper() == network_upper:
                    return v
        
        return network_str.upper()
    except Exception as e:
        logger.warning(f"Error normalizing network name '{network}': {e}")
        return 'UNKNOWN'

# Backward compatibility
def normalize_polish_network_name(network: str) -> str:
    """Backward compatibility function for Polish networks."""
    return normalize_network_name_by_country(network, "PL")

def analyze_receipt_for_city(api_id: str, products_json: List[dict], nip: str = None, shopnetwork: str = None, country: str = "PL", raw_address: str = None) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int], Optional[str], Optional[str], Optional[str]]:
    """
    Анализирует чек через Gemini API для определения города на основе NIP, продуктов и сети магазинов.
    Поддерживает извлечение zip_code и различение адреса магазина и главного офиса.
    """
    # DEBUG: Отслеживание параметра country
    logger.info(f"=== AI ANALYSIS DEBUG ===")
    logger.info(f"API_ID: {api_id}, COUNTRY: {country}")
    logger.info(f"NIP: {nip}, SHOPNETWORK: {shopnetwork}")
    logger.info(f"Raw Address: {raw_address}")
    
    if not products_json:
        return None, None, None, None, None, None, None
    
    # Извлекаем адреса (магазин и главный офис)
    shop_address, headquarters_address = extract_addresses_from_receipt(raw_address) if raw_address else (None, None)
    
    # Извлекаем zip_code из адреса магазина (приоритет) или из общего адреса
    zip_code = None
    if shop_address:
        zip_code = extract_zip_code(shop_address, country)
    if not zip_code and raw_address:
        zip_code = extract_zip_code(raw_address, country)
    
    # Извлекаем код провинции для Италии
    province_code = None
    if country == "IT":
        if shop_address:
            province_code = extract_province_code(shop_address, country)
        if not province_code and raw_address:
            province_code = extract_province_code(raw_address, country)
    
    logger.info(f"Extracted addresses - Shop: {shop_address}, HQ: {headquarters_address}, ZIP: {zip_code}, Province: {province_code}")
    
    # Собираем уникальные продукты (как в старом коде)
    unique = []
    seen = set()
    for p in products_json[:15]:
        name = (p.get('name') or '').upper()
        if name and len(name) > 2 and name not in seen:
            unique.append(name)
            seen.add(name)
    products_text = "\n".join(f"- {p}" for p in unique[:10])
    
    # Улучшенный промпт на основе старого кода с поддержкой разных стран
    if country == "PL":
        prompt = f"""
You are an expert in Polish retail geography and store networks. Analyze this Polish receipt to determine the most likely city.

CRITICAL MATCHING PRIORITY (use in this exact order):
1. ZIP CODE MATCH (HIGHEST PRIORITY) ⭐⭐⭐
   - If zip_code is provided and not 'none', use it FIRST
   - Match city by zip_code from your knowledge
   - Example: zip_code "20-315" → Lublin (Poland)
   - Example: zip_code "00-001" → Warszawa (Poland)
   
2. STREET ADDRESS CONTEXT ⭐
   - Use street name to disambiguate between similar cities
   
3. NORMALIZED CITY NAME SIMILARITY
   - Use only if ZIP not available
   
4. NIP REGION HINTS
   - Use only as last resort

RECEIPT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Shop Address (if separated): {shop_address or 'none'}
- Headquarters Address (if separated): {headquarters_address or 'none'}
- Extracted ZIP Code: {zip_code or 'none'} {'⭐ USE THIS FIRST!' if zip_code and zip_code != 'none' else ''}
- Sample products from receipt:
{products_text}

IMPORTANT: Receipts often contain TWO addresses:
1. Shop/Store Address - This is the location where the purchase was made (USE THIS for city determination)
2. Headquarters Address - This is the company's main office (IGNORE THIS for city determination)

If addresses are separated, prioritize the SHOP ADDRESS for determining the city and zip code.

EXAMPLES OF CORRECT MATCHING:

Example 1:
- Address: "20-315 Lublin, Al.Witosa 8"
- ZIP Code: "20-315"
- ✅ CORRECT: Match by ZIP "20-315" → Lublin (Lubelskie)
- ❌ WRONG: Ignore ZIP and use only "Lublin" text

Example 2:
- Address: "00-001 Warszawa, ul. Marszałkowska 1"
- ZIP Code: "00-001"
- ✅ CORRECT: Match by ZIP "00-001" → Warszawa (Mazowieckie)
- ❌ WRONG: Ignore ZIP and guess randomly

POLISH RETAIL CHAINS KNOWLEDGE:
- BIEDRONKA: Strong in all major cities, especially Warszawa, Kraków, Wrocław
- ŻABKA: Especially strong in Warszawa, Kraków, Wrocław, Poznań
- KAUFLAND: Major cities, especially Wrocław, Poznań, Warszawa
- LIDL: All major cities, strong presence everywhere
- CARREFOUR: Warszawa, Kraków, Poznań, Wrocław
- TESCO: Major cities, especially Warszawa, Kraków
- ALDI: Growing presence in major cities
- NETTO: Strong in northern regions

GEOGRAPHIC STRATEGY:
1. Use NIP first digits for regional hints:
   - 10-19: Mazowieckie (Warszawa area)
   - 20-29: Małopolskie (Kraków area) 
   - 30-39: Lubelskie, Podkarpackie
   - 40-49: Śląskie (Katowice area)
   - 50-59: Dolnośląskie (Wrocław area)
   - 60-69: Wielkopolskie (Poznań area)
   - 70-79: Zachodniopomorskie (Szczecin area)
   - 80-89: Pomorskie (Gdańsk area)
   - 90-99: Warmińsko-mazurskie, Podlaskie

2. Consider all Polish cities and towns (population 50,000+)
3. Use retail chain geographic presence knowledge
4. Analyze product types for regional preferences
5. Provide population estimates based on city size knowledge

POLISH REGIONS AND MAJOR CITIES (USE STANDARD FORMS):
- MAZOWIECKIE: Warszawa, Radom, Płock, Siedlce, Ostrołęka
- MAŁOPOLSKIE: Kraków, Tarnów, Nowy Sącz, Oświęcim
- ŚLĄSKIE: Katowice, Częstochowa, Sosnowiec, Gliwice, Zabrze, Bytom
- WIELKOPOLSKIE: Poznań, Kalisz, Konin, Piła
- DOLNOŚLĄSKIE: Wrocław, Wałbrzych, Legnica, Jelenia Góra
- ŁÓDZKIE: Łódź, Piotrków Trybunalski, Pabianice, Tomaszów Mazowiecki
- POMORSKIE: Gdańsk, Gdynia, Sopot, Słupsk, Tczew
- ZACHODNIOPOMORSKIE: Szczecin, Koszalin, Stargard, Kołobrzeg
- LUBELSKIE: Lublin, Chełm, Zamość, Biała Podlaska
- PODKARPACKIE: Rzeszów, Przemyśl, Stalowa Wola, Mielec
- PODLASKIE: Białystok, Suwałki, Łomża, Augustów
- WARMIŃSKO-MAZURSKIE: Olsztyn, Elbląg, Ełk, Ostróda
- KUJAWSKO-POMORSKIE: Bydgoszcz, Toruń, Włocławek, Grudziądz
- ŚWIĘTOKRZYSKIE: Kielce, Ostrowiec Świętokrzyski, Starachowice
- LUBUSKIE: Zielona Góra, Gorzów Wielkopolski, Żary, Nowa Sól
- OPOLSKIE: Opole, Kędzierzyn-Koźle, Nysa, Brzeg

IMPORTANT: Always use standard city names like "Warszawa" (not "WARSAW"), "Kraków" (not "CRACOW"), "Gdańsk" (not "DANZIG").

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "nip": "{nip or 'none'}",
  "city": "city name or UNKNOWN (use standard forms: Warszawa, Kraków, Gdańsk)",
  "region": "region name or UNKNOWN",
  "zip_code": "postal code (e.g., 00-000 for Poland, 00000 for Italy) or null if not found",
  "city_population": "population number (e.g., 1791000 for Warszawa, 779000 for Kraków)",
  "match_method": "zip_code_match" | "address_match" | "name_match" | "nip_hint",
  "confidence": "HIGH/MEDIUM/LOW",
  "evidence": "brief explanation of how you identified the location, including which address was used (shop vs headquarters) and which matching method was used"
}}

CONFIDENCE LEVELS:
- HIGH: Multiple clear indicators (product brands + geographic logic + NIP region match)
- MEDIUM: Some indicators with reasonable geographic assumptions
- LOW: Best educated guess based on limited evidence

IMPORTANT: 
1. Always attempt to provide city and region even if uncertain. 
2. ALWAYS provide city_population - this is mandatory! Use your knowledge of Polish cities:
   - Warszawa: ~1,790,000
   - Kraków: ~779,000  
   - Wrocław: ~643,000
   - Gdańsk: ~470,000
   - Poznań: ~534,000
   - Łódź: ~677,000
   - Katowice: ~294,000
   - Lublin: ~339,000
   - Białystok: ~297,000
   - Szczecin: ~400,000
   - Gdynia: ~246,000
   - Bydgoszcz: ~346,000
   - Lublin: ~339,000
   - Częstochowa: ~214,000
   - Radom: ~211,000
   - Sosnowiec: ~199,000
   - Toruń: ~201,000
   - Kielce: ~194,000
   - Gliwice: ~178,000
   - Zabrze: ~171,000
   - Bytom: ~164,000
   - Olsztyn: ~171,000
   - Rzeszów: ~196,000
   - Ruda Śląska: ~136,000
   - Rybnik: ~138,000
   - Tychy: ~127,000
   - Dąbrowa Górnicza: ~119,000
   - Elbląg: ~119,000
   - Płock: ~119,000
   - Wałbrzych: ~112,000
   - Włocławek: ~108,000
   - Tarnów: ~108,000
   - Chorzów: ~107,000
   - Kalisz: ~100,000
   - Koszalin: ~107,000
   - Legnica: ~99,000
   - Grudziądz: ~95,000
   - Słupsk: ~90,000
   - Jaworzno: ~89,000
   - Jastrzębie-Zdrój: ~87,000
   - Nowy Sącz: ~83,000
   - Jelenia Góra: ~79,000
   - Konin: ~73,000
   - Piotrków Trybunalski: ~72,000
   - Lubin: ~71,000
   - Inowrocław: ~70,000
   - Ostrów Wielkopolski: ~69,000
   - Stargard: ~67,000
   - Mysłenice: ~65,000
   - Pabianice: ~63,000
   - Gniezno: ~68,000
   - Ostrów Mazowiecka: ~22,000
   - Słupca: ~13,000
   - Żywiec: ~31,000
   - Stalowa Wola: ~60,000
   - Mielec: ~59,000
   - Łęczyca: ~14,000
   - Tarnobrzeg: ~46,000
   - Puławy: ~47,000
   - Oleśnica: ~36,000
   - Gorzów Wielkopolski: ~123,000
   - Włocławek: ~108,000
   - Zielona Góra: ~140,000
   - Krosno: ~45,000
   - Legionowo: ~64,000
   - Skarżysko-Kamienna: ~44,000
   - Radomsko: ~46,000
   - Oświęcim: ~37,000
   - Starachowice: ~48,000
   - Zawiercie: ~49,000
   - Międzyrzecz: ~18,000
   - Płońsk: ~22,000
   - Oława: ~32,000
   - Głogów: ~67,000
   - Jarosław: ~37,000
   - Nowy Targ: ~33,000
   - Jasło: ~35,000
   - Kętrzyn: ~27,000
   - Racibórz: ~54,000
   - Świętochłowice: ~49,000
3. Use geographic knowledge about Polish retail distribution patterns. 
4. Prefer real Polish city names over 'UNKNOWN'. 
5. Use standard city forms (Warszawa, Kraków, Gdańsk).

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON."""
    elif country == "IT":
        prompt = f"""
You are an expert in Italian retail geography and store chains.

CRITICAL MATCHING PRIORITY (use in this exact order):
1. ZIP CODE MATCH (HIGHEST PRIORITY) ⭐⭐⭐
   - If zip_code is provided and not 'none', use it FIRST
   - Match city by zip_code from your knowledge
   - Example: zip_code "80126" → NAPOLI (Campania)
   - Example: zip_code "00100" → ROMA (Lazio)
   
2. PROVINCE/REGION CODE MATCH ⭐⭐
   - If address contains (RM), (NA), (MI), etc. → filter by province
   - Example: "(RM)" → Roma province → filter cities in Lazio
   - Example: "(NA)" → Napoli province → filter cities in Campania
   
3. STREET ADDRESS CONTEXT ⭐
   - Use street name to disambiguate between similar cities
   - Example: "VIA GIUSTINIANO" in Napoli vs other cities
   
4. NORMALIZED CITY NAME SIMILARITY
   - Use only if ZIP/province not available
   
5. NIP REGION HINTS
   - Use only as last resort

REQUEST: We need the name of the chain of stores/restaurants (shop_chain), the city, shop street name, zip code, 
building number and integer population size of that city (city_population) of Italy. If possible, derive it from the 
receipt data, NIP number, or from clues and general knowledge about Italian geography and retail distribution.

If totally not possible please return 'UNKNOWN' as value. Try to provide best-guess based on available information. 
Try to fix mistakes in names and use standard Italian geographic naming.

INPUT DATA:
- API_ID: {api_id}
- NIP (VAT Number): {nip or 'none'}
- Raw Address: {raw_address or 'none'}
- Shop Address (if separated): {shop_address or 'none'}
- Headquarters Address (if separated): {headquarters_address or 'none'}
- Extracted ZIP Code: {zip_code or 'none'} {'⭐ USE THIS FIRST!' if zip_code and zip_code != 'none' else ''}
- Extracted Province Code: {province_code or 'none'} {'⭐ USE THIS SECOND!' if province_code and province_code != 'none' else ''}
- Sample products from receipt:
{products_text}

IMPORTANT: Receipts often contain TWO addresses:
1. Shop/Store Address - This is the location where the purchase was made (USE THIS for city determination)
2. Headquarters Address - This is the company's main office (IGNORE THIS for city determination)

If addresses are separated, prioritize the SHOP ADDRESS for determining the city and zip code.

EXAMPLES OF CORRECT MATCHING:

Example 1:
- Address: "VIA GIUSTINIANO, 150-80126 NAPOLI"
- ZIP Code: "80126"
- ✅ CORRECT: Match by ZIP "80126" → NAPOLI (Campania)
- ❌ WRONG: Ignore ZIP and use only "NAPOLI" text

Example 2:
- Address: "VIA ULE PLATANI, 10 MANTEANA (RM)"
- Province: "RM"
- ZIP Code: None
- ✅ CORRECT: Filter by province "RM" → Roma area → MANTEANA (Lazio)
- ❌ WRONG: Ignore province and guess randomly

Example 3:
- Address: "VIA ROMA, 1-00100 ROMA"
- ZIP Code: "00100"
- Province: None
- ✅ CORRECT: Match by ZIP "00100" → ROMA (Lazio)

ITALIAN RETAIL CHAINS KNOWLEDGE:
- CONAD: Cooperative stores… "CONAD", "C-", "FIOR FIORE"
- CARREFOUR: … "TERRE D'ITALIA"
- ESSELUNGA: … Lombardia, "NATURAMA"
- COOP: … "SAPORI&DINTORNI"
- LIDL: … discount
- EUROSPIN: … very cheap, "TRE MULINI"
- PAM / PANORAMA, DESPAR, PENNY MARKET, MD DISCOUNT, SELEX, FAMILA, BENNET, CRAI

GEOGRAPHIC STRATEGY:
1. Use NIP first digits for regional hints (Northern Italy: 01-19, Central: 20-59, Southern: 60-99)
2. Consider all Italian cities and towns (population 10,000+)
3. Use retail chain geographic presence knowledge
4. Estimate reasonable street addresses and zip codes
5. Provide population estimates based on city size knowledge

ITALIAN REGIONS AND MAJOR CITIES (USE STANDARD FORMS):
- LAZIO: Roma, Latina, Frosinone, Viterbo, Rieti
- LOMBARDIA: Milano, Bergamo, Brescia, Monza, Como, Varese, Pavia, Cremona
- CAMPANIA: Napoli, Salerno, Caserta, Avellino, Benevento
- PIEMONTE: Torino, Alessandria, Novara, Cuneo, Asti
- SICILIA: Palermo, Catania, Messina, Siracusa, Agrigento
- VENETO: Venezia, Verona, Padova, Vicenza, Treviso
- EMILIA-ROMAGNA: Bologna, Modena, Parma, Reggio Emilia, Ravenna, Ferrara
- TOSCANA: Firenze, Pisa, Livorno, Prato, Siena, Arezzo
- PUGLIA: Bari, Taranto, Foggia, Lecce, Brindisi
- LIGURIA: Genova, La Spezia, Savona, Imperia
- CALABRIA: Reggio Calabria, Catanzaro, Cosenza
- MARCHE: Ancona, Pesaro, Macerata, Ascoli Piceno
- ABRUZZO: L'Aquila, Pescara, Chieti, Teramo
- UMBRIA: Perugia, Terni

IMPORTANT: Always use standard city names like "Roma" (not "ROMA" or "Rome"), "Milano" (not "MILAN"), "Napoli" (not "Naples").

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "nip": "{nip or 'none'}",
  "shop_chain": "identified chain name or UNKNOWN",
  "city": "city name or UNKNOWN (use standard forms: Roma, Milano, Napoli)",
  "region": "region name or UNKNOWN",
  "shop_address": "street name and building number or VIA ROMA 1",
  "zip_code": "5-digit postal code or 00000",
  "province_code": "extracted province code (RM, NA, MI, etc.) or null",
  "city_population": integer,
  "match_method": "zip_code_match" | "province_match" | "address_match" | "name_match" | "nip_hint",
  "confidence": "HIGH/MEDIUM/LOW",
  "evidence": "brief explanation of how you identified the location and chain, including which matching method was used"
}}

CONFIDENCE LEVELS:
- HIGH: Multiple clear indicators (product brands + geographic logic + NIP region match)
- MEDIUM: Some indicators with reasonable geographic assumptions
- LOW: Best educated guess based on limited evidence

IMPORTANT: 
1. Always attempt to provide city and region even if uncertain. 
2. ALWAYS provide city_population - this is mandatory! Use your knowledge of Italian cities:
   - Roma: ~2,800,000
   - Milano: ~1,400,000
   - Napoli: ~910,000
   - Torino: ~848,000
   - Palermo: ~630,000
   - Genova: ~570,000
   - Bologna: ~390,000
   - Firenze: ~366,000
   - Bari: ~315,000
   - Catania: ~298,000
   - Venezia: ~258,000
   - Verona: ~258,000
   - Messina: ~232,000
   - Padova: ~210,000
   - Trieste: ~200,000
   - Brescia: ~196,000
   - Parma: ~195,000
   - Taranto: ~188,000
   - Prato: ~185,000
   - Modena: ~184,000
   - Reggio Calabria: ~180,000
   - Reggio Emilia: ~170,000
   - Perugia: ~165,000
   - Livorno: ~157,000
   - Ravenna: ~155,000
   - Cagliari: ~154,000
   - Foggia: ~150,000
   - Rimini: ~148,000
   - Salerno: ~133,000
   - Ferrara: ~132,000
   - Sassari: ~127,000
   - Monza: ~123,000
   - Bergamo: ~120,000
   - Forlì: ~118,000
   - Trento: ~117,000
   - Vicenza: ~111,000
   - Terni: ~110,000
   - Bolzano: ~106,000
   - Novara: ~104,000
   - Piacenza: ~103,000
   - Ancona: ~101,000
   - Andria: ~100,000
   - Arezzo: ~99,000
   - Udine: ~99,000
   - Cesena: ~97,000
   - L'Aquila: ~70,000
   - La Spezia: ~93,000
   - Pescara: ~120,000
   - Como: ~84,000
   - Pisa: ~89,000
   - Treviso: ~85,000
   - Varese: ~81,000
   - Busto Arsizio: ~83,000
   - Vigevano: ~63,000
   - Gallarate: ~54,000
   - Saronno: ~39,000
   - Legnano: ~60,000
   - Rho: ~51,000
   - Cinisello Balsamo: ~76,000
   - Sesto San Giovanni: ~81,000
   - Cologno Monzese: ~47,000
   - Paderno Dugnano: ~47,000
   - Rozzano: ~42,000
   - Pioltello: ~37,000
   - Segrate: ~39,000
   - San Giuliano Milanese: ~37,000
   - Corsico: ~34,000
   - Cesano Maderno: ~38,000
   - Limbiate: ~35,000
   - Bollate: ~36,000
   - Arese: ~19,000
   - Garbagnate Milanese: ~27,000
   - Lainate: ~26,000
   - Pero: ~11,000
   - Baranzate: ~11,000
   - Solaro: ~14,000
   - Bresso: ~26,000
   - Cormano: ~20,000
   - Cusano Milanino: ~20,000
   - Novate Milanese: ~20,000
   - Settimo Milanese: ~19,000
   - Vimodrone: ~16,000
   - Peschiera Borromeo: ~23,000
   - San Donato Milanese: ~32,000
   - Mediglia: ~12,000
   - Pieve Emanuele: ~15,000
   - Opera: ~13,000
   - Locate di Triulzi: ~10,000
   - San Zenone al Lambro: ~4,000
   - Zibido San Giacomo: ~6,000
   - Vermezzo con Zelo: ~8,000
   - Bubbiano: ~2,000
   - Calvignasco: ~1,000
   - Casirate d'Adda: ~4,000
   - Cassina de' Pecchi: ~13,000
   - Gessate: ~9,000
   - Gorgonzola: ~20,000
   - Grezzago: ~3,000
   - Inzago: ~11,000
   - Masate: ~3,000
   - Melzo: ~18,000
   - Pozzo d'Adda: ~6,000
   - Trezzano Rosa: ~5,000
   - Trezzo sull'Adda: ~12,000
   - Truccazzano: ~6,000
   - Vaprio d'Adda: ~8,000
   - Vignate: ~9,000
   - Basiano: ~3,000
   - Cambiago: ~6,000
   - Carugate: ~15,000
   - Cernusco sul Naviglio: ~31,000
   - Cologno Monzese: ~47,000
   - Gorgonzola: ~20,000
   - Inzago: ~11,000
   - Liscate: ~4,000
   - Melzo: ~18,000
   - Pioltello: ~37,000
   - Pozzo d'Adda: ~6,000
   - Rodano: ~5,000
   - Settala: ~7,000
   - Truccazzano: ~6,000
   - Vignate: ~9,000
   - Vimodrone: ~16,000
3. Use geographic knowledge about Italian retail distribution patterns. 
4. Prefer real Italian city names over 'UNKNOWN'. 
5. Use standard city forms (Roma, Milano, Napoli).

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON."""
    else:
        # Детальный промпт для других стран (Германия, Франция, Испания и др.)
        prompt = f"""
You are an expert in European retail geography and store networks. Analyze this receipt to determine the most likely city.

CRITICAL MATCHING PRIORITY (use in this exact order):
1. ZIP CODE MATCH (HIGHEST PRIORITY) ⭐⭐⭐
   - If zip_code is provided and not 'none', use it FIRST
   - Match city by zip_code from your knowledge
   - Example: zip_code "18690" → ALMUNECAR (Spain)
   - Example: zip_code "75001" → PARIS (France)
   
2. STREET ADDRESS CONTEXT ⭐
   - Use street name to disambiguate between similar cities
   
3. NORMALIZED CITY NAME SIMILARITY
   - Use only if ZIP not available
   
4. VAT/NIP REGION HINTS
   - Use only as last resort

RECEIPT DATA:
- API_ID: {api_id}
- NIP/VAT Number: {nip or 'none'}
- Store Network: {shopnetwork or 'none'}
- Raw Address: {raw_address or 'none'}
- Shop Address (if separated): {shop_address or 'none'}
- Headquarters Address (if separated): {headquarters_address or 'none'}
- Extracted ZIP Code: {zip_code or 'none'} {'⭐ USE THIS FIRST!' if zip_code and zip_code != 'none' else ''}
- Sample products from receipt:
{products_text}

IMPORTANT: Receipts often contain TWO addresses:
1. Shop/Store Address - This is the location where the purchase was made (USE THIS for city determination)
2. Headquarters Address - This is the company's main office (IGNORE THIS for city determination)

If addresses are separated, prioritize the SHOP ADDRESS for determining the city and zip code.

EXAMPLES OF CORRECT MATCHING:

Example 1 (Spain):
- Address: "C/ MARIANA PINEDA S/N, 18690 ALMUNECAR"
- ZIP Code: "18690"
- ✅ CORRECT: Match by ZIP "18690" → ALMUNECAR (Andalusia)
- ❌ WRONG: Ignore ZIP and use only "ALMUNECAR" text

Example 2 (France):
- Address: "RUE DE LA PAIX, 75001 PARIS"
- ZIP Code: "75001"
- ✅ CORRECT: Match by ZIP "75001" → PARIS (Île-de-France)
- ❌ WRONG: Ignore ZIP and guess randomly

EUROPEAN RETAIL CHAINS KNOWLEDGE:
- ALDI: Strong in Germany, Netherlands, Belgium, France, Austria, Switzerland
- LIDL: Pan-European presence, strong in Germany, France, Italy, Spain, Poland
- CARREFOUR: Major cities in France, Spain, Italy, Belgium, Romania
- TESCO: UK, Ireland, Czech Republic, Hungary, Slovakia, Poland
- REWE: Germany, Austria, Czech Republic, Hungary
- SPAR: Pan-European, especially Austria, Netherlands, Germany, Italy
- EDEKA: Germany, strong regional presence
- AUCHAN: France, Poland, Romania, Russia, Ukraine
- INTERMARCHE: France, Belgium, Luxembourg, Portugal
- PENNY: Germany, Austria, Italy, Czech Republic

GEOGRAPHIC STRATEGY:
1. Use VAT/NIP first digits for regional hints if available
2. Consider retail chain geographic presence knowledge
3. Analyze product types for regional preferences (local brands, languages)
4. Use general European geographic knowledge
5. Consider population centers and economic hubs

MAJOR EUROPEAN CITIES BY COUNTRY (USE NATIONAL NAMES):
- POLAND (PL): Warszawa, Kraków, Łódź, Wrocław, Poznań, Gdańsk, Szczecin, Bydgoszcz, Lublin, Katowice
- GERMANY (DE): Berlin, Hamburg, München, Köln, Frankfurt, Stuttgart, Düsseldorf, Dortmund, Essen, Leipzig
- FRANCE (FR): Paris, Marseille, Lyon, Toulouse, Nice, Nantes, Montpellier, Strasbourg, Bordeaux, Lille
- ITALY (IT): Roma, Milano, Napoli, Torino, Palermo, Genova, Bologna, Firenze, Bari, Catania
- SPAIN (ES): Madrid, Barcelona, Valencia, Sevilla, Zaragoza, Málaga, Murcia, Palma, Las Palmas, Bilbao
- NETHERLANDS (NL): Amsterdam, Rotterdam, Den Haag, Utrecht, Eindhoven, Tilburg, Groningen, Almere, Breda, Nijmegen
- BELGIUM (BE): Brussels, Antwerp, Ghent, Charleroi, Liège, Bruges, Namur, Leuven, Mons, Aalst
- AUSTRIA (AT): Vienna, Graz, Linz, Salzburg, Innsbruck, Klagenfurt, Villach, Wels, Sankt Pölten, Dornbirn
- ROMANIA (RO): București, Cluj-Napoca, Timișoara, Iași, Constanța, Craiova, Galați, Ploiești, Brașov, Brăila
- PORTUGAL (PT): Lisboa, Porto, Braga, Setúbal, Coimbra, Queluz, Funchal, Cacém, Vila Nova de Gaia, Loures
- HUNGARY (HU): Budapest, Debrecen, Szeged, Miskolc, Pécs, Győr, Nyíregyháza, Kecskemét, Székesfehérvár, Szombathely
- UNITED KINGDOM (GB): London, Birmingham, Manchester, Glasgow, Liverpool, Leeds, Sheffield, Edinburgh, Bristol, Leicester
- SERBIA (RS): Belgrade, Novi Sad, Niš, Kragujevac, Subotica, Zrenjanin, Pančevo, Čačak, Novi Pazar, Kraljevo
- ESTONIA (EE): Tallinn, Tartu, Narva, Pärnu, Kohtla-Järve, Viljandi, Rakvere, Maardu, Kuressaare, Võru
- GREECE (GR): Athens, Thessaloniki, Patras, Heraklion, Larissa, Volos, Ioannina, Kavala, Kalamata, Rhodes
- IRELAND (IE): Dublin, Cork, Limerick, Galway, Waterford, Drogheda, Dundalk, Swords, Bray, Navan
- FINLAND (FI): Helsinki, Espoo, Tampere, Vantaa, Oulu, Turku, Jyväskylä, Lahti, Kuopio, Pori
- SLOVAKIA (SK): Bratislava, Košice, Prešov, Žilina, Nitra, Banská Bystrica, Trnava, Martin, Trenčín, Poprad
- SLOVENIA (SI): Ljubljana, Maribor, Celje, Kranj, Velenje, Koper, Novo Mesto, Ptuj, Trbovlje, Kamnik
- LATVIA (LV): Riga, Daugavpils, Liepāja, Jelgava, Jūrmala, Ventspils, Rēzekne, Valmiera, Ogre, Tukums
- LITHUANIA (LT): Vilnius, Kaunas, Klaipėda, Šiauliai, Panevėžys, Alytus, Marijampolė, Mažeikiai, Jonava, Utena
- LUXEMBOURG (LU): Luxembourg City, Esch-sur-Alzette, Differdange, Dudelange, Pétange, Sanem, Hesperange, Bettembourg, Schifflange, Kayl
- MALTA (MT): Valletta, Birkirkara, Mosta, Qormi, Żabbar, San Pawl il-Baħar, Sliema, Żejtun, Fgura, Żebbuġ
- CYPRUS (CY): Nicosia, Limassol, Larnaca, Paphos, Famagusta, Kyrenia, Morphou, Aradippou, Paralimni, Geroskipou

OUTPUT FORMAT (strict JSON):
{{
  "api_id": "{api_id}",
  "nip": "{nip or 'none'}",
  "city": "city name or UNKNOWN",
  "region": "region/province name or UNKNOWN",
  "zip_code": "postal code (country-specific format) or null if not found",
  "city_population": "population number if known",
  "match_method": "zip_code_match" | "address_match" | "name_match" | "vat_hint",
  "confidence": "HIGH/MEDIUM/LOW",
  "evidence": "brief explanation of how you identified the location, including which address was used (shop vs headquarters) and which matching method was used"
}}

CONFIDENCE LEVELS:
- HIGH: Multiple clear indicators (product brands + geographic logic + VAT region match)
- MEDIUM: Some indicators with reasonable geographic assumptions
- LOW: Best educated guess based on limited evidence

IMPORTANT: Always attempt to provide city and region even if uncertain. 
Use geographic knowledge about European retail distribution patterns. Prefer real city names over 'UNKNOWN'.

CRITICAL: Return ONLY valid JSON, no markdown formatting, no code blocks, no explanations. Just pure JSON."""

    try:
        response = call_gemini_api_with_retry(prompt)
        
        # Парсим JSON ответ
        try:
            result = clean_and_parse_json(response)
            city_name = result.get('city', '').strip()
            region = result.get('region', '').strip()
            city_population = result.get('city_population')
            confidence = result.get('confidence', 'LOW')
            evidence = result.get('evidence', '')
            
            # ИСПРАВЛЕНИЕ: Извлекаем match_method, zip_code, province_code из ответа AI
            match_method = result.get('match_method')
            ai_zip_code = result.get('zip_code', '').strip() if result.get('zip_code') else None
            ai_province_code = result.get('province_code', '').strip() if result.get('province_code') else None
            
            # Получаем zip_code для всех стран (не только IT)
            shop_chain = None
            shop_address = None
            
            # Используем zip_code из ИИ, если он есть, иначе используем извлеченный из адреса
            final_zip_code = ai_zip_code if ai_zip_code else zip_code
            
            # Для итальянских чеков получаем дополнительные поля
            if country == "IT":
                shop_chain = result.get('shop_chain', '').strip() or None
                shop_address = result.get('shop_address', '').strip() or None
            
            # Для других стран тоже получаем city_population если есть
            if country != "IT" and not city_population:
                city_population = result.get('city_population')
            
            # Преобразуем city_population в int если возможно
            if city_population:
                try:
                    city_population = int(city_population)
                except (ValueError, TypeError):
                    city_population = None
            
            logger.info(f"AI analysis for {api_id}: city={city_name}, region={region}, population={city_population}, confidence={confidence}")
            
        except Exception as e:
            logger.warning(f"Failed to parse JSON response for {api_id}: {e}")
            # Fallback к старому методу
            city_name = response.strip()
            city_name = re.sub(r'[^\wąćęłńóśźżĄĆĘŁŃÓŚŹŻ\s-]', '', city_name).strip()
            city_name = city_name.split()[0] if city_name.split() else ""
            region = None
            city_population = None
        
        if city_name.upper() == 'UNKNOWN':
            return None, None, None, None, None, None, None
        
        # Проверяем город через справочник BigQuery с использованием zip_code для всех стран
        # Используем универсальные таблицы dict_cities_all и dict_regions_all
        if country in SUPPORTED_COUNTRIES:
            # Используем найденный город для поиска в универсальном справочнике с zip_code
            city_result, region_result, region_code_result = lookup_pl_location(city_name, country, final_zip_code)
            logger.info(f"Lookup result for {country}: city={city_result}, region={region_result}, region_code={region_code_result}, zip_code={final_zip_code}")
            return city_result, region_result, region_code_result, city_population, shop_chain, shop_address, final_zip_code
        else:
            # Для неподдерживаемых стран просто возвращаем результат AI
            logger.info(f"Using AI result for {country}: city={city_name}, region={region}")
            return city_name, region, None, city_population, shop_chain, shop_address, final_zip_code
        
    except Exception as e:
        logger.warning(f"Failed to analyze receipt for city: {e}")
        return None, None, None, None, None, None, None


def lookup_pl_location(address: str, country: str = "PL", zip_code: Optional[str] = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Получаем город и region используя универсальные таблицы dict_cities_all и dict_regions_all.
    Поддерживает поиск по zip_code для большей точности.
    """
    # DEBUG: Отслеживание параметра country в lookup
    logger.info(f"LOOKUP DEBUG: address={address}, country={country}, zip_code={zip_code}")
    
    if not address:
        return None, None, None

    original_token = re.split(r"[, ]+", address.strip(), 1)[0] or None
    if not original_token:
        return None, None, None

    norm_candidate = normalise_text(original_token)
    country_code = country.upper() if country else "PL"
    
    # Используем универсальные таблицы для всех стран
    cities_table = DICT_CITIES_ALL_TABLE
    regions_table = DICT_REGIONS_ALL_TABLE

    # Строим SQL запрос с учетом zip_code для большей точности
    if zip_code:
        # Если есть zip_code, используем его для более точного поиска
        sql = f"""
        WITH ranked AS (
          SELECT
            city_name,
            region_code,
            city_norm,
            zip_code,
            CASE 
              WHEN zip_code = @zip_code THEN 1
              WHEN city_norm = @city_norm THEN 2
              ELSE 3
            END AS priority,
            COUNT(*) OVER (
              PARTITION BY country, city_norm, region_code, zip_code
            ) AS rcnt
          FROM `{cities_table}`
          WHERE country = @country_code 
            AND (city_norm = @city_norm OR zip_code = @zip_code)
        )
        SELECT city_name, region_code, zip_code
        FROM ranked
        ORDER BY priority ASC, rcnt DESC, region_code DESC
        LIMIT 1
        """
        
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
                bigquery.ScalarQueryParameter("city_norm", "STRING", norm_candidate),
                bigquery.ScalarQueryParameter("zip_code", "STRING", zip_code),
            ]
        )
    else:
        # Если zip_code нет, используем только city_norm
        sql = f"""
        WITH ranked AS (
          SELECT
            city_name,
            region_code,
            city_norm,
            COUNT(*) OVER (
              PARTITION BY country, city_norm, region_code
            ) AS rcnt
          FROM `{cities_table}`
          WHERE country = @country_code AND city_norm = @city_norm
        )
        SELECT city_name, region_code
        FROM ranked
        ORDER BY rcnt DESC, region_code DESC
        LIMIT 1
        """
        
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
                bigquery.ScalarQueryParameter("city_norm", "STRING", norm_candidate),
            ]
        )

    try:
        rows = list(bq_client.query(sql, job_config=job_cfg))
        if not rows:
            logger.info(f"No city found for norm_candidate: {norm_candidate}, zip_code: {zip_code}")
            return original_token, None, None

        city_name = rows[0].city_name or original_token
        region_code = rows[0].region_code
        
        logger.info(f"City lookup: {original_token} -> city_name={city_name}, region_code={region_code}")
    except Exception as e:
        logger.error(f"Error in city lookup query: {e}")
        return original_token, None, None

    region_name = None
    if region_code:
        sql_reg = (
            f"SELECT region_name "
            f"FROM `{regions_table}` "
            f"WHERE country = @country_code AND region_code = @region_code LIMIT 1"
        )
        job_cfg_reg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
                bigquery.ScalarQueryParameter("region_code", "STRING", region_code),
            ]
        )
        try:
            logger.info(f"Executing region query for region_code: {region_code}")
            reg = list(bq_client.query(sql_reg, job_config=job_cfg_reg))
            logger.info(f"Region query returned {len(reg)} rows")
            
            if reg and reg[0].region_name:
                region_name = reg[0].region_name
                logger.info(f"Region found: {region_code} -> {region_name}")
            else:
                logger.warning(f"Region not found for region_code: {region_code}, reg={reg}")
        except Exception as e:
            logger.error(f"Error fetching region for {region_code}: {e}")

    logger.info(f"Final result: city={city_name}, region={region_name}, region_code={region_code}")
    city_display = city_name if (city_name and len(city_name) >= 3) else original_token
    return city_display, region_name, region_code

def save_corrected_products_to_bq(report_id: str,
                                  report_name: str,
                                  corrected_data: Dict[str, Any]) -> None:
    """Save corrected products to BigQuery tables."""
    rows = []
    now = _now().isoformat()
    for p in corrected_data.get("products", []):
        try:
            row = {
                "report_id": report_id,
                "report_name": report_name,
                "api_id": corrected_data.get("api_id", ""),
                "product_name_original": str(p.get("name_original", ""))[:500],
                "product_name_corrected": str(p.get("name_corrected", ""))[:500],
                "quantity": int(p.get("quantity") or 1),
                "price_single_original": float(p.get("price_single_original") or 0.0),
                "price_single_corrected": float(p.get("price_single_corrected") or 0.0),
                "price_total": float(p.get("price_total") or 0.0),
                "name_correction_made": p.get("name_original") != p.get("name_corrected"),
                "price_correction_made": bool(p.get("price_correction_reason")),
                "price_correction_reason": p.get("price_correction_reason"),
                "created_at": now,
            }
            rows.append(row)
        except Exception as e:
            logger.warning("Skipping product due to error: %s", e)
            continue
    if not rows:
        return

    # Используем Storage Write API для максимальной скорости
    _storage_write_api_load(PRODUCTS_TABLE, rows, report_id, report_name)
    
    vec_rows = [{
        "report_id": r["report_id"],
        "report_name": r["report_name"],
        "api_id": r["api_id"],
        "clean_product_name": r["product_name_corrected"],
        "quantity": r["quantity"],
        "price_single": r["price_single_corrected"],
        "price_total": r["price_total"],
        "created_at": r["created_at"],
    } for r in rows]
    _storage_write_api_load(VECTOR_READY_TABLE, vec_rows, report_id, report_name)

def save_shop_to_bq(report_id: str,
                    report_name: str,
                    api_id: str,
                    country: str,
                    shopnetwork: Optional[str],
                    shop_name: Optional[str],
                    raw_address: Optional[str],
                    nip: Optional[str],
                    products_json: Optional[List[dict]] = None,
                    city: Optional[str] = None,
                    region: Optional[str] = None,
                    region_code: Optional[str] = None,
                    city_population: Optional[int] = None,
                    confidence: Optional[str] = None,
                    evidence: Optional[str] = None,
                    gamification_id: Optional[str] = None) -> None:
    """Save shop metadata to BigQuery."""
    now = _now().isoformat()
    shop_chain = shopnetwork or shop_name or "UNKNOWN"
    
    # Используем переданные параметры города или fallback к старой логике
    if city is None:
        if country in ["PL", "IT", "GB", "HU", "PT", "RO", "FR"]:  # Поддержка всех стран
            # Всегда вызываем AI анализ для определения города и населения
            if products_json:
                logger.info(f"Using AI analysis for city determination for {api_id}")
                city, region, region_code, city_population, ai_shop_chain, ai_shop_address, ai_zip_code = analyze_receipt_for_city(
                api_id=api_id,
                products_json=products_json,
                nip=nip,
                shopnetwork=shopnetwork,
                    country=country,
                    raw_address=raw_address  # Передаем адрес как дополнительную информацию
            )
            if city:
                logger.info(f"AI determined city for {api_id}: {city}, {region}, population={city_population}")
                # Если AI не нашел город - оставляем UNKNOWN (никакого fallback)
                if not city or city == "UNKNOWN":
                    logger.warning(f"AI could not determine city for {api_id}, leaving as UNKNOWN")
                    city = "UNKNOWN"
                    region = "UNKNOWN"
                    region_code = None
            
                raw_address = f"{city}, {raw_address or ''}".strip(', ')
    
    shop_city = city if city else "UNKNOWN"
    shop_region = region if region else "UNKNOWN"
    
    # ИСПРАВЛЕНО: Получаем gamification_id из all_data если не передан
    if not gamification_id:
        try:
            gamification_query = f"""
            SELECT gamification_id
            FROM `{PROJECT_ID}.{DATASET}.all_data`
            WHERE api_id = @api_id
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("api_id", "STRING", api_id)
                ]
            )
            gamification_result = list(bq_client.query(gamification_query, job_config=job_config, location=LOCATION))
            if gamification_result:
                gamification_id = gamification_result[0].gamification_id
        except Exception as e:
            logger.warning(f"Could not get gamification_id from all_data for {api_id}: {e}")
    
    row = {
        "report_id": report_id,
        "report_name": report_name,
        "api_id": api_id,
        "nip": nip or "",
        "shop_chain": normalize_network_name_by_country(shop_chain, country),
        "city": shop_city,
        "region": shop_region,
        "region_code": region_code,
        "shop_address": raw_address[:255] if raw_address else None,
        "city_population": city_population,
        "confidence": confidence or "AUTO",
        "evidence": evidence,
        "country": country,
        "gamification_id": gamification_id,  # ИСПРАВЛЕНО: сохраняем gamification_id
        "created_at": now,
    }
    _storage_write_api_load(SHOP_TABLE, [row], report_id, report_name)

def save_shop_to_bq_with_city(report_id: str,
                              report_name: str,
                              api_id: str,
                              country: str,
                              shopnetwork: Optional[str],
                              shop_name: Optional[str],
                              raw_address: Optional[str],
                              nip: Optional[str],
                              products_json: Optional[List[dict]] = None,
                              city: Optional[str] = None,
                              region: Optional[str] = None,
                              region_code: Optional[str] = None,
                              city_population: Optional[int] = None,
                              confidence: Optional[str] = None,
                              evidence: Optional[str] = None,
                              gamification_id: Optional[str] = None) -> None:
    """Save shop metadata to BigQuery with city data."""
    now = _now().isoformat()
    shop_chain = shopnetwork or shop_name or "UNKNOWN"
    
    shop_city = city if city else "UNKNOWN"
    shop_region = region if region else "UNKNOWN"
    
    # ИСПРАВЛЕНО: Получаем gamification_id из all_data если не передан
    if not gamification_id:
        try:
            gamification_query = f"""
            SELECT gamification_id
            FROM `{PROJECT_ID}.{DATASET}.all_data`
            WHERE api_id = @api_id
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("api_id", "STRING", api_id)
                ]
            )
            gamification_result = list(bq_client.query(gamification_query, job_config=job_config, location=LOCATION))
            if gamification_result:
                gamification_id = gamification_result[0].gamification_id
        except Exception as e:
            logger.warning(f"Could not get gamification_id from all_data for {api_id}: {e}")
    
    row = {
        "report_id": report_id,
        "report_name": report_name,
        "api_id": api_id,
        "nip": nip or "",
        "shop_chain": normalize_network_name_by_country(shop_chain, country),
        "city": shop_city,
        "region": shop_region,
        "region_code": region_code,
        "shop_address": raw_address[:255] if raw_address else None,
        "city_population": city_population,
        "confidence": confidence or "AUTO",
        "evidence": evidence,
        "country": country,
        "gamification_id": gamification_id,  # ИСПРАВЛЕНО: сохраняем gamification_id
        "created_at": now,
    }
    _storage_write_api_load(SHOP_TABLE, [row], report_id, report_name)

def process_single_receipt(api_id: str,
                           products_json: List[dict],
                           total_price: Optional[float],
                           country: str,
                           report_id: str,
                           report_name: str,
                           shopnetwork: Optional[str] = None,
                           shop_name: Optional[str] = None,
                           raw_address: Optional[str] = None,
                           nip: Optional[str] = None,
                           gamification_id: Optional[str] = None) -> Dict[str, Any]:
    """Process a single receipt."""
    logger.info("Processing single receipt: api_id=%s", api_id)
    if country not in SUPPORTED_COUNTRIES:
        raise ValueError(f"Unsupported country code: {country}")
    
    prompt = create_combined_correction_prompt(api_id, products_json, total_price, country, nip, shopnetwork, raw_address)
    gemini_response = call_gemini_api_with_retry(prompt)
    corrected = clean_and_parse_json(gemini_response)
    
    save_corrected_products_to_bq(report_id, report_name, corrected)
    
    save_shop_to_bq(
        report_id=report_id,
        report_name=report_name,
        api_id=api_id,
        country=country,
        shopnetwork=shopnetwork,
        shop_name=shop_name,
        raw_address=raw_address,
        nip=nip,
        products_json=products_json,
        gamification_id=gamification_id,  # ИСПРАВЛЕНО: передаем gamification_id
    )
    return {
        "status": "success",
        "api_id": api_id,
        "corrected_products": len(corrected.get("products", [])),
        "shop_saved": True,
    }

def process_single_receipt_by_id(api_id: str, country: str, report_id: str, report_name: str) -> Dict[str, Any]:
    """Process a single receipt by ID - получает данные из BigQuery и обрабатывает их."""
    logger.info(f"🔄 Processing single receipt by ID: {api_id}")
    
    try:
        # 1. Получаем данные чека из BigQuery
        receipt_query = f"""
        SELECT 
            api_id,
            JSON_VALUE(data, '$.products_string') as products,
            JSON_VALUE(data, '$.sum') as total_price,
            JSON_VALUE(data, '$.shop_name') as shopnetwork,
            JSON_VALUE(data, '$.shop_name') as shop_name,
            JSON_VALUE(data, '$.address') as address,
            JSON_VALUE(data, '$.nip') as nip,
            gamification_id
        FROM `{PROJECT_ID}.{DATASET}.gamification_bills_flat`
        WHERE api_id = @api_id
        AND is_success = 1 
        AND is_finished = true
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("api_id", "STRING", api_id)
            ]
        )
        
        query_job = bq_client.query(receipt_query, job_config=job_config, location=LOCATION)
        results = list(query_job.result())
        
        if not results:
            return {
                "status": "error", 
                "message": f"Receipt with api_id {api_id} not found or not successful"
            }
        
        receipt_data = results[0]
        
        # 2. Автоматически определяем страну если не указана
        if not country:
            country = get_country_from_gamification(receipt_data.gamification_id) or "PL"
            logger.info(f"🌍 Auto-detected country: {country}")
        
        # 3. Парсим JSON данные
        products_json = json.loads(receipt_data.products) if isinstance(receipt_data.products, str) else receipt_data.products
        total_price = float(receipt_data.total_price) if receipt_data.total_price else None
        
        # 4. Обрабатываем чек
        result = process_single_receipt(
            api_id=api_id,
            products_json=products_json,
            total_price=total_price,
            country=country,
            report_id=report_id,
            report_name=report_name,
            shopnetwork=receipt_data.shopnetwork,
            shop_name=receipt_data.shop_name,
            raw_address=receipt_data.address,
            nip=receipt_data.nip,
            gamification_id=receipt_data.gamification_id,  # ИСПРАВЛЕНО: передаем gamification_id из gamification_bills_flat
        )
        
        result["message"] = f"Successfully processed receipt {api_id} from BigQuery"
        return result
        
    except Exception as e:
        logger.error(f"❌ Error processing receipt by ID {api_id}: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Failed to process receipt {api_id}: {str(e)}"
    }

def _extract_text_from_batch_line(obj: dict) -> str:
    """Extract text from Vertex AI batch result."""
    try:
        return obj["predictions"][0]["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        pass
    try:
        return obj["response"]["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        pass
    return ""

def load_receipts_to_fact_scan(country: str, 
                              target_date: Optional[str],
                              report_id: str,
                              report_name: str,
                              since_timestamp: Optional[str] = None,
                              gamification_id: Optional[str] = None,
                              overwrite_mode: bool = False) -> Dict[str, Any]:
    """
    Load ALL receipts from gamification_bills_flat to fact_scan
    
    This includes ALL receipts regardless of is_success status:
    - Successful receipts (is_success = 1, 2, 4)
    
    Args:
        country: Country code (PL, IT, DE, FR)
        target_date: Target date for processing (YYYY-MM-DD)
        report_id: Report ID for tracking
        report_name: Report name for tracking
        since_timestamp: Optional timestamp filter
        gamification_id: Optional specific gamification ID to process
        overwrite_mode: If True, overwrites existing data for the gamification_id
    - Failed receipts (is_success = -5, -4, -3, -2, -1, 0)
    - Temporary receipts (is_success = 3)
    
    Used for complete analytics and error tracking.
    """
    staging_table = None  # Initialize for cleanup in finally block
    try:
        # Date/time filter
        if since_timestamp:
            # Фильтр по времени (для инкрементальной загрузки)
            date_filter = f"AND time_added_ts >= TIMESTAMP '{since_timestamp}'"
        elif target_date:
            # Фильтр по дате (для дневной загрузки)
            date_filter = f"AND time_added_date = DATE '{target_date}'"
        else:
            # Загружаем ВСЕ данные без фильтра по дате
            date_filter = ""
        
        # Gamification ID filter
        gamification_filter = ""
        if gamification_id:
            gamification_filter = f"AND gamification_id = '{gamification_id}'"
            logger.info(f"🎯 Processing specific gamification_id: {gamification_id}")
            
            # Если overwrite_mode = True, удаляем существующие данные для этого промо
            if overwrite_mode:
                logger.info(f"🗑️ Overwrite mode enabled - will delete existing data for gamification_id: {gamification_id}")
                _delete_existing_promo_fact_scan_data(gamification_id)
        
        # БЕЗОПАСНАЯ ЗАГРУЗКА: staging → MERGE для предотвращения дубликатов
        staging_table = f"{FACT_SCAN_TABLE}_staging_{report_id.replace('-', '_')}"
        logger.info(f"Creating staging table {staging_table} for country {country}")
        
        # 1. Создаем staging таблицу с данными
        staging_sql = f"""
        CREATE OR REPLACE TABLE `{staging_table}` (
            scan_id STRING,
            user_id STRING,
            place_id STRING,
            date_id STRING,
            currency_id STRING,
            amount_in_eur FLOAT64,
            amount_in_pln FLOAT64,
            amount_in_usd FLOAT64,
            original_currency_amount FLOAT64,
            promo_currency_amount FLOAT64,
            purchase_price FLOAT64,
            items_count INT64,
            start_scan TIMESTAMP,
            finish_scan TIMESTAMP,
            enter_scan_mode TIMESTAMP,
            scan_duration_seconds INT64,
            shopnetwork STRING,
            error_code STRING,
            error_substatus STRING,
            is_promoted BOOLEAN,
            gain_currency BOOLEAN,
            is_finished BOOL,
            report_id STRING,
            is_success INT64,
            country STRING,
            gamification_id STRING,
            processed_priority INT64,
            ingested_at TIMESTAMP,
            sum FLOAT64,
            products_string STRING,
            total_items_count INT64
        ) AS
        SELECT
            scan_id,
            user_id,
            place_id,
            date_id,
            currency_id,
            amount_in_eur,
            amount_in_pln,
            amount_in_usd,
            original_currency_amount,
            promo_currency_amount,
            purchase_price,
            items_count,
            start_scan,
            finish_scan,
            enter_scan_mode,
            scan_duration_seconds,
            shopnetwork,
            error_code,
            error_substatus,
            is_promoted,
            gain_currency,
            is_finished,
            report_id,
            is_success,
            country,
            gamification_id,
            processed_priority,
            ingested_at,
            sum,
            products_string,
            total_items_count
        FROM (
        SELECT
            b.api_id AS scan_id,
            b.user_id,
            CAST(NULL AS STRING) AS place_id,
            FORMAT_DATE('%Y%m%d', b.time_added_date) AS date_id,
            CASE 
                WHEN b.country = 'PL' THEN 'PLN'
                WHEN b.country = 'GB' THEN 'GBP'
                WHEN b.country = 'RO' THEN 'RON'
                WHEN b.country = 'HU' THEN 'HUF'
                WHEN b.country = 'RS' THEN 'RSD'
                ELSE 'EUR'
            END AS currency_id,
            
            -- Currency amounts
            -- Конвертация в EUR по фиксированным курсам валют
            CASE
                WHEN b.country = 'PL' THEN 
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 4.5  -- PLN to EUR: ~4.5 PLN = 1 EUR
                WHEN b.country = 'GB' THEN
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 0.85  -- GBP to EUR: ~0.85 GBP = 1 EUR
                WHEN b.country = 'RO' THEN
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 5.0  -- RON to EUR: ~5.0 RON = 1 EUR
                WHEN b.country = 'HU' THEN
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 400.0  -- HUF to EUR: ~400 HUF = 1 EUR
                WHEN b.country = 'RS' THEN
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 117.0  -- RSD to EUR: ~117 RSD = 1 EUR
                ELSE SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64)  -- Уже в EUR
            END AS amount_in_eur,
            CASE WHEN b.country = 'PL' THEN SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) ELSE NULL END AS amount_in_pln,
            NULL AS amount_in_usd,
            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS original_currency_amount,
            SAFE_CAST(JSON_VALUE(b.data, '$.points') AS FLOAT64) AS promo_currency_amount,
            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS purchase_price,
            
            -- Items count from products_string array
            -- products_string хранится как JSON строка, поэтому нужно сначала извлечь как VALUE, потом парсить
            CASE
                WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                THEN (
                    SELECT COUNT(*) 
                    FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                )
                ELSE NULL
            END AS items_count,
            
            -- Timestamps
            b.time_added_ts AS start_scan,
            CASE
                WHEN b.is_finished = TRUE
                THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(b.data, '$.time_processed'))
                ELSE NULL
            END AS finish_scan,
            b.time_added_ts AS enter_scan_mode,
            
            -- Duration calculation
            CASE
                WHEN b.is_finished = TRUE AND JSON_VALUE(b.data, '$.time_processed') IS NOT NULL
                THEN DATETIME_DIFF(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(b.data, '$.time_processed')),
                    b.time_added_ts,
                    SECOND
                )
                ELSE NULL
            END AS scan_duration_seconds,
            
            -- Shop and status info
            JSON_VALUE(b.data, '$.shop_name') AS shopnetwork,
            b.status AS error_code,
            b.substatus AS error_substatus,
            
            -- Derived fields
            CASE WHEN b.is_success = 1 THEN TRUE ELSE FALSE END AS is_promoted,
            CASE WHEN SAFE_CAST(JSON_VALUE(b.data, '$.points') AS INT64) > 0 THEN TRUE ELSE FALSE END AS gain_currency,
            b.is_finished,
            
                    -- Report info
                    @report_id AS report_id,
                CAST(COALESCE(b.is_success, 0) AS INT64) AS is_success,
                b.country AS country,  -- Берем из таблицы, не из параметра
                COALESCE(b.gamification_id, '') AS gamification_id,
                
                -- Приоритет: обработанные (1,2,4) важнее необработанных (0,3)
                CAST(1 AS INT64) AS processed_priority,
                CURRENT_TIMESTAMP() AS ingested_at,
                
                -- Новые поля: sum и products_string (STRING для Looker Studio)
                SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS sum,
                -- products_string: только названия продуктов (массив строк)
                CASE
                    WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                    THEN (
                        SELECT TO_JSON_STRING(ARRAY_AGG(JSON_VALUE(product, '$.name')))
                        FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                        WHERE JSON_VALUE(product, '$.name') IS NOT NULL
                    )
                    ELSE NULL
                END AS products_string,
                
                -- Общее количество товаров (total items count)
                -- УМНАЯ ЛОГИКА: number может быть количеством единиц ИЛИ весом в граммах
                -- Если number <= 20 И price_total / number >= 0.5 → number = количество единиц → суммируем
                -- Иначе → number = вес в граммах → считаем как 1 единица
                CASE
                    WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                    THEN (
                        SELECT SUM(
                            CASE
                                WHEN SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64) <= 20
                                 AND SAFE_CAST(JSON_VALUE(product, '$.price_total') AS FLOAT64) / 
                                     NULLIF(SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64), 0) >= 0.5
                                THEN SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64)
                                ELSE 1
                            END
                        )
                        FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                        WHERE JSON_VALUE(product, '$.number') IS NOT NULL
                    )
                    ELSE (
                        -- Fallback: используем количество строк продуктов
                        CASE
                            WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                            THEN (
                                SELECT COUNT(*) 
                                FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                            )
                            ELSE NULL
                        END
                    )
                END AS total_items_count,
                
                -- ДЕДУПЛИКАЦИЯ ПО API_ID: берем самую последнюю запись для каждого api_id
                ROW_NUMBER() OVER (
                    PARTITION BY b.api_id, b.country, b.gamification_id
                    ORDER BY b.time_added_ts DESC, b.read_ts DESC
                ) as rn
            
        FROM `{GAMIFICATION_BILLS_FLAT}` b
        -- Используем фиксированные курсы валют (fallback, так как нет доступа к публичной таблице)
        WHERE b.time_added_date IS NOT NULL
          {date_filter}
              {gamification_filter}
              -- НЕТ фильтра по country - загружаем ВСЕ страны!
        )
        WHERE rn = 1  -- Только первая (самая последняя) запись для каждого api_id
        """
        
        staging_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("report_id", "STRING", report_id)
                # Не передаем country - загружаем ВСЕ страны
            ]
        )
        
        staging_job = bq_client.query(staging_sql, job_config=staging_job_config, location=LOCATION)
        staging_result = staging_job.result()
        
        logger.info(f"Created staging table {staging_table} with {staging_job.num_dml_affected_rows or 0} rows")
        
        # 2. MERGE из staging в целевую таблицу (теперь работает с дедуплицированными данными)
        merge_sql = f"""
        MERGE `{FACT_SCAN_TABLE}` t
        USING `{staging_table}` s
        ON t.scan_id = s.scan_id AND t.country = s.country AND t.gamification_id = s.gamification_id
        
        WHEN MATCHED AND CAST(s.processed_priority AS INT64) > t.processed_priority THEN
            UPDATE SET 
                user_id = s.user_id,
                place_id = s.place_id,
                date_id = s.date_id,
                currency_id = s.currency_id,
                amount_in_eur = s.amount_in_eur,
                amount_in_pln = s.amount_in_pln,
                amount_in_usd = s.amount_in_usd,
                original_currency_amount = s.original_currency_amount,
                promo_currency_amount = s.promo_currency_amount,
                purchase_price = s.purchase_price,
                items_count = s.items_count,
                start_scan = s.start_scan,
                finish_scan = s.finish_scan,
                enter_scan_mode = s.enter_scan_mode,
                scan_duration_seconds = s.scan_duration_seconds,
                shopnetwork = s.shopnetwork,
                error_code = s.error_code,
                error_substatus = s.error_substatus,
                is_promoted = s.is_promoted,
                gain_currency = s.gain_currency,
                is_finished = s.is_finished,
                report_id = s.report_id,
                is_success = CAST(s.is_success AS INT64),
                country = s.country,
                gamification_id = s.gamification_id,
                processed_priority = s.processed_priority,
                ingested_at = s.ingested_at,
                sum = s.sum,
                products_string = s.products_string,
                total_items_count = s.total_items_count
                
        WHEN MATCHED AND CAST(s.processed_priority AS INT64) = t.processed_priority AND s.ingested_at > t.ingested_at THEN
            UPDATE SET 
                user_id = s.user_id,
                place_id = s.place_id,
                date_id = s.date_id,
                currency_id = s.currency_id,
                amount_in_eur = s.amount_in_eur,
                amount_in_pln = s.amount_in_pln,
                amount_in_usd = s.amount_in_usd,
                original_currency_amount = s.original_currency_amount,
                promo_currency_amount = s.promo_currency_amount,
                purchase_price = s.purchase_price,
                items_count = s.items_count,
                start_scan = s.start_scan,
                finish_scan = s.finish_scan,
                enter_scan_mode = s.enter_scan_mode,
                scan_duration_seconds = s.scan_duration_seconds,
                shopnetwork = s.shopnetwork,
                error_code = s.error_code,
                error_substatus = s.error_substatus,
                is_promoted = s.is_promoted,
                gain_currency = s.gain_currency,
                is_finished = s.is_finished,
                report_id = s.report_id,
                is_success = CAST(s.is_success AS INT64),
                country = s.country,
                gamification_id = s.gamification_id,
                processed_priority = s.processed_priority,
                ingested_at = s.ingested_at,
                sum = s.sum,
                products_string = s.products_string,
                total_items_count = s.total_items_count
                
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                scan_id, user_id, place_id, date_id, currency_id, amount_in_eur,
                amount_in_pln, amount_in_usd, original_currency_amount,
                promo_currency_amount, purchase_price, items_count, start_scan,
                finish_scan, enter_scan_mode, scan_duration_seconds, shopnetwork,
                error_code, error_substatus, is_promoted, gain_currency, is_finished, report_id,
                is_success, country, gamification_id, processed_priority, ingested_at,
                sum, products_string, total_items_count
            )
            VALUES (
                s.scan_id, s.user_id, s.place_id, s.date_id, s.currency_id, s.amount_in_eur,
                s.amount_in_pln, s.amount_in_usd, s.original_currency_amount,
                s.promo_currency_amount, s.purchase_price, s.items_count, s.start_scan,
                s.finish_scan, s.enter_scan_mode, s.scan_duration_seconds, s.shopnetwork,
                s.error_code, s.error_substatus, s.is_promoted, s.gain_currency, s.is_finished, s.report_id,
                s.is_success, s.country, s.gamification_id, s.processed_priority, s.ingested_at,
                s.sum, s.products_string, s.total_items_count
            )
        """
        
        merge_job = bq_client.query(merge_sql, location=LOCATION)
        merge_result = merge_job.result()
        
        rows_merged = merge_job.num_dml_affected_rows or 0
        
        logger.info(f"Merged {rows_merged} rows into fact_scan for {country} (deduplicated by api_id)")
        
        return {
            "status": "success",
            "table": "fact_scan",
            "rows_inserted": rows_merged,
            "country": country,
            "target_date": target_date
        }
        
    except Exception as e:
        logger.exception(f"Error loading receipts to fact_scan for {country}")
        return {
            "status": "error",
            "table": "fact_scan",
            "message": str(e),
            "country": country,
            "rows_inserted": 0
        }

    finally:
        # Всегда удаляем staging таблицу, даже если произошла ошибка
        if staging_table:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS `{staging_table}`"
                bq_client.query(cleanup_sql, location=LOCATION).result()
                logger.info(f"Cleaned up staging table {staging_table}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup staging table {staging_table}: {cleanup_error}")

def load_all_promos_to_fact_scan(start_date: str,
                                end_date: str,
                                       report_id: str,
                                report_name: str,
                                overwrite_mode: bool = False) -> Dict[str, Any]:
    """
    Load ALL promotions data from gamification_bills_flat to fact_scan for date range
    
    This processes ALL countries and ALL gamification_ids in the specified date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        report_id: Report ID for tracking
        report_name: Report name for tracking
        overwrite_mode: If True, deletes existing data before loading
    """
    try:
        # Date filter
        date_filter = f"AND time_added_date >= DATE '{start_date}' AND time_added_date <= DATE '{end_date}'"
        
        logger.info(f"🌍 Loading ALL promotions data from {start_date} to {end_date}")
        
        if overwrite_mode:
            logger.info(f"🗑️ Overwrite mode - deleting existing data for date range")
            delete_sql = f"""
            DELETE FROM `{FACT_SCAN_TABLE}`
            WHERE time_added_date >= DATE '{start_date}' 
              AND time_added_date <= DATE '{end_date}'
            """
            delete_job = bq_client.query(delete_sql, location=LOCATION)
            delete_job.result()
            deleted_rows = delete_job.num_dml_affected_rows or 0
            logger.info(f"🗑️ Deleted {deleted_rows} existing rows for date range")
        
        # Get unique countries from the data
        countries_query = f"""
        SELECT DISTINCT country
        FROM `{GAMIFICATION_BILLS_FLAT}`
        WHERE time_added_date >= DATE '{start_date}' 
          AND time_added_date <= DATE '{end_date}'
          AND country IS NOT NULL
        ORDER BY country
        """
        
        countries_result = bq_client.query(countries_query, location=LOCATION)
        countries = [row.country for row in countries_result]
        
        logger.info(f"📊 Found {len(countries)} countries: {countries}")
        
        total_rows = 0
        results_by_country = {}
        
        # Process each country separately
        for country in countries:
            logger.info(f"🔄 Processing country: {country}")
            
            # БЕЗОПАСНАЯ ЗАГРУЗКА: staging → MERGE для каждой страны
            staging_table = f"{FACT_SCAN_TABLE}_staging_{report_id.replace('-', '_')}_{country}"
            
            # 1. Создаем staging таблицу с данными для этой страны
            staging_sql = f"""
            CREATE OR REPLACE TABLE `{staging_table}` (
                scan_id STRING,
                user_id STRING,
                place_id STRING,
                date_id STRING,
                currency_id STRING,
                amount_in_eur FLOAT64,
                amount_in_pln FLOAT64,
                amount_in_usd FLOAT64,
                original_currency_amount FLOAT64,
                promo_currency_amount FLOAT64,
                purchase_price FLOAT64,
                items_count INT64,
                start_scan TIMESTAMP,
                finish_scan TIMESTAMP,
                enter_scan_mode TIMESTAMP,
                scan_duration_seconds INT64,
                shopnetwork STRING,
                error_code STRING,
                error_substatus STRING,
                is_promoted BOOLEAN,
                gain_currency BOOLEAN,
                is_finished BOOL,
                report_id STRING,
                is_success INT64,
                country STRING,
                gamification_id STRING,
                processed_priority INT64,
                ingested_at TIMESTAMP,
                sum FLOAT64,
                products_string STRING,
                total_items_count INT64
            ) AS
            SELECT
                scan_id,
                user_id,
                place_id,
                date_id,
                currency_id,
                amount_in_eur,
                amount_in_pln,
                amount_in_usd,
                original_currency_amount,
                promo_currency_amount,
                purchase_price,
                items_count,
                start_scan,
                finish_scan,
                enter_scan_mode,
                scan_duration_seconds,
                shopnetwork,
                error_code,
                error_substatus,
                is_promoted,
                gain_currency,
                is_finished,
                report_id,
                is_success,
                country,
                gamification_id,
                processed_priority,
                ingested_at,
                sum,
                products_string,
                total_items_count
            FROM (
                SELECT
                    b.api_id AS scan_id,
                    b.user_id,
                    CAST(NULL AS STRING) AS place_id,
                    FORMAT_DATE('%Y%m%d', b.time_added_date) AS date_id,
                    CASE 
                        WHEN b.country = 'PL' THEN 'PLN'
                        WHEN b.country = 'GB' THEN 'GBP'
                        WHEN b.country = 'RO' THEN 'RON'
                        WHEN b.country = 'HU' THEN 'HUF'
                        WHEN b.country = 'RS' THEN 'RSD'
                        ELSE 'EUR'
                    END AS currency_id,
                    
                    -- Currency amounts
                    -- Конвертация в EUR по фиксированным курсам валют
                    CASE
                        WHEN b.country = 'PL' THEN 
                            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 4.5  -- PLN to EUR: ~4.5 PLN = 1 EUR
                        WHEN b.country = 'GB' THEN
                            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 0.85  -- GBP to EUR: ~0.85 GBP = 1 EUR
                        WHEN b.country = 'RO' THEN
                            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 5.0  -- RON to EUR: ~5.0 RON = 1 EUR
                        WHEN b.country = 'HU' THEN
                            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 400.0  -- HUF to EUR: ~400 HUF = 1 EUR
                        WHEN b.country = 'RS' THEN
                            SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) / 117.0  -- RSD to EUR: ~117 RSD = 1 EUR
                        ELSE SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64)  -- Уже в EUR
                    END AS amount_in_eur,
                    CASE WHEN b.country = 'PL' THEN SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) ELSE NULL END AS amount_in_pln,
                    NULL AS amount_in_usd,
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS original_currency_amount,
                    SAFE_CAST(JSON_VALUE(b.data, '$.points') AS FLOAT64) AS promo_currency_amount,
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS purchase_price,
                    
                    -- Items count from products_string array
                    -- products_string хранится как JSON строка, поэтому нужно сначала извлечь как VALUE, потом парсить
                    CASE
                        WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                        THEN (
                            SELECT COUNT(*) 
                            FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                        )
                        ELSE NULL
                    END AS items_count,
                    
                    -- Timestamps
                    b.time_added_ts AS start_scan,
                    CASE
                        WHEN b.is_finished = TRUE
                        THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(b.data, '$.time_processed'))
                        ELSE NULL
                    END AS finish_scan,
                    b.time_added_ts AS enter_scan_mode,
                    
                    -- Duration calculation
                    CASE
                        WHEN b.is_finished = TRUE AND JSON_VALUE(b.data, '$.time_processed') IS NOT NULL
                        THEN DATETIME_DIFF(
                            SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(b.data, '$.time_processed')),
                            b.time_added_ts,
                            SECOND
                        )
                        ELSE NULL
                    END AS scan_duration_seconds,
                    
                    -- Shop and status info
                    JSON_VALUE(b.data, '$.shop_name') AS shopnetwork,
                    b.status AS error_code,
                    b.substatus AS error_substatus,
                    
                    -- Derived fields
                    CASE WHEN b.is_success = 1 THEN TRUE ELSE FALSE END AS is_promoted,
                    CASE WHEN SAFE_CAST(JSON_VALUE(b.data, '$.points') AS INT64) > 0 THEN TRUE ELSE FALSE END AS gain_currency,
                    b.is_finished,
                    
                    -- Report info
                    '{report_id}' AS report_id,
                    CAST(COALESCE(b.is_success, 0) AS INT64) AS is_success,
                    b.country,
                    COALESCE(b.gamification_id, '') AS gamification_id,
                    
                    -- Приоритет: обработанные (1,2,4) важнее необработанных (0,3)
                    CAST(1 AS INT64) AS processed_priority,
                    CURRENT_TIMESTAMP() AS ingested_at,
                    
                    -- Новые поля: sum и products_string (STRING для Looker Studio)
                    SAFE_CAST(JSON_VALUE(b.data, '$.sum') AS FLOAT64) AS sum,
                    -- products_string: только названия продуктов (массив строк)
                    CASE
                        WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                        THEN (
                            SELECT TO_JSON_STRING(ARRAY_AGG(JSON_VALUE(product, '$.name')))
                            FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                            WHERE JSON_VALUE(product, '$.name') IS NOT NULL
                        )
                        ELSE NULL
                    END AS products_string,
                    
                    -- Общее количество товаров (total items count)
                    -- УМНАЯ ЛОГИКА: number может быть количеством единиц ИЛИ весом в граммах
                    -- Если number <= 20 И price_total / number >= 0.5 → number = количество единиц → суммируем
                    -- Иначе → number = вес в граммах → считаем как 1 единица
                    CASE
                        WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                        THEN (
                            SELECT SUM(
                                CASE
                                    WHEN SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64) <= 20
                                     AND SAFE_CAST(JSON_VALUE(product, '$.price_total') AS FLOAT64) / 
                                         NULLIF(SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64), 0) >= 0.5
                                    THEN SAFE_CAST(JSON_VALUE(product, '$.number') AS INT64)
                                    ELSE 1
                                END
                            )
                            FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                            WHERE JSON_VALUE(product, '$.number') IS NOT NULL
                        )
                        ELSE (
                            -- Fallback: используем количество строк продуктов
                            CASE
                                WHEN JSON_VALUE(b.data, '$.products_string') IS NOT NULL
                                THEN (
                                    SELECT COUNT(*) 
                                    FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(b.data, '$.products_string'))) AS product
                                )
                                ELSE NULL
                            END
                        )
                    END AS total_items_count,
                    
                    -- ДЕДУПЛИКАЦИЯ ПО API_ID: берем самую последнюю запись для каждого api_id
                    ROW_NUMBER() OVER (
                        PARTITION BY b.api_id, b.country, b.gamification_id
                        ORDER BY b.time_added_ts DESC, b.read_ts DESC
                    ) as rn
                    
                FROM `{GAMIFICATION_BILLS_FLAT}` b
                -- Используем фиксированные курсы валют (fallback, так как нет доступа к публичной таблице)
                WHERE b.time_added_date IS NOT NULL
                  AND b.country = '{country}'
                  {date_filter}
            )
            WHERE rn = 1  -- Только первая (самая последняя) запись для каждого api_id
            """
            
            staging_job = bq_client.query(staging_sql, location=LOCATION)
            staging_result = staging_job.result()
            
            country_rows = staging_job.num_dml_affected_rows or 0
            logger.info(f"Created staging table {staging_table} with {country_rows} rows for {country}")
            
            # 2. MERGE из staging в целевую таблицу
            merge_sql = f"""
            MERGE `{FACT_SCAN_TABLE}` t
            USING `{staging_table}` s
            ON t.scan_id = s.scan_id AND t.country = s.country AND t.gamification_id = s.gamification_id
            
            WHEN MATCHED AND CAST(s.processed_priority AS INT64) > t.processed_priority THEN
                UPDATE SET 
                    user_id = s.user_id,
                    place_id = s.place_id,
                    date_id = s.date_id,
                    currency_id = s.currency_id,
                    amount_in_eur = s.amount_in_eur,
                    amount_in_pln = s.amount_in_pln,
                    amount_in_usd = s.amount_in_usd,
                    original_currency_amount = s.original_currency_amount,
                    promo_currency_amount = s.promo_currency_amount,
                    purchase_price = s.purchase_price,
                    items_count = s.items_count,
                    start_scan = s.start_scan,
                    finish_scan = s.finish_scan,
                    enter_scan_mode = s.enter_scan_mode,
                    scan_duration_seconds = s.scan_duration_seconds,
                    shopnetwork = s.shopnetwork,
                    error_code = s.error_code,
                    error_substatus = s.error_substatus,
                    is_promoted = s.is_promoted,
                    gain_currency = s.gain_currency,
                    report_id = s.report_id,
                    is_success = CAST(s.is_success AS INT64),
                    country = s.country,
                    gamification_id = s.gamification_id,
                    processed_priority = s.processed_priority,
                    ingested_at = s.ingested_at,
                    sum = s.sum,
                    products_string = s.products_string,
                    total_items_count = s.total_items_count
                    
            WHEN MATCHED AND CAST(s.processed_priority AS INT64) = t.processed_priority AND s.ingested_at > t.ingested_at THEN
                UPDATE SET 
                    user_id = s.user_id,
                    place_id = s.place_id,
                    date_id = s.date_id,
                    currency_id = s.currency_id,
                    amount_in_eur = s.amount_in_eur,
                    amount_in_pln = s.amount_in_pln,
                    amount_in_usd = s.amount_in_usd,
                    original_currency_amount = s.original_currency_amount,
                    promo_currency_amount = s.promo_currency_amount,
                    purchase_price = s.purchase_price,
                    items_count = s.items_count,
                    start_scan = s.start_scan,
                    finish_scan = s.finish_scan,
                    enter_scan_mode = s.enter_scan_mode,
                    scan_duration_seconds = s.scan_duration_seconds,
                    shopnetwork = s.shopnetwork,
                    error_code = s.error_code,
                    error_substatus = s.error_substatus,
                    is_promoted = s.is_promoted,
                    gain_currency = s.gain_currency,
                    report_id = s.report_id,
                    is_success = CAST(s.is_success AS INT64),
                    country = s.country,
                    gamification_id = s.gamification_id,
                    processed_priority = s.processed_priority,
                    ingested_at = s.ingested_at,
                    sum = s.sum,
                    products_string = s.products_string,
                    total_items_count = s.total_items_count
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    scan_id, user_id, place_id, date_id, currency_id, amount_in_eur,
                    amount_in_pln, amount_in_usd, original_currency_amount,
                    promo_currency_amount, purchase_price, items_count, start_scan,
                    finish_scan, enter_scan_mode, scan_duration_seconds, shopnetwork,
                    error_code, error_substatus, is_promoted, gain_currency, is_finished, report_id,
                    is_success, country, gamification_id, processed_priority, ingested_at,
                    sum, products_string, total_items_count
                )
                VALUES (
                    s.scan_id, s.user_id, s.place_id, s.date_id, s.currency_id, s.amount_in_eur,
                    s.amount_in_pln, s.amount_in_usd, s.original_currency_amount,
                    s.promo_currency_amount, s.purchase_price, s.items_count, s.start_scan,
                    s.finish_scan, s.enter_scan_mode, s.scan_duration_seconds, s.shopnetwork,
                    s.error_code, s.error_substatus, s.is_promoted, s.gain_currency, s.is_finished, s.report_id,
                    s.is_success, s.country, s.gamification_id, s.processed_priority, s.ingested_at,
                    s.sum, s.products_string, s.total_items_count
                )
            """
            
            merge_job = bq_client.query(merge_sql, location=LOCATION)
            merge_result = merge_job.result()
            
            country_merged = merge_job.num_dml_affected_rows or 0
            logger.info(f"Merged {country_merged} rows into fact_scan for {country}")
            
            total_rows += country_merged
            results_by_country[country] = {
                "staging_rows": country_rows,
                "merged_rows": country_merged
            }
            
            # Cleanup staging table
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS `{staging_table}`"
                bq_client.query(cleanup_sql, location=LOCATION).result()
                logger.info(f"Cleaned up staging table {staging_table}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup staging table {staging_table}: {cleanup_error}")
        
        logger.info(f"✅ Completed loading ALL promotions data: {total_rows} total rows across {len(countries)} countries")
        
        return {
            "status": "success",
            "table": "fact_scan",
            "total_rows_inserted": total_rows,
            "countries_processed": len(countries),
            "countries": countries,
            "results_by_country": results_by_country,
            "date_range": f"{start_date} to {end_date}"
        }
        
    except Exception as e:
        logger.exception(f"Error loading all promotions to fact_scan")
        return {
            "status": "error",
            "table": "fact_scan",
            "message": str(e),
            "total_rows_inserted": 0
        }

def load_successful_receipts_to_all_data(country: str, 
                                       target_date: Optional[str],
                                       report_id: str,
                                       report_name: str,
                                       since_timestamp: Optional[str] = None) -> Dict[str, Any]:
    """
    Load only SUCCESSFUL receipts from gamification_bills_flat to all_data
    
    Only includes receipts with is_success IN (2, 4):
    - 2: accepted by moderator  
    - 4: synchronized with CCA and points given
    
    Additional filters:
    - is_finished = true
    - points > 0
    - country IN SUPPORTED_COUNTRIES  -- ВСЕ поддерживаемые страны (24 страны)
    
    Excludes:
    - Automation accepted (is_success = 1) - requires moderator approval
    - Failed receipts (is_success = -5, -4, -3, -2, -1, 0)
    - Temporary receipts (is_success = 3)
    
    These receipts will be processed further (AI correction, product matching, aggregates).
    """
    staging_table = None  # Initialize for cleanup in finally block
    try:
        # Date/time filter - ЗАЩИТА ОТ СЛУЧАЙНОЙ ЗАГРУЗКИ ВСЕХ ДАННЫХ
        if since_timestamp:
            # Фильтр по времени (для инкрементальной загрузки)
            date_filter = f"AND time_added_ts >= TIMESTAMP '{since_timestamp}'"
        elif target_date:
            # Фильтр по дате (для дневной загрузки)
            date_filter = f"AND time_added_date = DATE '{target_date}'"
        else:
            # КРИТИЧЕСКАЯ ЗАЩИТА: не загружаем все данные без фильтра
            logger.error("КРИТИЧЕСКАЯ ОШИБКА: Попытка загрузить ВСЕ данные без фильтра по дате/времени!")
            return {
                "status": "error",
                "table": "all_data",
                "message": "Нельзя загружать все данные без фильтра по дате или времени. Укажите target_date или since_timestamp.",
                "country": country,
                "rows_inserted": 0
            }
        
        # БЕЗОПАСНАЯ ЗАГРУЗКА: staging → MERGE для предотвращения дубликатов
        staging_table = f"{ALL_DATA_TABLE}_staging_{report_id.replace('-', '_')}"
        logger.info(f"Creating staging table {staging_table} for all supported countries (not just {country})")
        
        # Формируем список всех поддерживаемых стран для SQL
        countries_list = ', '.join([f"'{c}'" for c in SUPPORTED_COUNTRIES])
        logger.info(f"Loading data for all supported countries: {', '.join(SUPPORTED_COUNTRIES)}")
        
        # 1. Создаем staging таблицу с данными
        staging_sql = f"""
        CREATE OR REPLACE TABLE `{staging_table}` (
            api_id STRING,
            user_id STRING,
            event_ts TIMESTAMP,
            event_date DATE,
            source_collection STRING,
            gamification_id STRING,
            bill_image STRING,
            status STRING,
            substatus STRING,
            raw_doc JSON,
            country STRING,
            shopnetwork STRING,
            shop_name STRING,
            processed_priority INT64,
            ingested_at TIMESTAMP
        ) AS
        SELECT
            api_id,
            user_id,
            time_added_ts AS event_ts,
            time_added_date AS event_date,
            'gamification_bills' AS source_collection,
            gamification_id,
            bill_image,
            status,
            substatus,
            data AS raw_doc,
            s.country AS country,  -- Используем country из таблицы, а не параметр
            JSON_VALUE(data, '$.shop_name') AS shopnetwork,
            JSON_VALUE(data, '$.shop_name') AS shop_name,
            
            -- Приоритет: обработанные (1,2,4) важнее необработанных (0,3)
            CAST(1 AS INT64) AS processed_priority,
            CURRENT_TIMESTAMP() AS ingested_at
            
        FROM `{GAMIFICATION_BILLS_FLAT}` s
        WHERE s.time_added_date IS NOT NULL
          AND s.is_success IN (2, 4)  -- ТОЛЬКО: 2=moderator, 4=synchronized (БЕЗ 1=automation!)
          AND s.is_finished = true  -- Only finished receipts
          AND CAST(JSON_VALUE(s.data, '$.points') AS INT64) > 0  -- Есть поинты (gained currency)
          AND s.country IN ({countries_list})  -- ВСЕ поддерживаемые страны (24 страны)
          {date_filter}
          -- Убрали фильтр по products_string - берем ВСЕ чеки с points > 0
        """
        
        # Подготовка параметров для staging (БЕЗ country - фильтруем в WHERE)
        staging_params = []
        
        if since_timestamp:
            staging_params.append(bigquery.ScalarQueryParameter("since_timestamp", "TIMESTAMP", since_timestamp))
        elif target_date:
            staging_params.append(bigquery.ScalarQueryParameter("target_date", "DATE", target_date))
        
        staging_job_config = bigquery.QueryJobConfig(query_parameters=staging_params)
        
        staging_job = bq_client.query(staging_sql, job_config=staging_job_config, location=LOCATION)
        staging_result = staging_job.result()
        
        logger.info(f"Created staging table {staging_table} with {staging_job.num_dml_affected_rows or 0} rows")
        
        # 2. MERGE из staging в целевую таблицу
        merge_sql = f"""
        MERGE `{ALL_DATA_TABLE}` t
        USING `{staging_table}` s
        ON t.api_id = s.api_id AND t.country = s.country
        
        WHEN MATCHED AND CAST(s.processed_priority AS INT64) > t.processed_priority THEN
            UPDATE SET 
                user_id = s.user_id,
                event_ts = s.event_ts,
                event_date = s.event_date,
                source_collection = s.source_collection,
                gamification_id = s.gamification_id,
                bill_image = s.bill_image,
                status = s.status,
                substatus = s.substatus,
                raw_doc = s.raw_doc,
                shopnetwork = s.shopnetwork,
                shop_name = s.shop_name,
                processed_priority = s.processed_priority,
                ingested_at = s.ingested_at
                
        WHEN MATCHED AND CAST(s.processed_priority AS INT64) = t.processed_priority AND s.ingested_at > t.ingested_at THEN
            UPDATE SET 
                user_id = s.user_id,
                event_ts = s.event_ts,
                event_date = s.event_date,
                source_collection = s.source_collection,
                gamification_id = s.gamification_id,
                bill_image = s.bill_image,
                status = s.status,
                substatus = s.substatus,
                raw_doc = s.raw_doc,
                shopnetwork = s.shopnetwork,
                shop_name = s.shop_name,
                processed_priority = s.processed_priority,
                ingested_at = s.ingested_at
                
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                api_id, user_id, event_ts, event_date, source_collection, gamification_id,
                bill_image, status, substatus, raw_doc, country, shopnetwork, shop_name,
                processed_priority, ingested_at
            )
            VALUES (
                s.api_id, s.user_id, s.event_ts, s.event_date, s.source_collection, s.gamification_id,
                s.bill_image, s.status, s.substatus, s.raw_doc, s.country, s.shopnetwork, s.shop_name,
                s.processed_priority, s.ingested_at
            )
        """
        
        merge_job = bq_client.query(merge_sql, location=LOCATION)
        merge_result = merge_job.result()
        
        rows_merged = merge_job.num_dml_affected_rows or 0
        
        logger.info(f"Merged {rows_merged} successful receipts into all_data for ALL supported countries (no duplicates)")
        
        return {
            "status": "success",
            "table": "all_data",
            "rows_inserted": rows_merged,
            "countries": SUPPORTED_COUNTRIES,  # Возвращаем список всех обработанных стран
            "target_date": target_date
        }
        
    except Exception as e:
        logger.exception(f"Error loading successful receipts to all_data for all supported countries")
        return {
            "status": "error",
            "table": "all_data",
            "message": str(e),
            "countries": SUPPORTED_COUNTRIES,
            "rows_inserted": 0
        }
        
    finally:
        # Всегда удаляем staging таблицу, даже если произошла ошибка
        if staging_table:
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS `{staging_table}`"
                bq_client.query(cleanup_sql, location=LOCATION).result()
                logger.info(f"Cleaned up staging table {staging_table}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup staging table {staging_table}: {cleanup_error}")

class BatchReceiptProcessor:
    """Batch processing of receipts using Vertex AI Batch Inference with complete workflow."""
    
    def __init__(self) -> None:
        self.project_id = PROJECT_ID
        self.dataset = DATASET
        self.gamification_bills_table = ALL_DATA_TABLE
        self.bq = bq_client
        self.bucket_name = BUCKET_NAME
        logger.info("BatchReceiptProcessor initialised: dataset=%s", DATASET)

    def create_batch_job_async(self,
                                       report_id: str,
                                       report_name: str,
                                       countries: List[str] = None,
                                       target_date: str = None,
                              date_from: str = None,
                              date_to: str = None,
                              no_date_filter: bool = False,
                              limit: int = None,
                                       test_mode: bool = False) -> Dict[str, Any]:
        """
        Создать batch job и вернуть информацию о нем без ожидания завершения.
        Аналогично старому коду для асинхронного режима.
        """
        logger.info("=== ASYNC BATCH JOB CREATION START ===")
        logger.info(f"Parameters: countries={countries}, target_date={target_date}, limit={limit}")
        
        if countries:
            for c in countries:
                if c not in SUPPORTED_COUNTRIES:
                    raise ValueError(f"Unsupported country code in batch: {c}")
        
        # Query receipts from all_data (same logic as complete method)
        where_conditions = ["raw_doc IS NOT NULL"]
        query_params = []

        # ИСПРАВЛЕННАЯ ЛОГИКА ФИЛЬТРАЦИИ ПО ДАТЕ
        if no_date_filter:
            # Обрабатываем ВСЕ чеки без фильтра по дате
            logger.info("Processing ALL receipts without date filter")
        elif target_date:
            # Конкретная дата
            where_conditions.append("event_date = @target_date")
            query_params.append(bigquery.ScalarQueryParameter("target_date", "DATE", target_date))
            logger.info(f"Processing receipts for date: {target_date}")
        elif date_from or date_to:
            # Диапазон дат
            if date_from:
                where_conditions.append("event_date >= @date_from")
                query_params.append(bigquery.ScalarQueryParameter("date_from", "DATE", date_from))
            if date_to:
                where_conditions.append("event_date <= @date_to")
                query_params.append(bigquery.ScalarQueryParameter("date_to", "DATE", date_to))
            logger.info(f"Processing receipts from {date_from} to {date_to}")
        else:
            # ПО УМОЛЧАНИЮ: последние 30 дней (а не 24 часа!)
            date_from_default = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
            where_conditions.append("event_date >= @date_from")
            query_params.append(bigquery.ScalarQueryParameter("date_from", "DATE", date_from_default))
            logger.info(f"Processing receipts from last 30 days: {date_from_default}")

        # Добавляем фильтр по странам если указаны
        if countries:
            country_list = "', '".join(countries)
            where_conditions.append(f"country IN ('{country_list}')")

        where_clause = " AND ".join(where_conditions)

        # Добавляем LIMIT если указан
        limit_clause = ""
        if limit and limit > 0:
            limit_clause = f"LIMIT {limit}"
            logger.info(f"Applying limit: {limit} receipts")

        sql = f"""
        SELECT
          ad.api_id,
          ad.user_id,
          JSON_VALUE(ad.raw_doc, '$.products_string') AS products_str,
          COALESCE(JSON_VALUE(ad.raw_doc, '$.sum'),
                   JSON_VALUE(ad.raw_doc, '$.total')) AS total_str,
          JSON_VALUE(ad.raw_doc, '$.address') AS address,
          JSON_VALUE(ad.raw_doc, '$.nip') AS nip,
          ad.shopnetwork,
          ad.shop_name,
          ad.event_ts,
          ad.event_date,
          JSON_VALUE(ad.raw_doc, '$.is_success') AS is_success,
          ad.status,
          ad.substatus,
          ad.gamification_id,
          ad.country
        FROM `{self.gamification_bills_table}` ad
        WHERE {where_clause}
          AND JSON_VALUE(ad.raw_doc, '$.is_success') IN ('1', '2', '4')  -- Success: 1=automation, 2=moderator, 4=synchronized
          AND JSON_VALUE(ad.raw_doc, '$.is_finished') = 'true'  -- Only finished receipts
          AND JSON_VALUE(ad.raw_doc, '$.products_string') IS NOT NULL
          AND JSON_VALUE(ad.raw_doc, '$.products_string') != '[]'
        ORDER BY ad.event_ts DESC
        {limit_clause}
        """
        
        job_cfg = bigquery.QueryJobConfig(query_parameters=query_params)
        df = self.bq.query(sql, job_config=job_cfg).to_dataframe()

        logger.info(f"Found {len(df)} total rows from BigQuery (limit={limit if limit else 'none'})")

        if df.empty:
            return {"status": "error", "message": "no data found for batch processing"}

        # Prepare batch data (same logic as complete method)
        shop_data_map = {}
        jsonl_lines = []
        processed_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                api_id = str(row["api_id"])
                products_str = row["products_str"]
                
                if not products_str or pd.isna(products_str):
                    skipped_count += 1
                    continue
                
                try:
                    products_json = json.loads(products_str)
                except json.JSONDecodeError as e:
                    skipped_count += 1
                    continue
                
                if not isinstance(products_json, list):
                    skipped_count += 1
                    continue
                    
                valid_products = []
                for p in products_json:
                    if isinstance(p, dict) and p.get('name'):
                        valid_products.append(p)
                
                if not valid_products:
                    skipped_count += 1
                    continue
                
                total_price = None
                if row.get("total_str"):
                    try:
                        total_price = float(row["total_str"])
                    except (ValueError, TypeError):
                        pass
                
                # Получаем реальную страну чека из данных
                receipt_country = row.get("country", "PL")
                
                # Проверяем, что страна чека входит в список запрашиваемых стран
                if countries and receipt_country not in countries:
                    skipped_count += 1
                    continue
                
                # Store shop data for later
                shop_data_map[api_id] = {
                    'country': receipt_country,
                    'shopnetwork': row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    'shop_name': row["shop_name"] if pd.notna(row["shop_name"]) else None,
                    'address': row["address"] if pd.notna(row["address"]) else None,
                    'nip': row["nip"] if pd.notna(row["nip"]) else None,
                    'gamification_id': row["gamification_id"] if pd.notna(row["gamification_id"]) else None,  # ИСПРАВЛЕНО: добавляем gamification_id
                    'products_json': valid_products
                }
                
                # ИСПРАВЛЕНИЕ: Используем комбинированный промпт для продуктов И города
                logger.info(f"Creating combined prompt for {api_id} in {receipt_country}")
                prompt = create_combined_correction_prompt(
                    api_id=api_id,
                    products_json=valid_products,
                    total_price=total_price,
                    country_code=receipt_country,
                    nip=row["nip"] if pd.notna(row["nip"]) else None,
                    shopnetwork=row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    raw_address=row["address"] if pd.notna(row["address"]) else None
                )

                jsonl_line = {
                        "request": {
                            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "temperature": 0, 
                                "maxOutputTokens": 8192, 
                                "candidateCount": 1,
                                "responseMimeType": "application/json"
                            }
                        }
                }
                jsonl_lines.append(json.dumps(jsonl_line))
                processed_count += 1
                
            except Exception as e:
                skipped_count += 1
                logger.error(f"Failed to prepare batch data for api_id={row.get('api_id')}: {e}")
                continue

        logger.info(f"Batch preparation summary: {processed_count} processed, {skipped_count} skipped from {len(df)} total rows")

        if not jsonl_lines:
            return {"status": "error", "message": "no valid receipts found for batch processing"}
        
        # Upload JSONL to GCS
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        input_filename = f"batch_async_input_{report_id}_{timestamp}.jsonl"
        output_prefix = f"batch_async_output_{report_id}_{timestamp}"
        
        input_uri = f"gs://{self.bucket_name}/{input_filename}"
        output_uri = f"gs://{self.bucket_name}/{output_prefix}/"
        
        # Upload input file
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(input_filename)
        blob.upload_from_string("\n".join(jsonl_lines), content_type="application/jsonl")
        logger.info("Uploaded %s lines to %s", len(jsonl_lines), input_uri)
        
        # Create batch prediction job
        job_display_name = f"receipt-batch-async-{report_id}-{timestamp}"
        
        if test_mode:
            test_lines = jsonl_lines[:5]  # Limit for testing
            test_filename = f"test_{input_filename}"
            test_uri = f"gs://{self.bucket_name}/{test_filename}"
            test_blob = bucket.blob(test_filename)
            test_blob.upload_from_string("\n".join(test_lines), content_type="application/jsonl")
            input_uri = test_uri
            logger.info("Test mode: processing only %s lines", len(test_lines))
        
        batch_job = aiplatform.BatchPredictionJob.create(
            job_display_name=job_display_name,
            model_name="publishers/google/models/gemini-2.0-flash-001",
            instances_format="jsonl",
            predictions_format="jsonl",
            gcs_source=[input_uri],
            gcs_destination_prefix=output_uri
        )
        
        logger.info("Created async batch job: %s", batch_job.resource_name)
        
        return {
            "status": "created",
            "message": "Batch job created and running",
            "job_name": batch_job.resource_name,
            "report_id": report_id,
            "report_name": report_name,
            "job_display_name": job_display_name,
            "input_uri": input_uri,
            "output_uri": output_uri,
            "processing_summary": {
                "total_rows": len(df),
                "processed": processed_count,
                "skipped": skipped_count
            }
        }

    def process_batch_receipts_complete(self,
                                       report_id: str,
                                       report_name: str,
                                       countries: List[str] = None,
                                       target_date: str = None,
                                       date_from: str = None,
                                       date_to: str = None,
                                       since_timestamp: str = None,  # ДОБАВЛЯЕМ since_timestamp!
                                       no_date_filter: bool = False,
                                       limit: int = None,
                                       test_mode: bool = False) -> Dict[str, Any]:
        """
        Complete batch processing: create job, wait for completion, process results.
        """
        logger.info("=== COMPLETE BATCH PROCESSING START ===")
        logger.info(f"Parameters: countries={countries}, target_date={target_date}, limit={limit}")
        
        start_time = time.time()
        
        # Фильтруем только поддерживаемые страны
        if countries:
            unsupported = [c for c in countries if c not in SUPPORTED_COUNTRIES]
            if unsupported:
                logger.warning(f"⚠️ Ignoring unsupported countries: {unsupported}")
            countries = [c for c in countries if c in SUPPORTED_COUNTRIES]
            if not countries:
                logger.warning("❌ No supported countries in request, using all supported countries")
                countries = None  # Будет обработано все
        
        # Query receipts from all_data
        where_conditions = ["raw_doc IS NOT NULL"]
        query_params = []

        # ИСПРАВЛЕННАЯ ЛОГИКА ФИЛЬТРАЦИИ ПО ДАТЕ ДЛЯ ALL_DATA
        if no_date_filter:
            # Обрабатываем ВСЕ чеки без фильтра по дате
            logger.info("Processing ALL receipts without date filter")
        elif since_timestamp:
            # Фильтр по времени (для инкрементальной загрузки)
            where_conditions.append("event_ts >= @since_timestamp")
            query_params.append(bigquery.ScalarQueryParameter("since_timestamp", "TIMESTAMP", since_timestamp))
            logger.info(f"Processing receipts since: {since_timestamp}")
        elif target_date:
            # Конкретная дата
            where_conditions.append("event_date = @target_date")
            query_params.append(bigquery.ScalarQueryParameter("target_date", "DATE", target_date))
            logger.info(f"Processing receipts for date: {target_date}")
        elif date_from or date_to:
            # Диапазон дат
            if date_from:
                where_conditions.append("event_date >= @date_from")
                query_params.append(bigquery.ScalarQueryParameter("date_from", "DATE", date_from))
            if date_to:
                where_conditions.append("event_date <= @date_to")
                query_params.append(bigquery.ScalarQueryParameter("date_to", "DATE", date_to))
            logger.info(f"Processing receipts from {date_from} to {date_to}")
        else:
            # ПО УМОЛЧАНИЮ: ВСЕ данные (а не последние 24 часа!)
            logger.info("Processing ALL receipts (default behavior)")

        # Добавляем фильтр по странам если указаны
        if countries:
            country_list = "', '".join(countries)
            where_conditions.append(f"country IN ('{country_list}')")

        where_clause = " AND ".join(where_conditions)

        # Добавляем LIMIT если указан
        limit_clause = ""
        if limit and limit > 0:
            limit_clause = f"LIMIT {limit}"
            logger.info(f"Applying limit: {limit} receipts")

        sql = f"""
        SELECT
          ad.api_id,
          ad.user_id,
          JSON_VALUE(ad.raw_doc, '$.products_string') AS products_str,
          COALESCE(JSON_VALUE(ad.raw_doc, '$.sum'),
                   JSON_VALUE(ad.raw_doc, '$.total')) AS total_str,
          JSON_VALUE(ad.raw_doc, '$.address') AS address,
          JSON_VALUE(ad.raw_doc, '$.nip') AS nip,
          ad.shopnetwork,
          ad.shop_name,
          ad.event_ts,
          ad.event_date,
          JSON_VALUE(ad.raw_doc, '$.is_success') AS is_success,
          ad.status,
          ad.substatus,
          ad.gamification_id,
          ad.country
        FROM `{self.gamification_bills_table}` ad
        WHERE {where_clause}
          AND JSON_VALUE(ad.raw_doc, '$.is_success') IN ('1', '2', '4')  -- Success: 1=automation, 2=moderator, 4=synchronized
          AND JSON_VALUE(ad.raw_doc, '$.is_finished') = 'true'  -- Only finished receipts
          AND JSON_VALUE(ad.raw_doc, '$.products_string') IS NOT NULL
          AND JSON_VALUE(ad.raw_doc, '$.products_string') != '[]'
        ORDER BY ad.event_ts DESC
        {limit_clause}
        """
        
        job_cfg = bigquery.QueryJobConfig(query_parameters=query_params)
        df = self.bq.query(sql, job_config=job_cfg).to_dataframe()

        logger.info(f"Found {len(df)} total rows from BigQuery (limit={limit if limit else 'none'})")

        if df.empty:
            return {"status": "error", "message": "no data found for batch processing"}

        # Store metadata for later use
        shop_data_map = {}
        jsonl_lines = []
        processed_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                api_id = str(row["api_id"])
                products_str = row["products_str"]
                
                if not products_str or pd.isna(products_str):
                    skipped_count += 1
                    continue
                
                try:
                    products_json = json.loads(products_str)
                except json.JSONDecodeError as e:
                    skipped_count += 1
                    continue
                
                if not isinstance(products_json, list):
                    skipped_count += 1
                    continue
                    
                valid_products = []
                for p in products_json:
                    if isinstance(p, dict) and p.get('name'):
                        valid_products.append(p)
                
                if not valid_products:
                    skipped_count += 1
                    continue
                
                total_price = None
                if row.get("total_str"):
                    try:
                        total_price = float(row["total_str"])
                    except (ValueError, TypeError):
                        pass
                
                # Получаем реальную страну чека из данных
                receipt_country = row.get("country", "PL")
                
                # Проверяем, что страна чека входит в список запрашиваемых стран
                if countries and receipt_country not in countries:
                    skipped_count += 1
                    continue
                
                # Store shop data for later
                shop_data_map[api_id] = {
                    'country': receipt_country,
                    'shopnetwork': row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    'shop_name': row["shop_name"] if pd.notna(row["shop_name"]) else None,
                    'address': row["address"] if pd.notna(row["address"]) else None,
                    'nip': row["nip"] if pd.notna(row["nip"]) else None,
                    'gamification_id': row["gamification_id"] if pd.notna(row["gamification_id"]) else None,  # ИСПРАВЛЕНО: добавляем gamification_id
                    'products_json': valid_products
                }
                
                # ИСПРАВЛЕНИЕ: Используем комбинированный промпт для продуктов И города
                logger.info(f"Creating combined prompt for {api_id} in {receipt_country}")
                prompt = create_combined_correction_prompt(
                    api_id=api_id,
                    products_json=valid_products,
                    total_price=total_price,
                    country_code=receipt_country,
                    nip=row["nip"] if pd.notna(row["nip"]) else None,
                    shopnetwork=row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    raw_address=row["address"] if pd.notna(row["address"]) else None
                )

                jsonl_line = {
                    "request": {
                        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "temperature": 0, 
                            "maxOutputTokens": 8192, 
                            "candidateCount": 1,
                            "responseMimeType": "application/json"
                        }
                    }
                }
                jsonl_lines.append(json.dumps(jsonl_line))
                processed_count += 1
                
            except Exception as e:
                skipped_count += 1
                logger.error(f"Failed to prepare batch data for api_id={row.get('api_id')}: {e}")
                continue

        logger.info(f"Batch processing summary: {processed_count} processed, {skipped_count} skipped from {len(df)} total rows")

        if not jsonl_lines:
            return {"status": "error", "message": "no valid receipts found for batch processing"}
        
        # Обрабатываем все чеки без ограничений
        logger.info(f"Processing {len(jsonl_lines)} receipts in batch job")
        
        # Upload JSONL to GCS
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        input_filename = f"batch_input_{report_id}_{timestamp}.jsonl"
        output_prefix = f"batch_output_{report_id}_{timestamp}"
        
        input_uri = f"gs://{self.bucket_name}/{input_filename}"
        output_uri = f"gs://{self.bucket_name}/{output_prefix}/"
        
        # Upload input file
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(input_filename)
        blob.upload_from_string("\n".join(jsonl_lines), content_type="application/jsonl")
        logger.info("Uploaded %s lines to %s", len(jsonl_lines), input_uri)
        
        # Create batch prediction job
        job_display_name = f"receipt-batch-{report_id}-{timestamp}"
        
        if test_mode:
            test_lines = jsonl_lines[:5]  # Limit for testing
            test_filename = f"test_{input_filename}"
            test_uri = f"gs://{self.bucket_name}/{test_filename}"
            test_blob = bucket.blob(test_filename)
            test_blob.upload_from_string("\n".join(test_lines), content_type="application/jsonl")
            input_uri = test_uri
            logger.info("Test mode: processing only %s lines", len(test_lines))
        
        batch_job = aiplatform.BatchPredictionJob.create(
            job_display_name=job_display_name,
            model_name="publishers/google/models/gemini-2.0-flash-001",
            instances_format="jsonl",
            predictions_format="jsonl",
            gcs_source=[input_uri],
            gcs_destination_prefix=output_uri
        )
        
        logger.info("Created batch job: %s", batch_job.resource_name)
        logger.info("Waiting for batch job completion...")
        
        # ИСПРАВЛЕНИЕ: Использовать простой wait() как в старом коде
        try:
            # Простое ожидание завершения как в старом коде
            batch_job.wait()
            logger.info("Batch job completed successfully!")
        except Exception as e:
            return {"status": "error", "message": f"Batch job failed: {str(e)}"}
        
        # Проверить финальное состояние
        state = batch_job.state
        if JobState and state == JobState.JOB_STATE_SUCCEEDED:
            logger.info("Job succeeded")
        elif "SUCCEEDED" in str(state):
            logger.info("Job succeeded (string check)")
        else:
            return {"status": "error", "message": f"Job failed with state: {state}"}
        
        # PROCESS RESULTS IMMEDIATELY - ИСПРАВЛЕННАЯ ЛОГИКА КАК В СТАРОМ КОДЕ
        logger.info("Processing batch job results...")
        
        output_location = batch_job.output_info.gcs_output_directory
        
        # НАКОПЛЕНИЕ ДАННЫХ ДЛЯ БАТЧЕВОЙ ЗАГРУЗКИ
        accumulated_products = []
        accumulated_shops = []
        accumulated_vector_products = []
        bucket_name = output_location.replace("gs://", "").split("/")[0]
        prefix = "/".join(output_location.replace("gs://", "").split("/")[1:])
        
        bucket = storage_client.bucket(bucket_name)
        
        logger.info(f"Processing results from: gs://{bucket_name}/{prefix}")
        
        total_processed = 0
        total_products = 0
        stats = {
            "successful_products": 0,
            "successful_shops": 0,
            "failed_products": 0,
            "failed_shops": 0,
            "price_corrections": 0,
            "name_corrections": 0
        }
        
        # Обработка всех .jsonl файлов как в старом коде
        result_files_processed = 0
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith('.jsonl'):
                logger.info(f"Processing result file: {blob.name}")
                try:
                    content = blob.download_as_text()
                    result_files_processed += 1
                    
                    for line_num, line in enumerate(content.strip().split('\n'), 1):
                        if not line.strip():
                            continue
                        
                        try:
                            result = json.loads(line)
                            response_text = _extract_text_from_batch_line(result)
                            
                            if not response_text:
                                logger.warning(f"No response text in line {line_num} of {blob.name}")
                                continue
                            
                            corrected_data = clean_and_parse_json(response_text)
                            
                            # Save products
                            if corrected_data.get("products"):
                                try:
                                    # НАКОПЛЯЕМ ДАННЫЕ ВМЕСТО НЕМЕДЛЕННОГО СОХРАНЕНИЯ
                                    now = _now().isoformat()
                                    for p in corrected_data.get("products", []):
                                        try:
                                            product_row = {
                                                "report_id": report_id,
                                                "report_name": report_name,
                                                "api_id": corrected_data.get("api_id", ""),
                                                "product_name_original": str(p.get("name_original", ""))[:500],
                                                "product_name_corrected": str(p.get("name_corrected", ""))[:500],
                                                "quantity": int(p.get("quantity") or 1),
                                                "price_single_original": float(p.get("price_single_original") or 0.0),
                                                "price_single_corrected": float(p.get("price_single_corrected") or 0.0),
                                                "price_total": float(p.get("price_total") or 0.0),
                                                "name_correction_made": p.get("name_original") != p.get("name_corrected"),
                                                "price_correction_made": bool(p.get("price_correction_reason")),
                                                "price_correction_reason": p.get("price_correction_reason"),
                                                "created_at": now,
                                            }
                                            accumulated_products.append(product_row)
                                            
                                            # Vector products
                                            vector_row = {
                                                "report_id": report_id,
                                                "report_name": report_name,
                                                "api_id": corrected_data.get("api_id", ""),
                                                "clean_product_name": str(p.get("name_corrected", ""))[:500],
                                                "quantity": int(p.get("quantity") or 1),
                                                "price_single": float(p.get("price_single_corrected") or 0.0),
                                                "price_total": float(p.get("price_total") or 0.0),
                                                "created_at": now,
                                            }
                                            accumulated_vector_products.append(vector_row)
                                            
                                        except Exception as e:
                                            logger.warning("Skipping product due to error: %s", e)
                                            continue
                                    
                                    stats['successful_products'] += 1
                                    
                                    for product in corrected_data.get('products', []):
                                        if product.get('name_original') != product.get('name_corrected'):
                                            stats['name_corrections'] += 1
                                        if product.get('price_single_original') != product.get('price_single_corrected'):
                                            stats['price_corrections'] += 1
                                    
                                    total_products += len(corrected_data.get('products', []))
                                except Exception as e:
                                    stats['failed_products'] += 1
                                    logger.error(f"Failed to process products for line {line_num}: {e}")
                            
                            # ИСПРАВЛЕНИЕ: Извлекаем данные о городе из комбинированного ответа
                            api_id = corrected_data.get('api_id')
                            if api_id:
                                # Найти shop_info по api_id
                                shop_info = shop_data_map.get(api_id)
                        
                                if shop_info:
                                    # Извлекаем данные о городе из комбинированного ответа
                                    city_analysis = corrected_data.get('city_analysis', {})
                                    ai_city = city_analysis.get('city')
                                    ai_region = city_analysis.get('region')
                                    city_population = city_analysis.get('city_population')
                                    confidence = city_analysis.get('confidence', 'LOW')
                                    evidence_text = city_analysis.get('evidence', '')
                                    
                                    # ИСПРАВЛЕНИЕ: Извлекаем match_method, zip_code, province_code из city_analysis
                                    match_method = city_analysis.get('match_method')
                                    ai_zip_code = city_analysis.get('zip_code')
                                    ai_province_code = city_analysis.get('province_code')
                            
                                    # ИСПРАВЛЕНИЕ: Нормализуем город через справочники как в старой версии
                                    if ai_city and ai_city != 'UNKNOWN':
                                        # Нормализуем через справочники
                                        normalized_city, normalized_region, region_code = lookup_pl_location(ai_city, shop_info.get('country', 'PL'))
                                
                                        if normalized_city:
                                            # Используем нормализованные значения
                                            final_city = normalized_city
                                            final_region = normalized_region
                                            final_region_code = region_code
                                            logger.info(f"Normalized city for {api_id}: {ai_city} -> {final_city}, region: {final_region}")
                                        else:
                                            # Используем значения от AI
                                            final_city = ai_city
                                            final_region = ai_region
                                            final_region_code = None
                                            logger.warning(f"Could not normalize city {ai_city} for {api_id}, using AI result")
                                
                                        # ИСПРАВЛЕНИЕ: Создаем JSON объект для evidence с match_method, zip_code, province_code
                                        evidence_dict = {
                                            'match_method': match_method,
                                            'zip_code': ai_zip_code,
                                            'province_code': ai_province_code,
                                            'confidence': confidence,
                                            'evidence': evidence_text
                                        }
                                        evidence_json = json.dumps(evidence_dict, ensure_ascii=False)
                                
                                        shop_info.update({
                                            'city': final_city,
                                            'region': final_region,
                                            'region_code': final_region_code,
                                            'city_population': city_population,
                                            'confidence': confidence,
                                            'evidence': evidence_json,
                                            'match_method': match_method,
                                            'zip_code': ai_zip_code,
                                            'province_code': ai_province_code
                                        })
                                        logger.info(f"Final city for {api_id}: {final_city}, {final_region}, population={city_population}, confidence={confidence}")
                                    else:
                                        logger.warning(f"AI could not determine city for {api_id}")
                            
                                    try:
                                        # НАКОПЛЯЕМ SHOP ДАННЫЕ (используем правильные названия полей)
                                        shop_chain = shop_info.get('shopnetwork') or shop_info.get('shop_name') or "UNKNOWN"
                                        shop_row = {
                                            "report_id": report_id,
                                            "report_name": report_name,
                                            "api_id": api_id,
                                            "nip": shop_info.get('nip') or "",
                                            "shop_chain": normalize_network_name_by_country(shop_chain, shop_info.get('country', 'PL')),
                                            "city": shop_info.get('city') or "UNKNOWN",
                                            "region": shop_info.get('region') or "UNKNOWN",
                                            "region_code": shop_info.get('region_code'),
                                            "shop_address": shop_info.get('address')[:255] if shop_info.get('address') else None,
                                            "city_population": parse_city_population(shop_info.get('city_population')),
                                            "country": shop_info.get('country', 'PL'),
                                            "gamification_id": shop_info.get('gamification_id'),
                                            "confidence": shop_info.get('confidence') or "AUTO",
                                            "evidence": shop_info.get('evidence'),
                                            "match_method": shop_info.get('match_method'),
                                            "zip_code": shop_info.get('zip_code'),
                                            "province_code": shop_info.get('province_code'),
                                            "created_at": _now().isoformat(),
                                        }
                                        accumulated_shops.append(shop_row)
                                        stats['successful_shops'] += 1
                                    except Exception as e:
                                        stats['failed_shops'] += 1
                                        logger.error(f"Failed to process shop for line {line_num}: {e}")
                                else:
                                    logger.warning(f"No shop data found for api_id {api_id} in line {line_num}")
                            
                            total_processed += 1
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error in line {line_num} of {blob.name}: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing line {line_num}: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Error processing result file {blob.name}: {e}")
                    continue
        
        logger.info(f"Processed {result_files_processed} result files, {total_processed} total receipts")
        
        # ФИНАЛЬНАЯ БАТЧЕВАЯ ЗАГРУЗКА ВСЕХ НАКОПЛЕННЫХ ДАННЫХ
        logger.info("🚀 STARTING FINAL BATCH LOAD OF ACCUMULATED DATA")
        logger.info("📊 Products: %s, Shops: %s, Vector Products: %s", 
                   len(accumulated_products), len(accumulated_shops), len(accumulated_vector_products))
        
        try:
            # Загружаем продукты большим батчем
            if accumulated_products:
                logger.info("🔄 Loading %s products to corrected_products", len(accumulated_products))
                _storage_write_api_load(PRODUCTS_TABLE, accumulated_products, report_id, report_name)
            
            # Загружаем векторные продукты большим батчем
            if accumulated_vector_products:
                logger.info("🔄 Loading %s vector products to products_vector_ready", len(accumulated_vector_products))
                _storage_write_api_load(VECTOR_READY_TABLE, accumulated_vector_products, report_id, report_name)
            
            # Загружаем магазины большим батчем
            if accumulated_shops:
                logger.info("🔄 Loading %s shops to shop_directory", len(accumulated_shops))
                _storage_write_api_load(SHOP_TABLE, accumulated_shops, report_id, report_name)
            
            logger.info("✅ ALL ACCUMULATED DATA LOADED SUCCESSFULLY")
            
        except Exception as e:
            logger.error("❌ FAILED TO LOAD ACCUMULATED DATA: %s", e)
            raise
        
        processing_time = int(time.time() - start_time)
        
        return {
            "status": "success",
            "message": f"Complete batch processing finished! Processed {total_processed} receipts",
            "report_id": report_id,
            "report_name": report_name,
            "processing_time_seconds": processing_time,
            "batch_job_name": batch_job.resource_name,
            "receipts_processed": total_processed,
            "products_saved": total_products,
            "stats": stats,
            "processing_summary": {
                "total_rows": len(df),
                "processed": processed_count,
                "skipped": skipped_count
            }
        }

    def process_batch_by_ids(self,
                            report_id: str,
                            report_name: str,
                            api_ids: List[str],
                            country: str = "PL",
                            test_mode: bool = False) -> Dict[str, Any]:
        """Process specific receipts by their api_ids with complete workflow."""
        logger.info("=== BATCH BY IDS COMPLETE PROCESSING START ===")
        logger.info(f"Processing {len(api_ids)} specific api_ids")
        
        start_time = time.time()
        
        if not api_ids:
            return {"status": "error", "message": "No api_ids provided"}
        
        # Query receipts for specific api_ids
        placeholders = ','.join([f'@api_id_{i}' for i in range(len(api_ids))])
        
        sql = f"""
        SELECT
          ad.api_id,
          ad.user_id,
          JSON_VALUE(ad.raw_doc, '$.products_string') AS products_str,
          COALESCE(JSON_VALUE(ad.raw_doc, '$.sum'),
                   JSON_VALUE(ad.raw_doc, '$.total')) AS total_str,
          JSON_VALUE(ad.raw_doc, '$.address') AS address,
          JSON_VALUE(ad.raw_doc, '$.nip') AS nip,
          ad.shopnetwork,
          ad.shop_name,
          ad.event_ts,
          ad.event_date,
          JSON_VALUE(ad.raw_doc, '$.is_success') AS is_success,
          ad.status,
          ad.substatus,
          ad.gamification_id,
          ad.country
        FROM `{self.gamification_bills_table}` ad
        WHERE ad.api_id IN ({placeholders})
        ORDER BY ad.api_id
        """
        
        query_params = [
            bigquery.ScalarQueryParameter(f"api_id_{i}", "STRING", api_id)
            for i, api_id in enumerate(api_ids)
        ]
        query_params.append(bigquery.ScalarQueryParameter("country", "STRING", country))
        
        job_cfg = bigquery.QueryJobConfig(query_parameters=query_params)
        df = self.bq.query(sql, job_config=job_cfg).to_dataframe()

        logger.info(f"Found {len(df)} records for {len(api_ids)} requested IDs")
        
        if df.empty:
            return {"status": "error", "message": f"No data found for provided api_ids"}

        # Check which IDs were not found
        found_ids = set(df['api_id'].tolist())
        missing_ids = set(api_ids) - found_ids
        if missing_ids:
            logger.warning(f"Missing api_ids: {list(missing_ids)}")

        # Process same as batch_receipts_complete but for specific IDs
        shop_data_map = {}
        jsonl_lines = []
        processed_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                api_id = str(row["api_id"])
                products_str = row["products_str"]
                
                if not products_str or pd.isna(products_str):
                    skipped_count += 1
                    continue
                
                try:
                    products_json = json.loads(products_str)
                except json.JSONDecodeError:
                    skipped_count += 1
                    continue
                
                if not isinstance(products_json, list):
                    skipped_count += 1
                    continue
                    
                valid_products = [p for p in products_json if isinstance(p, dict) and p.get('name')]
                
                if not valid_products:
                    skipped_count += 1
                    continue
                
                # Получаем реальную страну чека из данных
                receipt_country = row.get("country", country)
                
                # Store shop data using real country from data
                shop_data_map[api_id] = {
                    'country': receipt_country,
                    'shopnetwork': row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    'shop_name': row["shop_name"] if pd.notna(row["shop_name"]) else None,
                    'address': row["address"] if pd.notna(row["address"]) else None,
                    'nip': row["nip"] if pd.notna(row["nip"]) else None,
                    'gamification_id': row["gamification_id"] if pd.notna(row["gamification_id"]) else None,  # ИСПРАВЛЕНО: добавляем gamification_id
                    'products_json': valid_products
                }
                
                # ИСПРАВЛЕНИЕ: Используем комбинированный промпт для продуктов И города
                logger.info(f"Creating combined prompt for {api_id} in {receipt_country}")
                prompt = create_combined_correction_prompt(
                    api_id=api_id,
                    products_json=valid_products,
                    total_price=total_price,
                    country_code=receipt_country,
                    nip=row["nip"] if pd.notna(row["nip"]) else None,
                    shopnetwork=row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    raw_address=row["address"] if pd.notna(row["address"]) else None
                )
                
                # Create JSONL line for batch processing
                jsonl_lines.append(json.dumps({
                    "request": {
                        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "temperature": 0,
                            "maxOutputTokens": 8192,
                            "candidateCount": 1,
                            "responseMimeType": "application/json"
                        }
                    }
                }))
                
                total_price = None
                if row.get("total_str"):
                    try:
                        total_price = float(row["total_str"])
                    except (ValueError, TypeError):
                        pass
                
                # ИСПРАВЛЕНИЕ: Используем комбинированный промпт для продуктов И города
                prompt = create_combined_correction_prompt(
                    api_id=api_id,
                    products_json=valid_products,
                    total_price=total_price,
                    country_code=receipt_country,
                    nip=row["nip"] if pd.notna(row["nip"]) else None,
                    shopnetwork=row["shopnetwork"] if pd.notna(row["shopnetwork"]) else None,
                    raw_address=row["address"] if pd.notna(row["address"]) else None
                )

                jsonl_line = {
                    "request": {
                        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "temperature": 0, 
                            "maxOutputTokens": 8192, 
                            "candidateCount": 1,
                            "responseMimeType": "application/json"
                        }
                    }
                }
                jsonl_lines.append(json.dumps(jsonl_line))
                processed_count += 1
                
            except Exception as e:
                skipped_count += 1
                logger.error(f"Failed to prepare batch data for api_id={row.get('api_id')}: {e}")
                continue

        if not jsonl_lines:
            return {"status": "error", "message": "no valid receipts found for batch processing from requested IDs"}
        
        # Create and wait for batch job (same as above)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        input_filename = f"batch_ids_input_{report_id}_{timestamp}.jsonl"
        output_prefix = f"batch_ids_output_{report_id}_{timestamp}"
        
        input_uri = f"gs://{self.bucket_name}/{input_filename}"
        output_uri = f"gs://{self.bucket_name}/{output_prefix}/"
        
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(input_filename)
        blob.upload_from_string("\n".join(jsonl_lines), content_type="application/jsonl")
        
        job_display_name = f"receipt-batch-ids-{report_id}-{timestamp}"
        
        if test_mode:
            test_lines = jsonl_lines[:3]
            test_filename = f"test_{input_filename}"
            test_uri = f"gs://{self.bucket_name}/{test_filename}"
            test_blob = bucket.blob(test_filename)
            test_blob.upload_from_string("\n".join(test_lines), content_type="application/jsonl")
            input_uri = test_uri
        
        batch_job = aiplatform.BatchPredictionJob.create(
            job_display_name=job_display_name,
            model_name="publishers/google/models/gemini-2.0-flash-001",
            instances_format="jsonl",
            predictions_format="jsonl",
            gcs_source=[input_uri],
            gcs_destination_prefix=output_uri
        )
        
        logger.info("Created batch job, waiting for completion...")
        
        # ИСПРАВЛЕНИЕ: Использовать простой wait() как в старом коде
        try:
            # Простое ожидание завершения как в старом коде
            batch_job.wait()
            logger.info("Batch job completed successfully!")
        except Exception as e:
            return {"status": "error", "message": f"Batch job failed: {str(e)}"}
        
        # Проверить финальное состояние
        state = batch_job.state
        if JobState and state == JobState.JOB_STATE_SUCCEEDED:
            logger.info("Job succeeded")
        elif "SUCCEEDED" in str(state):
            logger.info("Job succeeded (string check)")
        else:
            return {"status": "error", "message": f"Job failed with state: {state}"}
        
        # Process results (same logic as above)
        output_location = batch_job.output_info.gcs_output_directory
        bucket_name = output_location.replace("gs://", "").split("/")[0]
        prefix = "/".join(output_location.replace("gs://", "").split("/")[1:])
        
        bucket = storage_client.bucket(bucket_name)
        
        total_processed = 0
        total_products = 0
        
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith('.jsonl'):
                content = blob.download_as_text()
                for line in content.strip().split('\n'):
                    if not line.strip():
                        continue
                    
                    try:
                        result = json.loads(line)
                        response_text = _extract_text_from_batch_line(result)
                        
                        if response_text:
                            corrected_data = clean_and_parse_json(response_text)
                            
                            # Products уже обработаны выше в цикле
                            
                            # ИСПРАВЛЕНИЕ: Извлекаем данные о городе из комбинированного ответа
                            api_id = corrected_data.get('api_id')
                            if api_id and api_id in shop_data_map:
                                shop_info = shop_data_map[api_id]
                                
                                # Извлекаем данные о городе из комбинированного ответа
                                city_analysis = corrected_data.get('city_analysis', {})
                                ai_city = city_analysis.get('city')
                                ai_region = city_analysis.get('region')
                                city_population = city_analysis.get('city_population')
                                confidence = city_analysis.get('confidence', 'LOW')
                                evidence_text = city_analysis.get('evidence', '')
                                
                                # ИСПРАВЛЕНИЕ: Извлекаем match_method, zip_code, province_code из city_analysis
                                match_method = city_analysis.get('match_method')
                                ai_zip_code = city_analysis.get('zip_code')
                                ai_province_code = city_analysis.get('province_code')
                                
                                # ИСПРАВЛЕНИЕ: Нормализуем город через справочники как в старой версии
                                if ai_city and ai_city != 'UNKNOWN':
                                    # Нормализуем через справочники
                                    normalized_city, normalized_region, region_code = lookup_pl_location(ai_city, shop_info.get('country', 'PL'))
                                    
                                    if normalized_city:
                                        # Используем нормализованные значения
                                        final_city = normalized_city
                                        final_region = normalized_region
                                        final_region_code = region_code
                                        logger.info(f"Normalized city for {api_id}: {ai_city} -> {final_city}, region: {final_region}")
                                    else:
                                        # Используем значения от AI
                                        final_city = ai_city
                                        final_region = ai_region
                                        final_region_code = None
                                        logger.warning(f"Could not normalize city {ai_city} for {api_id}, using AI result")
                                    
                                    # ИСПРАВЛЕНИЕ: Создаем JSON объект для evidence с match_method, zip_code, province_code
                                    evidence_dict = {
                                        'match_method': match_method,
                                        'zip_code': ai_zip_code,
                                        'province_code': ai_province_code,
                                        'confidence': confidence,
                                        'evidence': evidence_text
                                    }
                                    evidence_json = json.dumps(evidence_dict, ensure_ascii=False)
                                    
                                    shop_info.update({
                                        'city': final_city,
                                        'region': final_region,
                                        'region_code': final_region_code,
                                        'city_population': city_population,
                                        'confidence': confidence,
                                        'evidence': evidence_json,
                                        'match_method': match_method,
                                        'zip_code': ai_zip_code,
                                        'province_code': ai_province_code
                                    })
                                    logger.info(f"Final city for {api_id}: {final_city}, {final_region}, population={city_population}, confidence={confidence}")
                                else:
                                    logger.warning(f"AI could not determine city for {api_id}")
                                
                                save_shop_to_bq(
                                    report_id=report_id,
                                    report_name=report_name,
                                    api_id=api_id,
                                    country=shop_info.get('country', country),
                                    shopnetwork=shop_info.get('shopnetwork'),
                                    shop_name=shop_info.get('shop_name'),
                                    raw_address=shop_info.get('address'),
                                    nip=shop_info.get('nip'),
                                    products_json=shop_info.get('products_json', []),
                                    gamification_id=shop_info.get('gamification_id')  # ИСПРАВЛЕНО: передаем gamification_id
                                )
                            
                            total_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing result: {e}")
                        continue
        
        processing_time = int(time.time() - start_time)
        
        return {
            "status": "success",
            "message": f"Complete batch IDs processing finished! Processed {total_processed} receipts",
            "report_id": report_id,
            "report_name": report_name,
            "processing_time_seconds": processing_time,
            "batch_job_name": batch_job.resource_name,
            "requested_ids": len(api_ids),
            "found_ids": len(df),
            "missing_ids": list(missing_ids) if missing_ids else [],
            "receipts_processed": total_processed,
            "products_saved": total_products
        }

    def process_batch_results_from_gcs(self, report_id: str, report_name: str, input_uri: str) -> Dict[str, Any]:
        """Process batch results from GCS predictions.jsonl file."""
        logger.info(f"Processing batch results from GCS: {input_uri}")
        start_time = time.time()
        
        try:
            # Download and process the predictions.jsonl file
            import google.cloud.storage as storage
            
            # Parse GCS URI
            if not input_uri.startswith('gs://'):
                return {"status": "error", "message": "input_uri must be a GCS URI (gs://...)"}
            
            bucket_name = input_uri.split('/')[2]
            blob_name = '/'.join(input_uri.split('/')[3:])
            
            # Download file from GCS
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                return {"status": "error", "message": f"File not found: {input_uri}"}
            
            # Read the file content
            content = blob.download_as_text()
            lines = content.strip().split('\n')
            
            logger.info(f"Processing {len(lines)} prediction results from GCS")
            logger.info(f"First few lines preview:")
            for i, line in enumerate(lines[:3]):
                logger.info(f"Line {i+1}: {line[:200]}...")
            
            # Детальная диагностика
            total_lines = len(lines)
            processed_successfully = 0
            failed_parsing = 0
            failed_processing = 0
            
            # НАКОПЛЕНИЕ ДАННЫХ ДЛЯ БАТЧЕВОЙ ЗАГРУЗКИ
            accumulated_products = []
            accumulated_shops = []
            accumulated_vector_products = []
            
            # Process each prediction result
            total_processed = 0
            total_products = 0
            total_shops = 0
            
            for line_num, line in enumerate(lines, 1):
                try:
                    if not line.strip():
                        continue
                    
                    # Parse JSONL line
                    prediction_data = json.loads(line)
                    
                    if line_num <= 3:
                        logger.info(f"Line {line_num} keys: {list(prediction_data.keys())}")
                    
                    # Extract response text - handle both structures
                    response_text = None
                    
                    # Try new structure first (from our batch results)
                    if 'response' in prediction_data and 'candidates' in prediction_data['response']:
                        candidates = prediction_data['response']['candidates']
                        if candidates and len(candidates) > 0 and 'content' in candidates[0] and 'parts' in candidates[0]['content'] and len(candidates[0]['content']['parts']) > 0:
                            response_text = candidates[0]['content']['parts'][0]['text']
                    
                    # Try old structure (direct text field)
                    elif 'text' in prediction_data:
                        response_text = prediction_data['text']
                    
                    # Try alternative structure
                    elif 'content' in prediction_data and 'parts' in prediction_data['content']:
                        response_text = prediction_data['content']['parts'][0]['text']
                    
                    # Process if we found response text
                    if response_text:
                        logger.info(f"Found response text for line {line_num}, length: {len(response_text)}")
                        # Clean and parse the response
                        corrected_data = clean_and_parse_json(response_text)
                        
                        if corrected_data:
                            logger.info(f"Successfully parsed JSON for line {line_num}")
                            processed_successfully += 1
                            api_id = corrected_data.get('api_id')
                            if api_id:
                                # ИСПРАВЛЕНО: Логируем начало обработки shop для диагностики
                                logger.debug(f"Processing shop for api_id: {api_id}")
                                
                                # НАКОПЛЯЕМ ПРОДУКТЫ
                                if corrected_data.get('products'):
                                    now = _now().isoformat()
                                    for p in corrected_data.get("products", []):
                                        try:
                                            product_row = {
                                                "report_id": report_id,
                                                "report_name": report_name,
                                                "api_id": corrected_data.get("api_id", ""),
                                                "product_name_original": str(p.get("name_original", ""))[:500],
                                                "product_name_corrected": str(p.get("name_corrected", ""))[:500],
                                                "quantity": int(p.get("quantity") or 1),
                                                "price_single_original": float(p.get("price_single_original") or 0.0),
                                                "price_single_corrected": float(p.get("price_single_corrected") or 0.0),
                                                "price_total": float(p.get("price_total") or 0.0),
                                                "name_correction_made": p.get("name_original") != p.get("name_corrected"),
                                                "price_correction_made": bool(p.get("price_correction_reason")),
                                                "price_correction_reason": p.get("price_correction_reason"),
                                                "created_at": now,
                                            }
                                            accumulated_products.append(product_row)
                                            
                                            # Vector products
                                            vector_row = {
                                                "report_id": report_id,
                                                "report_name": report_name,
                                                "api_id": corrected_data.get("api_id", ""),
                                                "clean_product_name": str(p.get("name_corrected", ""))[:500],
                                                "quantity": int(p.get("quantity") or 1),
                                                "price_single": float(p.get("price_single_corrected") or 0.0),
                                                "price_total": float(p.get("price_total") or 0.0),
                                                "created_at": now,
                                            }
                                            accumulated_vector_products.append(vector_row)
                                            
                                        except Exception as e:
                                            logger.warning("Skipping product due to error: %s", e)
                                            continue
                                    
                                    total_products += len(corrected_data['products'])
                                
                                # ИСПРАВЛЕНО: Обработка shop данных с полной обработкой ошибок
                                try:
                                    # Save shop data with city analysis and normalization
                                    city_analysis = corrected_data.get('city_analysis')
                                    
                                    if city_analysis:
                                        # Существующий код - обработка когда city_analysis есть
                                        ai_city = city_analysis.get('city', 'UNKNOWN')
                                        ai_region = city_analysis.get('region', 'UNKNOWN')
                                        city_population = city_analysis.get('city_population', None)
                                        confidence = city_analysis.get('confidence', 'LOW')
                                        evidence = city_analysis.get('evidence', 'AI city analysis')
                                    else:
                                        # Fallback когда city_analysis отсутствует
                                        logger.warning(f"Missing city_analysis for api_id: {corrected_data.get('api_id')}")
                                        ai_city = 'UNKNOWN'
                                        ai_region = 'UNKNOWN'
                                        city_population = None
                                        confidence = 'LOW'
                                        evidence = 'No city analysis available'
                                    
                                    # Try to determine country from original request data
                                    country = "PL"  # Default fallback
                                    
                                    # Extract country from original request if available
                                    if 'request' in prediction_data and 'contents' in prediction_data['request']:
                                        request_contents = prediction_data['request']['contents']
                                        if request_contents and len(request_contents) > 0:
                                            parts = request_contents[0].get('parts', [])
                                            if parts and len(parts) > 0:
                                                request_text = parts[0].get('text', '')
                                                # Look for country in prompt text
                                                if 'Country: IT' in request_text:
                                                    country = "IT"
                                                elif 'Country: FR' in request_text:
                                                    country = "FR"
                                                elif 'Country: PL' in request_text:
                                                    country = "PL"
                                                logger.info(f"Detected country from request: {country}")
                                    
                                    # Normalize city through dictionaries if AI found a city
                                    final_city = ai_city
                                    final_region = ai_region
                                    final_region_code = None
                                    
                                    if ai_city and ai_city != 'UNKNOWN':
                                        try:
                                            normalized_city, normalized_region, region_code = lookup_pl_location(ai_city, country)
                                            if normalized_city:
                                                final_city = normalized_city
                                                final_region = normalized_region
                                                final_region_code = region_code
                                                logger.info(f"Normalized city for {api_id}: {ai_city} -> {final_city}, region: {final_region}")
                                            else:
                                                logger.warning(f"Could not normalize city {ai_city} for {api_id}, using AI result")
                                        except Exception as e:
                                            logger.warning(f"Error normalizing city {ai_city} for {api_id}: {e}. Using AI result.")
                                            final_city = ai_city
                                            final_region = ai_region
                                            final_region_code = None
                                    
                                    # НАКОПЛЯЕМ SHOP ДАННЫЕ (используем правильные названия полей)
                                    # Получаем gamification_id и другие данные напрямую из all_data
                                    gamification_id_value = None
                                    nip_value = None
                                    shopnetwork_value = "UNKNOWN"
                                    address_value = None
                                    
                                    try:
                                        shop_info_query = f"""
                                        SELECT 
                                            gamification_id,
                                            shopnetwork,
                                            shop_name,
                                            JSON_VALUE(raw_doc, '$.nip') as nip,
                                            JSON_VALUE(raw_doc, '$.address') as address
                                        FROM `{PROJECT_ID}.{DATASET}.all_data`
                                        WHERE api_id = '{api_id}'
                                        LIMIT 1
                                        """
                                        shop_info_rows = list(bq_client.query(shop_info_query))
                                        
                                        if shop_info_rows:
                                            shop_info_row = shop_info_rows[0]
                                            gamification_id_value = shop_info_row.gamification_id
                                            nip_value = shop_info_row.nip
                                            shopnetwork_value = shop_info_row.shopnetwork or shop_info_row.shop_name
                                            address_value = shop_info_row.address
                                    except Exception as e:
                                        logger.warning(f"⚠️ Error querying shop_info for {api_id}: {e}. Using fallback values.")
                                        # Используем fallback значения - shop все равно сохранится
                                    
                                    # ИСПРАВЛЕНО: Обработка ошибок при создании shop_row
                                    try:
                                        # ИСПРАВЛЕНО: Безопасное преобразование типов
                                        shopnetwork_safe = str(shopnetwork_value) if shopnetwork_value else "UNKNOWN"
                                        address_safe = str(address_value)[:255] if address_value else None
                                        city_population_safe = parse_city_population(str(city_population)) if city_population else None
                                        
                                        shop_row = {
                                            "report_id": report_id,
                                            "report_name": report_name,
                                            "api_id": api_id,
                                            "nip": str(nip_value) if nip_value else None,
                                            "shop_chain": normalize_network_name_by_country(shopnetwork_safe, country),
                                            "city": str(final_city) if final_city else "UNKNOWN",
                                            "region": str(final_region) if final_region else "UNKNOWN",
                                            "region_code": str(final_region_code) if final_region_code else None,
                                            "shop_address": address_safe,
                                            "city_population": city_population_safe,
                                            "country": str(country) if country else "PL",
                                            "gamification_id": str(gamification_id_value) if gamification_id_value else None,
                                            "confidence": str(confidence) if confidence else "LOW",
                                            "evidence": str(evidence)[:500] if evidence else "No evidence",
                                            "created_at": _now().isoformat(),
                                        }
                                        accumulated_shops.append(shop_row)
                                        total_shops += 1
                                        logger.info(f"Shop saved for {api_id}: country={country}, city={final_city}, region={final_region}")
                                        total_processed += 1
                                    except Exception as e:
                                        logger.error(f"⚠️ Error creating shop_row for {api_id}: {e}")
                                        import traceback
                                        logger.error(f"⚠️ Traceback: {traceback.format_exc()}")
                                        # Пытаемся сохранить shop с минимальными данными
                                        try:
                                            minimal_shop_row = {
                                                "report_id": report_id,
                                                "report_name": report_name,
                                                "api_id": str(api_id),
                                                "nip": None,
                                                "shop_chain": "UNKNOWN",
                                                "city": "UNKNOWN",
                                                "region": "UNKNOWN",
                                                "region_code": None,
                                                "shop_address": None,
                                                "city_population": None,
                                                "country": str(country) if country else "PL",
                                                "gamification_id": str(gamification_id_value) if gamification_id_value else None,
                                                "confidence": "LOW",
                                                "evidence": f"Error creating shop: {str(e)[:200]}",
                                                "created_at": _now().isoformat(),
                                            }
                                            accumulated_shops.append(minimal_shop_row)
                                            total_shops += 1
                                            logger.warning(f"⚠️ Saved minimal shop_row for {api_id} due to error")
                                            total_processed += 1
                                        except Exception as e2:
                                            logger.error(f"❌ Failed to save even minimal shop_row for {api_id}: {e2}")
                                            logger.error(f"❌ Traceback: {traceback.format_exc()}")
                                            # Не увеличиваем total_processed, так как shop не сохранился
                                except Exception as e:
                                    logger.error(f"⚠️ Error processing shop data for {api_id}: {e}")
                                    import traceback
                                    logger.error(f"⚠️ Traceback: {traceback.format_exc()}")
                                    # Пытаемся сохранить shop с минимальными данными даже при ошибке обработки shop данных
                                    try:
                                        minimal_shop_row = {
                                            "report_id": report_id,
                                            "report_name": report_name,
                                            "api_id": str(api_id),
                                            "nip": None,
                                            "shop_chain": "UNKNOWN",
                                            "city": "UNKNOWN",
                                            "region": "UNKNOWN",
                                            "region_code": None,
                                            "shop_address": None,
                                            "city_population": None,
                                            "country": "PL",  # Fallback country
                                            "gamification_id": None,
                                            "confidence": "LOW",
                                            "evidence": f"Error processing shop data: {str(e)[:200]}",
                                            "created_at": _now().isoformat(),
                                        }
                                        accumulated_shops.append(minimal_shop_row)
                                        total_shops += 1
                                        logger.warning(f"⚠️ Saved minimal shop_row for {api_id} due to error processing shop data")
                                        total_processed += 1
                                    except Exception as e2:
                                        logger.error(f"❌ Failed to save even minimal shop_row for {api_id}: {e2}")
                                        logger.error(f"❌ Traceback: {traceback.format_exc()}")
                                        # Не увеличиваем total_processed, так как shop не сохранился
                        else:
                            logger.warning(f"Could not parse JSON for line {line_num}")
                            failed_parsing += 1
                    else:
                        if line_num <= 3:
                            logger.warning(f"No response text found for line {line_num}")
                        failed_processing += 1
                    
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue
            
            # ФИНАЛЬНАЯ БАТЧЕВАЯ ЗАГРУЗКА ВСЕХ НАКОПЛЕННЫХ ДАННЫХ
            logger.info("🚀 STARTING FINAL BATCH LOAD OF ACCUMULATED DATA (GCS)")
            logger.info("📊 Products: %s, Shops: %s, Vector Products: %s", 
                       len(accumulated_products), len(accumulated_shops), len(accumulated_vector_products))
            
            try:
                # Загружаем продукты большим батчем
                if accumulated_products:
                    logger.info("🔄 Loading %s products to corrected_products", len(accumulated_products))
                    _storage_write_api_load(PRODUCTS_TABLE, accumulated_products, report_id, report_name)
                
                # Загружаем векторные продукты большим батчем
                if accumulated_vector_products:
                    logger.info("🔄 Loading %s vector products to products_vector_ready", len(accumulated_vector_products))
                    _storage_write_api_load(VECTOR_READY_TABLE, accumulated_vector_products, report_id, report_name)
                
                # Загружаем магазины большим батчем
                if accumulated_shops:
                    logger.info("🔄 Loading %s shops to shop_directory", len(accumulated_shops))
                    _storage_write_api_load(SHOP_TABLE, accumulated_shops, report_id, report_name)
                
                logger.info("✅ ALL ACCUMULATED DATA LOADED SUCCESSFULLY (GCS)")
                
            except Exception as e:
                logger.error("❌ FAILED TO LOAD ACCUMULATED DATA (GCS): %s", e)
                raise
            
            processing_time = int(time.time() - start_time)
            
            # Детальная диагностика результатов
            logger.info(f"📊 PROCESSING SUMMARY:")
            logger.info(f"  Total lines in file: {total_lines}")
            logger.info(f"  Successfully processed: {processed_successfully}")
            logger.info(f"  Failed parsing: {failed_parsing}")
            logger.info(f"  Failed processing: {failed_processing}")
            logger.info(f"  Final receipts processed: {total_processed}")
            
            if failed_parsing > 0 or failed_processing > 0:
                logger.warning(f"⚠️ LOST {failed_parsing + failed_processing} receipts during processing!")
            
            return {
                "status": "success",
                "message": f"Processed {total_processed} receipts from GCS results",
                "report_id": report_id,
                "report_name": report_name,
                "processing_time_seconds": processing_time,
                "input_uri": input_uri,
                "receipts_processed": total_processed,
                "products_saved": len(accumulated_products),
                "vector_products_saved": len(accumulated_vector_products),
                "diagnostics": {
                    "total_lines": total_lines,
                    "processed_successfully": processed_successfully,
                    "failed_parsing": failed_parsing,
                    "failed_processing": failed_processing
                },
                "shops_saved": len(accumulated_shops)
            }
            
        except Exception as e:
            logger.error(f"Error processing batch results from GCS: {e}")
            return {
                "status": "error",
                "message": f"Failed to process batch results: {str(e)}",
                "report_id": report_id,
                "report_name": report_name
        }

# Initialize batch processor
batch_processor = BatchReceiptProcessor()

# -----------------------------------------------------------------------------
# Batch Status Checker
# -----------------------------------------------------------------------------
@functions_framework.http
def batch_status_checker(request: Request):
    """Проверка статуса асинхронных batch jobs"""
    try:
        # Handle CORS preflight
        if request.method == "OPTIONS":
            headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
            return ("", 204, headers)
        
        if request.method != "GET":
            return jsonify({"status": "error", "message": "Only GET method allowed"}), 405
        
        # Extract report_id from path
        path_parts = request.path.strip('/').split('/')
        if len(path_parts) < 2 or path_parts[-2] != 'batch_status':
            return jsonify({"status": "error", "message": "Invalid path format. Use /batch_status/{report_id}"}), 400
        
        report_id = path_parts[-1]
        if not report_id:
            return jsonify({"status": "error", "message": "Report ID is required"}), 400
        
        logger.info(f"Checking batch status for report_id: {report_id}")
        
        # Search for batch jobs with this report_id in the name
        # This is a simplified approach - in production you might want to store job metadata in a database
        try:
            # List recent batch prediction jobs
            jobs = aiplatform.BatchPredictionJob.list(
                filter=f'display_name~"receipt-batch-async-{report_id}"',
                order_by="create_time desc",
                limit=1
            )
            
            if not jobs:
                return jsonify({
                    "status": "not_found",
                    "message": f"No batch job found for report_id: {report_id}",
                    "report_id": report_id
                }), 404
            
            job = jobs[0]
            state = job.state
            
            # Determine status and progress
            status_info = {
                "report_id": report_id,
                "job_name": job.resource_name,
                "display_name": job.display_name,
                "create_time": job.create_time.isoformat() if job.create_time else None,
                "update_time": job.update_time.isoformat() if job.update_time else None,
            }
            
            if JobState:
                if state == JobState.JOB_STATE_QUEUED:
                    status_info.update({
                        "status": "queued",
                        "progress": "0%",
                        "message": "Job is queued for execution"
                    })
                elif state == JobState.JOB_STATE_RUNNING:
                    status_info.update({
                        "status": "running", 
                        "progress": "50%",
                        "message": "Job is currently running"
                    })
                elif state == JobState.JOB_STATE_SUCCEEDED:
                    status_info.update({
                        "status": "completed",
                        "progress": "100%",
                        "message": "Job completed successfully",
                        "output_location": job.output_info.gcs_output_directory if job.output_info else None
                    })
                elif state == JobState.JOB_STATE_FAILED:
                    status_info.update({
                        "status": "failed",
                        "progress": "0%",
                        "message": f"Job failed: {job.error.message if job.error else 'Unknown error'}",
                        "error": str(job.error) if job.error else None
                    })
                elif state == JobState.JOB_STATE_CANCELLED:
                    status_info.update({
                        "status": "cancelled",
                        "progress": "0%",
                        "message": "Job was cancelled"
                    })
                else:
                    status_info.update({
                        "status": "unknown",
                        "progress": "unknown",
                        "message": f"Unknown job state: {state}",
                        "raw_state": str(state)
                    })
            else:
                # Fallback to string comparison
                state_str = str(state)
                if "QUEUED" in state_str:
                    status_info.update({
                        "status": "queued",
                        "progress": "0%",
                        "message": "Job is queued for execution"
                    })
                elif "RUNNING" in state_str:
                    status_info.update({
                        "status": "running",
                        "progress": "50%", 
                        "message": "Job is currently running"
                    })
                elif "SUCCEEDED" in state_str:
                    status_info.update({
                        "status": "completed",
                        "progress": "100%",
                        "message": "Job completed successfully",
                        "output_location": job.output_info.gcs_output_directory if job.output_info else None
                    })
                elif "FAILED" in state_str:
                    status_info.update({
                        "status": "failed",
                        "progress": "0%",
                        "message": f"Job failed: {job.error.message if job.error else 'Unknown error'}",
                        "error": str(job.error) if job.error else None
                    })
                else:
                    status_info.update({
                        "status": "unknown",
                        "progress": "unknown",
                        "message": f"Unknown job state: {state_str}",
                        "raw_state": state_str
                    })
            
            headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
            return (jsonify(status_info), 200, headers)
            
        except Exception as e:
            logger.error(f"Error checking batch status for {report_id}: {e}")
            return jsonify({
                "status": "error",
                "message": f"Error checking job status: {str(e)}",
                "report_id": report_id
            }), 500
            
    except Exception as e:
        logger.exception("Error in batch_status_checker")
        return jsonify({"status": "error", "message": str(e)}), 500

# -----------------------------------------------------------------------------
# Active Promotions Processor
# -----------------------------------------------------------------------------
@functions_framework.http
def process_active_promotions(request: Request):
    """
    Обрабатывает все активные промо-акции параллельно
    """
    try:
        logger.info("🚀 Starting active promotions processing")
        
        # Получаем параметры из запроса
        data = request.get_json(silent=True) or {}
        limit_per_promo = data.get("limit_per_promo", None)  # Без лимита - обрабатываем все
        
        # Генерируем уникальные ID для отчета
        report_id = generate_report_id("active_promotions")
        report_name = f"Active Promotions Processing - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
        
        logger.info(f"Report ID: {report_id}")
        logger.info(f"Report Name: {report_name}")
        logger.info(f"Limit per promo: {limit_per_promo}")
        
        # Обрабатываем активные промо-акции
        result = process_active_promotions_parallel(
            report_id=report_id,
            report_name=report_name,
            limit_per_promo=limit_per_promo
        )
        
        # Добавляем метаданные
        result.update({
            "report_id": report_id,
            "report_name": report_name,
            "timestamp": datetime.utcnow().isoformat(),
            "service": "receipt-data-processor-active-promotions",
            "dataset": DATASET,
            "supported_countries": SUPPORTED_COUNTRIES
        })
        
        logger.info(f"✅ Active promotions processing completed: {result.get('status', 'unknown')}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in active promotions processing: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Active promotions processing failed: {str(e)}",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "receipt-data-processor-active-promotions"
        }), 500

# -----------------------------------------------------------------------------
# HTTP handler
# -----------------------------------------------------------------------------


@functions_framework.http
def receipt_data_processor(request: Request):
    """
    Cloud Function entry point с полным циклом обработки.
    
    Поддерживает режимы:
    * mode="single" - обработка одного чека (передаете данные в JSON)
    * mode="single_by_id" - обработка одного чека по ID (данные берутся из BigQuery)
    * mode="batch" - полная обработка с ожиданием результатов
    * mode="batch_ids" - обработка конкретных ID с ожиданием результатов
    * mode="load_to_fact_scan" - загрузка всех чеков в fact_scan
    * mode="load_to_all_data" - загрузка успешных чеков в all_data
    * mode="load_both" - загрузка и в fact_scan, и в all_data
    """
    try:
        # Handle CORS preflight
        if request.method == "OPTIONS":
            headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
            return ("", 204, headers)
        
        # Health endpoint
        if request.path == '/health' or request.args.get('health') == 'true':
            return jsonify({
                "status": "ok", 
                "service": "receipt-data-processor-complete",
                "timestamp": datetime.utcnow().isoformat(),
                "dataset": DATASET,
                "supported_countries": SUPPORTED_COUNTRIES
            })
            
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "single")
        
        # Auto-detect country from gamification_id if not provided
        country = data.get("country")
        gamification_id = data.get("gamification_id")
        
        if not country and gamification_id:
            country = get_country_from_gamification(gamification_id)
            if country:
                logger.info(f"🌍 Auto-detected country: {country} from gamification_id: {gamification_id}")
            else:
                country = "PL"  # Default fallback
                logger.warning(f"⚠️ Could not detect country for gamification_id: {gamification_id}, using default: PL")
        
        # Validate country if provided
        if country and country not in SUPPORTED_COUNTRIES:
            return jsonify({"status": "error", "message": f"Unsupported country: {country}"}), 400
            
        # Determine report identifiers
        report_name = data.get("report_name") or f"Receipt_{mode.title()}"
        report_id = data.get("report_id") or generate_report_id(report_name)
        
        logger.info(f"Processing request: mode={mode}, report_id={report_id}")
        
        result = {}  # Инициализируем result заранее
        
        if mode == "single":
            api_id = data.get("api_id")
            products_json = data.get("products")
            if not api_id or not products_json:
                return jsonify({"status": "error", "message": "api_id and products are required"}), 400
                
            total_price = data.get("total_price")
            if total_price is not None:
                try:
                    total_price = float(total_price)
                except (TypeError, ValueError):
                    total_price = None
                    
            result = process_single_receipt(
                api_id=api_id,
                products_json=products_json,
                total_price=total_price,
                country=country,
                report_id=report_id,
                report_name=report_name,
                shopnetwork=data.get("shopnetwork"),
                shop_name=data.get("shop_name"),
                raw_address=data.get("address"),
                nip=data.get("nip"),
            )
            
        elif mode == "single_by_id":
            api_id = data.get("api_id")
            if not api_id:
                return jsonify({"status": "error", "message": "api_id is required"}), 400
                
            result = process_single_receipt_by_id(
                api_id=api_id,
                country=country,
                report_id=report_id,
                report_name=report_name
            )
            
        elif mode == "batch":
            target_date = data.get('target_date')
            date_from = data.get('date_from')
            date_to = data.get('date_to')
            since_timestamp = data.get('since_timestamp')
            no_date_filter = data.get('no_date_filter', False)
            countries = data.get('countries')
            if not countries and data.get('country'):
                countries = [data.get('country')]
            if not countries:
                countries = get_countries_from_data(target_date, None)
                if countries:
                    logger.info(f"🌍 Auto-detected countries: {countries}")
                else:
                    logger.error(f"❌ No countries found in data for date: {target_date}")
                    return jsonify({
                        'status': 'error',
                        'message': f'No countries found in data for date: {target_date}. Please specify countries parameter.'
                    }), 400
            limit = data.get('limit', None)
            test_mode = data.get('test_mode', False)
            async_mode = data.get('async', False)

            if async_mode:
                logger.info(f"Starting async batch job creation: countries={countries}, date={target_date}, limit={limit}")
                try:
                    result = batch_processor.create_batch_job_async(
                        report_id=report_id,
                        report_name=report_name,
                        countries=countries,
                        target_date=target_date,
                        date_from=date_from,
                        date_to=date_to,
                        no_date_filter=no_date_filter,
                        limit=limit,
                        test_mode=test_mode
                    )
                    result["check_status_endpoint"] = f"/batch_status/{report_id}"
                except Exception as e:
                    return jsonify({"status": "error", "message": str(e)}), 503
            else:
                logger.info(f"Starting complete batch processing: countries={countries}, date={target_date}, limit={limit}")
                result = batch_processor.process_batch_receipts_complete(
                    report_id=report_id,
                    report_name=report_name,
                    countries=countries,
                    target_date=target_date,
                    date_from=date_from,
                    date_to=date_to,
                    since_timestamp=since_timestamp,
                    no_date_filter=no_date_filter,
                    limit=limit,
                    test_mode=test_mode
                )
            
        elif mode == "load_to_fact_scan":
            target_date = data.get('target_date')
            gamification_id = data.get('gamification_id')
            overwrite_mode = data.get('overwrite_mode', False)
            result = load_receipts_to_fact_scan(
                country=country,
                target_date=target_date,
                report_id=report_id,
                report_name=report_name,
                gamification_id=gamification_id,
                overwrite_mode=overwrite_mode
            )
            
        elif mode == "load_all_promos_to_fact_scan":
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            overwrite_mode = data.get('overwrite_mode', False)
            result = load_all_promos_to_fact_scan(
                start_date=start_date,
                end_date=end_date,
                report_id=report_id,
                report_name=report_name,
                overwrite_mode=overwrite_mode
            )
            
        elif mode == "load_to_all_data":
            target_date = data.get('target_date')
            since_timestamp = data.get('since_timestamp')
            result = load_successful_receipts_to_all_data(
                country=country,
                target_date=target_date,
                report_id=report_id,
                report_name=report_name,
                since_timestamp=since_timestamp
            )
            
        elif mode == "load_both":
            target_date = data.get('target_date')
            since_timestamp = data.get('since_timestamp')
            incremental = data.get('incremental', False)
            countries = data.get('countries', [])
            gamification_id = data.get('gamification_id')
            overwrite_mode = data.get('overwrite_mode', False)
            
            # Если incremental=True и нет since_timestamp, берем за последний час
            if incremental and not since_timestamp:
                since_timestamp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
                logger.info(f"🔄 Incremental mode: using since_timestamp={since_timestamp}")
            
            if not countries and country:
                countries = [country]

            if not countries:
                return jsonify({"status": "error", "message": "A list of 'countries' is required for mode 'load_both'"}), 400

            results_by_country = {}
            overall_status = "success"

            for c in countries:
                logger.info(f"Processing 'load_both' for country: {c}")
            fact_scan_result = load_receipts_to_fact_scan(
                    country=c,
                target_date=target_date,
                report_id=report_id,
                    report_name=report_name,
                    since_timestamp=since_timestamp,
                    gamification_id=gamification_id,
                    overwrite_mode=overwrite_mode
            )
            
            all_data_result = load_successful_receipts_to_all_data(
                    country=c,
                target_date=target_date,
                report_id=report_id,
                report_name=report_name,
                since_timestamp=since_timestamp
            )

            if fact_scan_result['status'] == 'error' or all_data_result['status'] == 'error':
                overall_status = "partial_error"

            results_by_country[c] = {
                "fact_scan_result": fact_scan_result,
                "all_data_result": all_data_result
            }
            
            # Формирование result вынесено за пределы цикла и находится внутри блока 'load_both'
            result = {
                "status": overall_status,
                "message": f"Finished loading data for countries: {', '.join(countries)}",
                "results_by_country": results_by_country,
                "report_id": report_id,
                "report_name": report_name
            }
            
        elif mode == "batch_ids":
            api_ids = data.get('api_ids', [])
            input_uri = data.get('input_uri')
            
            if not api_ids and not input_uri:
                return jsonify({"status": "error", "message": "Either api_ids list or input_uri is required for batch_ids mode"}), 400
            
            if api_ids and input_uri:
                return jsonify({"status": "error", "message": "Provide either api_ids or input_uri, not both"}), 400
            
            test_mode = data.get('test_mode', False)

            # Исправлена структура if/else
            if api_ids:
                if not isinstance(api_ids, list):
                    return jsonify({"status": "error", "message": "api_ids must be a list"}), 400
                    
                if len(api_ids) > 5000:
                    return jsonify({"status": "error", "message": "api_ids list too large (max 5000)"}), 400
                
                result = batch_processor.process_batch_by_ids(
                    report_id=report_id,
                    report_name=report_name,
                    api_ids=api_ids,
                    country=country,
                    test_mode=test_mode
                )
            else:
                result = batch_processor.process_batch_results_from_gcs(
                    report_id=report_id,
                    report_name=report_name,
                    input_uri=input_uri
                )
        else:
            return jsonify({"status": "error", "message": f"Unknown mode: {mode}. Supported: single, single_by_id, batch, batch_ids, load_to_fact_scan, load_to_all_data, load_both"}), 400
            
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
        return (jsonify(result), 200, headers)
        
    except Exception as e:
        logger.exception("Error handling request")
        return (jsonify({"status": "error", "message": str(e)}), 500, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        })

def main():
    from flask import Flask, request as flask_request
    app = Flask(__name__)

    @app.route("/", methods=["POST", "GET", "OPTIONS"])
    def local_endpoint():
        return receipt_data_processor(flask_request)

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({
            "status": "ok", 
            "service": "receipt-data-processor-local",
            "timestamp": datetime.utcnow().isoformat(),
            "dataset": DATASET,
            "supported_countries": SUPPORTED_COUNTRIES
        })

    app.run(host="0.0.0.0", port=8080, debug=True)

if __name__ == "__main__":
    main()