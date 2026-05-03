"""
incremental_load.py - Incremental Load untuk Data Warehouse (Production)
========================================================================
Skrip ini dijalankan SECARA BERKALA (misal: harian) setelah One-Time Historical Load.

Strategi:
  - DimDate        : Dilewati (sudah di-generate penuh hingga 2030)
  - DimScrapReason : SCD Type 1 (Bandingkan & Timpa/Overwrite)
  - DimLocation    : SCD Type 2 (Bandingkan atribut -> Expire lama + Insert baru)
  - DimProduct     : SCD Type 2 (Bandingkan atribut -> Expire lama + Insert baru)
  - DimRouting     : SCD Type 2 (Bandingkan ID -> Expire lama + Insert baru)
  - Fact           : Delta dengan Watermark (Hapus data lama untuk PO yang ter-update, lalu Insert)
"""

import psycopg2
import time
from datetime import date, datetime

# ============================================================
# KONFIGURASI KONEKSI
# ============================================================
OLTP_CONFIG = {
    "host": "localhost", "port": 5432, "dbname": "adventureworks_local",
    "user": "postgres", "password": "postgres"
}

STAGING_CONFIG = {
    "host": "localhost", "port": 5432, "dbname": "production_staging",
    "user": "postgres", "password": "postgres"
}

DW_CONFIG = {
    "host": "localhost", "port": 5432, "dbname": "production_dw",
    "user": "postgres", "password": "postgres"
}

# ============================================================
# HELPER: Ambil Watermark (Timestamp terakhir)
# ============================================================
def get_last_load_timestamp(dw_cur):
    """Ambil tanggal StartDate maksimum dari tabel fakta sebagai watermark[cite: 5]."""
    dw_cur.execute("""
        SELECT MAX(dd.FullDate)
        FROM FactWorkOrderRouting f
        JOIN DimDate dd ON f.StartDateKey = dd.DateKey
    """)
    result = dw_cur.fetchone()
    # Jika tabel kosong, gunakan tanggal lampau yang aman
    if result and result[0]:
        return datetime.combine(result[0], datetime.min.time())
    return datetime(1900, 1, 1)

# ============================================================
# FASE 1: INCREMENTAL EXTRACT (OLTP -> Staging)
# ============================================================
def extract_to_staging(oltp_cur, stg_cur, query, target_table, columns, params=None):
    """Fungsi pembantu untuk menarik data dan memasukkannya ke Staging[cite: 5]."""
    if params:
        oltp_cur.execute(query, params)
    else:
        oltp_cur.execute(query)
    rows = oltp_cur.fetchall()
    if rows:
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(columns)
        stg_cur.executemany(f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})", rows)
    print(f"  [EXTRACT] {target_table}: {len(rows)} baris.")
    return rows

def run_extract_incremental(watermark):
    print("\n" + "=" * 60)
    print(f"FASE 1: INCREMENTAL EXTRACT (Delta sejak {watermark})")
    print("=" * 60)

    oltp_conn = psycopg2.connect(**OLTP_CONFIG)
    stg_conn = psycopg2.connect(**STAGING_CONFIG)
    oltp_cur = oltp_conn.cursor()
    stg_cur = stg_conn.cursor()

    try:
        # Kosongkan tabel staging sebelum extract[cite: 5]
        tables_to_truncate = [
            "production.workorderrouting", "production.workorder",
            "production.product", "production.location", "production.scrapreason"
        ]
        for t in tables_to_truncate:
            stg_cur.execute(f"TRUNCATE TABLE {t} CASCADE;")

        # 1. Tarik Fakta yang berstatus DELTA (baru/berubah berdasarkan watermark)[cite: 5]
        delta_routing = extract_to_staging(
            oltp_cur, stg_cur,
            """SELECT workorderid, productid, operationsequence, locationid,
                      scheduledstartdate, scheduledenddate, actualstartdate, actualenddate,
                      actualresourcehrs, plannedcost, actualcost, modifieddate
               FROM production.workorderrouting
               WHERE modifieddate > %s""",
            "production.workorderrouting",
            ["workorderid", "productid", "operationsequence", "locationid",
             "scheduledstartdate", "scheduledenddate", "actualstartdate", "actualenddate",
             "actualresourcehrs", "plannedcost", "actualcost", "modifieddate"],
            params=(watermark,)
        )

        # 2. Tarik WorkOrder yang terhubung dengan routing yang berubah[cite: 5]
        if delta_routing:
            wo_ids = list(set([r[0] for r in delta_routing]))
            placeholders = ", ".join(["%s"] * len(wo_ids))
            extract_to_staging(
                oltp_cur, stg_cur,
                f"""SELECT workorderid, productid, orderqty, stockedqty, scrappedqty,
                          startdate, enddate, duedate, scrapreasonid, modifieddate
                   FROM production.workorder
                   WHERE workorderid IN ({placeholders})""",
                "production.workorder",
                ["workorderid", "productid", "orderqty", "stockedqty", "scrappedqty",
                 "startdate", "enddate", "duedate", "scrapreasonid", "modifieddate"],
                 params=wo_ids
            )
        else:
             print("  [EXTRACT] production.workorder: 0 baris (Tidak ada routing baru)")

        # 3. Tarik Dimensi secara FULL (ukurannya relatif kecil, perbandingan dilakukan di fase Transform)[cite: 5]
        extract_to_staging(oltp_cur, stg_cur, "SELECT productid, name, productnumber, color, standardcost, listprice, modifieddate FROM production.product", "production.product", ["productid", "name", "productnumber", "color", "standardcost", "listprice", "modifieddate"])
        extract_to_staging(oltp_cur, stg_cur, "SELECT locationid, name, costrate, availability, modifieddate FROM production.location", "production.location", ["locationid", "name", "costrate", "availability", "modifieddate"])
        extract_to_staging(oltp_cur, stg_cur, "SELECT scrapreasonid, name, modifieddate FROM production.scrapreason", "production.scrapreason", ["scrapreasonid", "name", "modifieddate"])

        stg_conn.commit()
        return len(delta_routing)

    except Exception as e:
        stg_conn.rollback()
        raise e
    finally:
        oltp_cur.close(); stg_cur.close(); oltp_conn.close(); stg_conn.close()

# ============================================================
# FASE 2: INCREMENTAL TRANSFORM DIMENSI
# ============================================================
def transform_dim_scrapreason(stg_cur, dw_cur):
    print("\n  [DIM_SCRAPREASON] Incremental (SCD Type 1)...")
    stg_cur.execute("SELECT scrapreasonid, name FROM production.scrapreason")
    ins, upd = 0, 0
    for sid, sname in stg_cur.fetchall():
        dw_cur.execute("SELECT ScrapReasonKey, ReasonName FROM DimScrapReason WHERE ScrapReasonID = %s", (sid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO DimScrapReason (ScrapReasonID, ReasonName) VALUES (%s, %s)", (sid, sname))
            ins += 1
        elif ex[1] != sname:
            dw_cur.execute("UPDATE DimScrapReason SET ReasonName = %s WHERE ScrapReasonID = %s", (sname, sid))
            upd += 1
    print(f"  [DIM_SCRAPREASON] Baru: {ins}, Diperbarui: {upd}")

def transform_dim_location(stg_cur, dw_cur, today_str):
    print("\n  [DIM_LOCATION] Incremental (SCD Type 2)...")
    stg_cur.execute("SELECT locationid, name, costrate, availability FROM production.location")
    ins, scd2 = 0, 0
    for lid, lname, costrate, avail in stg_cur.fetchall():
        dw_cur.execute("SELECT LocationKey, CostRate, Availability FROM DimLocation WHERE LocationID = %s AND IsCurrent = TRUE", (lid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO DimLocation (LocationID, LocationName, CostRate, Availability, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, '9999-12-31', TRUE)", (lid, lname, costrate, avail, today_str))
            ins += 1
        else:
            loc_key, old_cost, old_avail = ex
            # Memeriksa jika ada perubahan pada CostRate atau Availability[cite: 5]
            if float(old_cost) != float(costrate) or float(old_avail) != float(avail):
                dw_cur.execute("UPDATE DimLocation SET ValidTo = %s, IsCurrent = FALSE WHERE LocationKey = %s", (today_str, loc_key))
                dw_cur.execute("INSERT INTO DimLocation (LocationID, LocationName, CostRate, Availability, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, '9999-12-31', TRUE)", (lid, lname, costrate, avail, today_str))
                scd2 += 1
    print(f"  [DIM_LOCATION] Baru: {ins}, SCD2 Terpicu: {scd2}")

def transform_dim_product(stg_cur, dw_cur, today_str):
    print("\n  [DIM_PRODUCT] Incremental (SCD Type 2)...")
    stg_cur.execute("SELECT productid, name, productnumber, color, standardcost FROM production.product")
    ins, scd2 = 0, 0
    for pid, pname, pnum, color, stdcost in stg_cur.fetchall():
        dw_cur.execute("SELECT ProductKey, StandardCost FROM DimProduct WHERE ProductID = %s AND IsCurrent = TRUE", (pid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO DimProduct (ProductID, ProductName, ProductNumber, Color, StandardCost, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)", (pid, pname, pnum, color, stdcost, today_str))
            ins += 1
        else:
            prod_key, old_cost = ex
            if float(old_cost) != float(stdcost):
                dw_cur.execute("UPDATE DimProduct SET ValidTo = %s, IsCurrent = FALSE WHERE ProductKey = %s", (today_str, prod_key))
                dw_cur.execute("INSERT INTO DimProduct (ProductID, ProductName, ProductNumber, Color, StandardCost, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)", (pid, pname, pnum, color, stdcost, today_str))
                scd2 += 1
            else:
                # Cost sama, tapi atribut lain mungkin berubah — overwrite
                dw_cur.execute(
                    "UPDATE DimProduct SET ProductName=%s, ProductNumber=%s, Color=%s WHERE ProductKey=%s",
                    (pname, pnum, color, prod_key))
    print(f"  [DIM_PRODUCT] Baru: {ins}, SCD2 Terpicu: {scd2}")

# Tambahkan fungsi ini dan panggil di run_transform_dimensions()
def transform_dim_routing(stg_cur, dw_cur, today_str):
    stg_cur.execute("SELECT DISTINCT productid, operationsequence FROM production.workorderrouting")
    ins = 0
    for (p_id, op_seq) in stg_cur.fetchall():
        dw_cur.execute(
            "SELECT RoutingKey FROM DimRouting WHERE ProductID = %s AND OperationSequence = %s AND IsCurrent = TRUE",
            (p_id, op_seq))
        if dw_cur.fetchone() is None:
            dw_cur.execute(
                "INSERT INTO DimRouting (ProductID, OperationSequence, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, '9999-12-31', TRUE)",
                (p_id, op_seq, today_str))
            ins += 1
    print(f"  [DIM_ROUTING] Baru: {ins}")    

def run_transform_dimensions():
    print("\n" + "=" * 60)
    print("FASE 2: INCREMENTAL TRANSFORM DIMENSI")
    print("=" * 60)
    
    stg_conn = psycopg2.connect(**STAGING_CONFIG)
    dw_conn = psycopg2.connect(**DW_CONFIG)
    stg_cur = stg_conn.cursor()
    dw_cur = dw_conn.cursor()
    today_str = date.today().strftime("%Y-%m-%d")

    try:
        transform_dim_scrapreason(stg_cur, dw_cur)
        transform_dim_location(stg_cur, dw_cur, today_str)
        transform_dim_product(stg_cur, dw_cur, today_str)
        transform_dim_routing(stg_cur, dw_cur, today_str)
        dw_conn.commit()
    except Exception as e:
        dw_conn.rollback()
        raise e
    finally:
         stg_cur.close(); dw_cur.close(); stg_conn.close(); dw_conn.close()

# ============================================================
# FASE 3: INCREMENTAL FACT LOAD
# ============================================================
def run_transform_facts():
    print("\n" + "=" * 60)
    print("FASE 3: INCREMENTAL FACT LOAD (UPSERT)")
    print("=" * 60)

    stg_conn = psycopg2.connect(**STAGING_CONFIG)
    dw_conn = psycopg2.connect(**DW_CONFIG)
    stg_cur = stg_conn.cursor()
    dw_cur = dw_conn.cursor()

    try:
        # Tarik data delta dari Staging dengan JOIN
        query = """
            SELECT wr.workorderid, wr.productid, wr.locationid, wr.operationsequence, 
                   wo.scrapreasonid, wr.actualstartdate, wr.actualenddate, wo.duedate, 
                   wo.orderqty, wo.scrappedqty, wr.actualresourcehrs
            FROM production.workorderrouting wr
            JOIN production.workorder wo ON wr.workorderid = wo.workorderid
        """
        stg_cur.execute(query)
        staging_rows = stg_cur.fetchall()

        if not staging_rows:
            print("  [FACT] Tidak ada data delta untuk dimuat.")
            return

        # 1. HAPUS FAKTA LAMA berdasarkan WorkOrderID yang mengalami pembaruan (Pola Upsert)[cite: 5]
        wo_ids = list(set([r[0] for r in staging_rows]))
        if wo_ids:
            placeholders = ", ".join(["%s"] * len(wo_ids))
            dw_cur.execute(f"DELETE FROM FactWorkOrderRouting WHERE WorkOrderID IN ({placeholders})", wo_ids)
            deleted = dw_cur.rowcount
            if deleted:
                print(f"  [FACT] Menghapus {deleted} baris lama untuk WorkOrder yang diperbarui.")

        # 2. PERSIAPKAN LOOKUP DI MEMORI
        dw_cur.execute("SELECT ProductID, ProductKey FROM DimProduct WHERE IsCurrent = TRUE")
        map_product = {row[0]: row[1] for row in dw_cur.fetchall()}
        
        dw_cur.execute("SELECT LocationID, LocationKey FROM DimLocation WHERE IsCurrent = TRUE")
        map_location = {row[0]: row[1] for row in dw_cur.fetchall()}
        
        dw_cur.execute("SELECT ProductID, OperationSequence, RoutingKey FROM DimRouting WHERE IsCurrent = TRUE")
        map_routing = {(row[0], row[1]): row[2] for row in dw_cur.fetchall()}

        dw_cur.execute("SELECT ScrapReasonID, ScrapReasonKey FROM DimScrapReason")
        map_scrap = {row[0]: row[1] for row in dw_cur.fetchall()}

        # 3. TRANSFORM & INSERT FAKTA BARU
        fact_data = []
        for row in staging_rows:
            wo_id, p_id, loc_id, op_seq, scrap_id, start_dt, end_dt, due_dt, ord_qty, scrap_qty, res_hrs = row
            
            p_key = map_product.get(p_id)
            loc_key = map_location.get(loc_id)
            rout_key = map_routing.get((p_id, op_seq))
            s_key = map_scrap.get(scrap_id, 0) if scrap_id is not None else 0
            
            start_key = int(start_dt.strftime("%Y%m%d")) if start_dt else None
            end_key = int(end_dt.strftime("%Y%m%d")) if end_dt else None
            due_key = int(due_dt.strftime("%Y%m%d")) if due_dt else None
            
            if p_key and loc_key and rout_key and start_key:
                fact_data.append((p_key, loc_key, rout_key, s_key, start_key, end_key, due_key, wo_id, ord_qty, scrap_qty, res_hrs))

        if fact_data:
            dw_cur.executemany("""
                INSERT INTO FactWorkOrderRouting 
                (ProductKey, LocationKey, RoutingKey, ScrapReasonKey, StartDateKey, EndDateKey, DueDateKey, WorkOrderID, OrderQty, ScrappedQty, ActualResourceHrs) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, fact_data)

        dw_conn.commit()
        print(f"  [FACT] {len(fact_data)} baris berhasil disisipkan (Incremental Load).")

    except Exception as e:
        dw_conn.rollback()
        raise e
    finally:
        stg_cur.close(); dw_cur.close(); stg_conn.close(); dw_conn.close()

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    start_time = time.time()
    print("=" * 60)
    print("MEMULAI PROSES INCREMENTAL LOAD")
    print("=" * 60)

    # Dapatkan watermark
    dw_conn = psycopg2.connect(**DW_CONFIG)
    dw_cur = dw_conn.cursor()
    watermark = get_last_load_timestamp(dw_cur)
    dw_cur.close()
    dw_conn.close()
    print(f"Timestamp Terakhir di DW (Watermark): {watermark}")

    # Eksekusi Pipeline
    new_records_count = run_extract_incremental(watermark)
    run_transform_dimensions()
    
    if new_records_count > 0:
        run_transform_facts()
    else:
        print("\n  [FACT] Tidak ada aktivitas produksi baru. Proses muat tabel fakta dilewati.")

    print("\n" + "=" * 60)
    print(f"INCREMENTAL LOAD SELESAI! (Waktu eksekusi: {time.time() - start_time:.2f} detik)")
    print("=" * 60)

if __name__ == "__main__":
    main()