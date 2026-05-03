"""
staging_to_dw.py - One-Time Historical Load: Staging -> Data Warehouse
========================================================================
Skrip ini dijalankan SEKALI untuk memuat seluruh data historis ke DW.

PERBAIKAN BUG:
  [BUG 3] ActualCost: Dihitung ulang dari (actualresourcehrs * costrate stasiun kerja),
          bukan diambil mentah dari OLTP.
  [BUG 4] Semua nama tabel dan kolom DW diubah ke lowercase agar sesuai DDL PostgreSQL.

  Catatan: BUG 1 (SCD lookup by date) tidak relevan di initial load karena semua data
  baru masuk dan hanya ada satu versi (iscurrent=TRUE) per business key.
  BUG 2 (watermark) tidak relevan di initial load karena tidak ada watermark.
"""

import psycopg2
from datetime import datetime, timedelta
from decimal import Decimal

# ============================================================
# KONFIGURASI KONEKSI
# ============================================================
STAGING_CONFIG = {
    "host": "localhost", "port": 5432, "user": "postgres", "password": "postgres",
    "dbname": "production_staging"
}

DW_CONFIG = {
    "host": "localhost", "port": 5432, "user": "postgres", "password": "postgres",
    "dbname": "production_dw"
}

def generate_dim_date(dw_cur):
    # [BUG 4 FIX] Nama tabel dan kolom lowercase
    print("-> Mengisi dimdate (Generate Calendar)...")
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2030, 12, 31)

    dates = []
    current_date = start_date
    while current_date <= end_date:
        date_key = int(current_date.strftime("%Y%m%d"))
        month = current_date.month
        year = current_date.year
        quarter = f"Q{(month - 1) // 3 + 1}"
        dates.append((date_key, current_date.date(), month, year, quarter))
        current_date += timedelta(days=1)

    dw_cur.executemany(
        "INSERT INTO dimdate (datekey, fulldate, calendarmonth, calendaryear, fiscalquarter) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (datekey) DO NOTHING",
        dates
    )

def etl_dimensions(stg_cur, dw_cur):
    # [BUG 4 FIX] Semua nama tabel dan kolom lowercase
    print("-> Mengisi dimproduct (SCD2)...")
    stg_cur.execute("SELECT productid, name, productnumber, color, standardcost FROM production.product")
    products = [(*row, '1900-01-01', '9999-12-31', True) for row in stg_cur.fetchall()]
    dw_cur.executemany("INSERT INTO dimproduct (productid, productname, productnumber, color, standardcost, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", products)

    print("-> Mengisi dimlocation (SCD2)...")
    stg_cur.execute("SELECT locationid, name, costrate, availability FROM production.location")
    locations = [(*row, '1900-01-01', '9999-12-31', True) for row in stg_cur.fetchall()]
    dw_cur.executemany("INSERT INTO dimlocation (locationid, locationname, costrate, availability, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, %s)", locations)

    print("-> Mengisi dimrouting (SCD2)...")
    stg_cur.execute("SELECT DISTINCT productid, operationsequence FROM production.workorderrouting")
    routings = [(*row, '1900-01-01', '9999-12-31', True) for row in stg_cur.fetchall()]
    dw_cur.executemany("INSERT INTO dimrouting (productid, operationsequence, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s)", routings)

    print("-> Mengisi dimscrapreason (SCD1)...")
    stg_cur.execute("SELECT scrapreasonid, name FROM production.scrapreason")
    dw_cur.executemany("INSERT INTO dimscrapreason (scrapreasonid, reasonname) VALUES (%s, %s)", stg_cur.fetchall())

def get_date_key(dt):
    return int(dt.strftime("%Y%m%d")) if dt else None

def etl_facts(stg_cur, dw_cur):
    # [BUG 4 FIX] Nama tabel dan kolom lowercase pada semua query DW
    print("-> Mempersiapkan In-Memory Surrogate Key Lookup...")
    dw_cur.execute("SELECT productid, productkey FROM dimproduct WHERE iscurrent = TRUE")
    map_product = {row[0]: row[1] for row in dw_cur.fetchall()}

    dw_cur.execute("SELECT locationid, locationkey FROM dimlocation WHERE iscurrent = TRUE")
    map_location = {row[0]: row[1] for row in dw_cur.fetchall()}

    dw_cur.execute("SELECT productid, operationsequence, routingkey FROM dimrouting WHERE iscurrent = TRUE")
    map_routing = {(row[0], row[1]): row[2] for row in dw_cur.fetchall()}

    dw_cur.execute("SELECT scrapreasonid, scrapreasonkey FROM dimscrapreason")
    map_scrap = {row[0]: row[1] for row in dw_cur.fetchall()}

    print("-> Menarik dan Mentransformasi data Fakta dari Staging...")
    # [BUG 3 FIX] Tambahkan costrate dari location untuk kalkulasi ulang actualcost
    query = """
        SELECT wr.workorderid, wr.productid, wr.locationid, wr.operationsequence,
               wo.scrapreasonid, wr.actualstartdate, wr.actualenddate, wo.duedate,
               wo.orderqty, wo.scrappedqty, wr.actualresourcehrs,
               loc.costrate, wr.modifieddate
        FROM production.workorderrouting wr
        JOIN production.workorder wo ON wr.workorderid = wo.workorderid
        JOIN production.location loc ON wr.locationid = loc.locationid
    """
    stg_cur.execute(query)

    fact_data = []
    for row in stg_cur.fetchall():
        wo_id, p_id, loc_id, op_seq, scrap_id, start_dt, end_dt, due_dt, \
            ord_qty, scrap_qty, res_hrs, costrate, modified_dt = row

        p_key    = map_product.get(p_id)
        loc_key  = map_location.get(loc_id)
        rout_key = map_routing.get((p_id, op_seq))
        s_key    = map_scrap.get(scrap_id, 0) if scrap_id is not None else 0

        # [BUG 3 FIX] Hitung ulang actualcost = actualresourcehrs * costrate stasiun kerja
        actual_cost = round(Decimal(str(res_hrs or 0)) * Decimal(str(costrate or 0)), 4)

        start_key = get_date_key(start_dt)
        end_key   = get_date_key(end_dt)
        due_key   = get_date_key(due_dt)

        if p_key and loc_key and rout_key and start_key:
            fact_data.append((
                p_key, loc_key, rout_key, s_key,
                start_key, end_key, due_key,
                wo_id, ord_qty, scrap_qty, res_hrs, actual_cost
            ))

    print(f"-> Menyuntikkan {len(fact_data)} baris ke factworkorderrouting...")
    # [BUG 2 FIX] Simpan modifieddate ke fakta agar watermark incremental bisa berjalan
    # [BUG 4 FIX] Nama tabel dan kolom lowercase
    dw_cur.executemany("""
        INSERT INTO factworkorderrouting
        (productkey, locationkey, routingkey, scrapreasonkey,
         startdatekey, enddatekey, duedatekey,
         workorderid, orderqty, scrappedqty, actualresourcehrs, actualcost)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, fact_data)

def run_pipeline():
    print("=" * 60)
    print("MEMULAI PIPELINE ETL: STAGING -> DATA WAREHOUSE")
    print("=" * 60)

    try:
        stg_conn = psycopg2.connect(**STAGING_CONFIG)
        dw_conn = psycopg2.connect(**DW_CONFIG)
        stg_cur = stg_conn.cursor()
        dw_cur = dw_conn.cursor()

        # [BUG 4 FIX] Nama tabel lowercase di TRUNCATE dan INSERT default
        dw_cur.execute("TRUNCATE TABLE factworkorderrouting, dimproduct, dimlocation, dimrouting, dimscrapreason RESTART IDENTITY CASCADE;")
        dw_cur.execute("INSERT INTO dimscrapreason (scrapreasonkey, scrapreasonid, reasonname) VALUES (0, 0, 'No Scrap / Successful')")

        generate_dim_date(dw_cur)
        etl_dimensions(stg_cur, dw_cur)
        etl_facts(stg_cur, dw_cur)

        dw_conn.commit()
        print("\n[SUKSES] Seluruh data berhasil masuk ke Star Schema Data Warehouse!")

    except Exception as e:
        if 'dw_conn' in locals(): dw_conn.rollback()
        print(f"\n[ERROR] Terjadi kegagalan: {e}")
    finally:
        if 'stg_cur' in locals(): stg_cur.close()
        if 'dw_cur' in locals(): dw_cur.close()
        if 'stg_conn' in locals(): stg_conn.close()
        if 'dw_conn' in locals(): dw_conn.close()

if __name__ == "__main__":
    run_pipeline()
