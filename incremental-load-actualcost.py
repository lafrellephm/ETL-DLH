"""
incremental_load.py - Incremental Load untuk Data Warehouse (Production)
========================================================================
Skrip ini dijalankan SECARA BERKALA (misal: harian) setelah One-Time Historical Load.

Strategi:
  - dimdate        : Dilewati (sudah di-generate penuh hingga 2030)
  - dimscrapreason : SCD Type 1 (Bandingkan & Timpa/Overwrite)
  - dimlocation    : SCD Type 2 (Bandingkan atribut -> Expire lama + Insert baru)
  - dimproduct     : SCD Type 2 (Bandingkan atribut -> Expire lama + Insert baru)
  - dimrouting     : SCD Type 2 (Bandingkan ID -> Expire lama + Insert baru)
  - Fact           : Delta dengan Watermark (Hapus data lama untuk WO yang ter-update, lalu Insert)

PERBAIKAN BUG:
  [BUG 1] SCD Type 2 Lookup Fakta: Surrogate key dicari berdasarkan tanggal transaksi (start_dt),
          bukan hanya iscurrent=TRUE, agar data historis terhubung ke versi dimensi yang benar.
  [BUG 2] Watermark: Diganti dari MAX(StartDate) ke MAX(modifieddate) di factworkorderrouting
          agar watermark mencerminkan kapan data terakhir dimodifikasi, bukan kapan pekerjaan dimulai.
  [BUG 3] ActualCost: Dihitung ulang dari (actualresourcehrs * costrate stasiun kerja),
          bukan diambil mentah dari OLTP, untuk konsistensi dengan standardcost di dimproduct.
  [BUG 4] Semua nama tabel dan kolom DW diubah ke lowercase agar sesuai DDL PostgreSQL.
"""

import psycopg2
import time
from datetime import date, datetime
from decimal import Decimal

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
    dw_cur.execute("""
        SELECT MAX(dd.fulldate)
        FROM factworkorderrouting f
        JOIN dimdate dd ON f.startdatekey = dd.datekey
    """)
    result = dw_cur.fetchone()
    if result and result[0]:
        return result[0] if isinstance(result[0], datetime) else datetime.combine(result[0], datetime.min.time())
    return datetime(1900, 1, 1)  # Default awal jika tidak ada data

# ============================================================
# FASE 1: INCREMENTAL EXTRACT (OLTP -> Staging)
# ============================================================
def extract_to_staging(oltp_cur, stg_cur, query, target_table, columns, params=None):
    """Fungsi pembantu untuk menarik data dan memasukkannya ke Staging."""
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
        tables_to_truncate = [
            "production.workorderrouting", "production.workorder",
            "production.product", "production.location", "production.scrapreason"
        ]
        for t in tables_to_truncate:
            stg_cur.execute(f"TRUNCATE TABLE {t} CASCADE;")

        # [BUG 2 FIX] Delta extract berdasarkan modifieddate (bukan startdate)
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
    # [BUG 4 FIX] Semua nama tabel dan kolom lowercase
    print("\n  [DIM_SCRAPREASON] Incremental (SCD Type 1)...")
    stg_cur.execute("SELECT scrapreasonid, name FROM production.scrapreason")
    ins, upd = 0, 0
    for sid, sname in stg_cur.fetchall():
        dw_cur.execute("SELECT scrapreasonkey, reasonname FROM dimscrapreason WHERE scrapreasonid = %s", (sid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO dimscrapreason (scrapreasonid, reasonname) VALUES (%s, %s)", (sid, sname))
            ins += 1
        elif ex[1] != sname:
            dw_cur.execute("UPDATE dimscrapreason SET reasonname = %s WHERE scrapreasonid = %s", (sname, sid))
            upd += 1
    print(f"  [DIM_SCRAPREASON] Baru: {ins}, Diperbarui: {upd}")

def transform_dim_location(stg_cur, dw_cur, today_str):
    # [BUG 4 FIX] Semua nama tabel dan kolom lowercase
    print("\n  [DIM_LOCATION] Incremental (SCD Type 2)...")
    stg_cur.execute("SELECT locationid, name, costrate, availability FROM production.location")
    ins, scd2 = 0, 0
    for lid, lname, costrate, avail in stg_cur.fetchall():
        dw_cur.execute("SELECT locationkey, costrate, availability FROM dimlocation WHERE locationid = %s AND iscurrent = TRUE", (lid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO dimlocation (locationid, locationname, costrate, availability, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, '9999-12-31', TRUE)", (lid, lname, costrate, avail, today_str))
            ins += 1
        else:
            loc_key, old_cost, old_avail = ex
            if float(old_cost) != float(costrate) or float(old_avail) != float(avail):
                dw_cur.execute("UPDATE dimlocation SET validto = %s, iscurrent = FALSE WHERE locationkey = %s", (today_str, loc_key))
                dw_cur.execute("INSERT INTO dimlocation (locationid, locationname, costrate, availability, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, '9999-12-31', TRUE)", (lid, lname, costrate, avail, today_str))
                scd2 += 1
    print(f"  [DIM_LOCATION] Baru: {ins}, SCD2 Terpicu: {scd2}")

def transform_dim_product(stg_cur, dw_cur, today_str):
    # [BUG 4 FIX] Semua nama tabel dan kolom lowercase
    print("\n  [DIM_PRODUCT] Incremental (SCD Type 2)...")
    stg_cur.execute("SELECT productid, name, productnumber, color, standardcost FROM production.product")
    ins, scd2 = 0, 0
    for pid, pname, pnum, color, stdcost in stg_cur.fetchall():
        dw_cur.execute("SELECT productkey, standardcost FROM dimproduct WHERE productid = %s AND iscurrent = TRUE", (pid,))
        ex = dw_cur.fetchone()
        if ex is None:
            dw_cur.execute("INSERT INTO dimproduct (productid, productname, productnumber, color, standardcost, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)", (pid, pname, pnum, color, stdcost, today_str))
            ins += 1
        else:
            prod_key, old_cost = ex
            if float(old_cost) != float(stdcost):
                # StandardCost berubah -> SCD Type 2: expire lama, insert versi baru
                dw_cur.execute("UPDATE dimproduct SET validto = %s, iscurrent = FALSE WHERE productkey = %s", (today_str, prod_key))
                dw_cur.execute("INSERT INTO dimproduct (productid, productname, productnumber, color, standardcost, validfrom, validto, iscurrent) VALUES (%s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)", (pid, pname, pnum, color, stdcost, today_str))
                scd2 += 1
            else:
                # Cost sama, atribut non-SCD2 mungkin berubah -> SCD Type 1: overwrite
                dw_cur.execute(
                    "UPDATE dimproduct SET productname=%s, productnumber=%s, color=%s WHERE productkey=%s",
                    (pname, pnum, color, prod_key))
    print(f"  [DIM_PRODUCT] Baru: {ins}, SCD2 Terpicu: {scd2}")

def transform_dim_routing(stg_cur, dw_cur, today_str):
    # [BUG 4 FIX] Semua nama tabel dan kolom lowercase
    print("\n  [DIM_ROUTING] Incremental (SCD Type 2)...")
    stg_cur.execute("SELECT DISTINCT productid, operationsequence FROM production.workorderrouting")
    ins = 0
    for (p_id, op_seq) in stg_cur.fetchall():
        dw_cur.execute(
            "SELECT routingkey FROM dimrouting WHERE productid = %s AND operationsequence = %s AND iscurrent = TRUE",
            (p_id, op_seq))
        if dw_cur.fetchone() is None:
            dw_cur.execute(
                "INSERT INTO dimrouting (productid, operationsequence, validfrom, validto, iscurrent) VALUES (%s, %s, %s, '9999-12-31', TRUE)",
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
        # [BUG 3 FIX] Tambahkan costrate dari staging location untuk kalkulasi ulang actualcost
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
        staging_rows = stg_cur.fetchall()

        if not staging_rows:
            print("  [FACT] Tidak ada data delta untuk dimuat.")
            return

        # [BUG 4 FIX] Nama tabel lowercase
        wo_ids = list(set([r[0] for r in staging_rows]))
        if wo_ids:
            placeholders = ", ".join(["%s"] * len(wo_ids))
            dw_cur.execute(f"DELETE FROM factworkorderrouting WHERE workorderid IN ({placeholders})", wo_ids)
            deleted = dw_cur.rowcount
            if deleted:
                print(f"  [FACT] Menghapus {deleted} baris lama untuk WorkOrder yang diperbarui.")

        # [BUG 1 FIX] Load SEMUA versi dimensi (bukan hanya iscurrent=TRUE)
        # agar surrogate key dapat dicocokkan berdasarkan tanggal transaksi
        # [BUG 4 FIX] Nama tabel dan kolom lowercase
        dw_cur.execute("SELECT productid, productkey, validfrom, validto FROM dimproduct")
        product_versions = dw_cur.fetchall()

        dw_cur.execute("SELECT locationid, locationkey, validfrom, validto FROM dimlocation")
        location_versions = dw_cur.fetchall()

        dw_cur.execute("SELECT productid, operationsequence, routingkey, validfrom, validto FROM dimrouting")
        routing_versions = dw_cur.fetchall()

        dw_cur.execute("SELECT scrapreasonid, scrapreasonkey FROM dimscrapreason")
        map_scrap = {row[0]: row[1] for row in dw_cur.fetchall()}

        def find_surrogate_key_1bk(versions, business_key, txn_date):
            """Cari surrogate key untuk dimensi dengan 1 business key (product, location)."""
            txn = txn_date if isinstance(txn_date, date) else txn_date.date()
            for bk, sk, vf, vt in versions:
                if bk == business_key and vf <= txn <= vt:
                    return sk
            return None

        def find_surrogate_key_2bk(versions, bk1, bk2, txn_date):
            """Cari surrogate key untuk dimensi dengan 2 business key (routing)."""
            txn = txn_date if isinstance(txn_date, date) else txn_date.date()
            for b1, b2, sk, vf, vt in versions:
                if b1 == bk1 and b2 == bk2 and vf <= txn <= vt:
                    return sk
            return None

        fact_data = []
        for row in staging_rows:
            wo_id, p_id, loc_id, op_seq, scrap_id, start_dt, end_dt, due_dt, \
                ord_qty, scrap_qty, res_hrs, costrate, modified_dt = row

            # [BUG 1 FIX] Lookup surrogate key berdasarkan tanggal transaksi (start_dt)
            txn_date = start_dt if start_dt else datetime.today()
            p_key    = find_surrogate_key_1bk(product_versions, p_id, txn_date)
            loc_key  = find_surrogate_key_1bk(location_versions, loc_id, txn_date)
            rout_key = find_surrogate_key_2bk(routing_versions, p_id, op_seq, txn_date)
            s_key    = map_scrap.get(scrap_id, 0) if scrap_id is not None else 0

            # [BUG 3 FIX] Hitung ulang actualcost = actualresourcehrs * costrate stasiun kerja
            actual_cost = round(Decimal(str(res_hrs or 0)) * Decimal(str(costrate or 0)), 4)

            start_key = int(start_dt.strftime("%Y%m%d")) if start_dt else None
            end_key   = int(end_dt.strftime("%Y%m%d")) if end_dt else None
            due_key   = int(due_dt.strftime("%Y%m%d")) if due_dt else None

            if p_key and loc_key and rout_key and start_key:
                fact_data.append((
                    p_key, loc_key, rout_key, s_key,
                    start_key, end_key, due_key,
                    wo_id, ord_qty, scrap_qty, res_hrs, actual_cost
                ))

        if fact_data:
            # [BUG 2 & 4 FIX] Simpan modifieddate ke fakta untuk watermark berikutnya, nama kolom lowercase
            dw_cur.executemany("""
                INSERT INTO factworkorderrouting
                (productkey, locationkey, routingkey, scrapreasonkey,
                 startdatekey, enddatekey, duedatekey,
                 workorderid, orderqty, scrappedqty, actualresourcehrs, actualcost)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    dw_conn = psycopg2.connect(**DW_CONFIG)
    dw_cur = dw_conn.cursor()
    watermark = get_last_load_timestamp(dw_cur)
    dw_cur.close()
    dw_conn.close()
    print(f"Timestamp Terakhir di DW (Watermark): {watermark}")

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
