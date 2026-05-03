import psycopg2
from datetime import datetime, timedelta

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
    print("-> Mengisi DimDate (Generate Calendar)...")
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
        "INSERT INTO DimDate (DateKey, FullDate, CalendarMonth, CalendarYear, FiscalQuarter) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (DateKey) DO NOTHING",
        dates
    )

def etl_dimensions(stg_cur, dw_cur):
    print("-> Mengisi DimProduct (SCD2)...")
    stg_cur.execute("SELECT productid, name, productnumber, color, standardcost FROM production.product")
    products = []
    for row in stg_cur.fetchall():
        # Menambahkan ValidFrom (1900-01-01), ValidTo (9999-12-31), IsCurrent (True) untuk Initial Load
        products.append((*row, '1900-01-01', '9999-12-31', True))
    dw_cur.executemany("INSERT INTO DimProduct (ProductID, ProductName, ProductNumber, Color, StandardCost, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", products)

    print("-> Mengisi DimLocation (SCD2)...")
    stg_cur.execute("SELECT locationid, name, costrate, availability FROM production.location")
    locations = [(*row, '1900-01-01', '9999-12-31', True) for row in stg_cur.fetchall()]
    dw_cur.executemany("INSERT INTO DimLocation (LocationID, LocationName, CostRate, Availability, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, %s)", locations)

    print("-> Mengisi DimRouting (SCD2)...")
    stg_cur.execute("SELECT DISTINCT productid, operationsequence FROM production.workorderrouting")
    routings = [(*row, '1900-01-01', '9999-12-31', True) for row in stg_cur.fetchall()]
    dw_cur.executemany("INSERT INTO DimRouting (ProductID, OperationSequence, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s)", routings)

    print("-> Mengisi DimScrapReason (SCD1)...")
    stg_cur.execute("SELECT scrapreasonid, name FROM production.scrapreason")
    dw_cur.executemany("INSERT INTO DimScrapReason (ScrapReasonID, ReasonName) VALUES (%s, %s)", stg_cur.fetchall())

def get_date_key(dt):
    return int(dt.strftime("%Y%m%d")) if dt else None

def etl_facts(stg_cur, dw_cur):
    print("-> Mempersiapkan In-Memory Surrogate Key Lookup...")
    # Tarik ID Asli dan Surrogate Key dari DW ke memori Python (Dictionary)
    dw_cur.execute("SELECT ProductID, ProductKey FROM DimProduct WHERE IsCurrent = True")
    map_product = {row[0]: row[1] for row in dw_cur.fetchall()}
    
    dw_cur.execute("SELECT LocationID, LocationKey FROM DimLocation WHERE IsCurrent = True")
    map_location = {row[0]: row[1] for row in dw_cur.fetchall()}
    
    dw_cur.execute("SELECT ProductID, OperationSequence, RoutingKey FROM DimRouting WHERE IsCurrent = True")
    map_routing = {(row[0], row[1]): row[2] for row in dw_cur.fetchall()}

    dw_cur.execute("SELECT ScrapReasonID, ScrapReasonKey FROM DimScrapReason")
    map_scrap = {row[0]: row[1] for row in dw_cur.fetchall()}

    print("-> Menarik dan Mentransformasi data Fakta dari Staging...")
    # Join WorkOrderRouting dengan WorkOrder untuk menyatukan metrik
    query = """
        SELECT wr.workorderid, wr.productid, wr.locationid, wr.operationsequence, 
               wo.scrapreasonid, wr.actualstartdate, wr.actualenddate, wo.duedate, 
               wo.orderqty, wo.scrappedqty, wr.actualresourcehrs
        FROM production.workorderrouting wr
        JOIN production.workorder wo ON wr.workorderid = wo.workorderid
    """
    stg_cur.execute(query)
    
    fact_data = []
    for row in stg_cur.fetchall():
        # Parsing data mentah
        wo_id, p_id, loc_id, op_seq, scrap_id, start_dt, end_dt, due_dt, ord_qty, scrap_qty, res_hrs = row
        
        # LOOKUP Surrogate Key (Transformasi Inti)
        p_key = map_product.get(p_id)
        loc_key = map_location.get(loc_id)
        rout_key = map_routing.get((p_id, op_seq))
        
        # Jika barang sukses (tidak ada alasan cacat), masukkan ke ScrapKey = 0
        s_key = map_scrap.get(scrap_id, 0) if scrap_id is not None else 0
        
        # Transform Date ke format Integer YYYYMMDD
        start_key = get_date_key(start_dt)
        end_key = get_date_key(end_dt)
        due_key = get_date_key(due_dt)
        
        # Validasi: Hanya masukkan fakta jika dimensi utamanya ditemukan
        if p_key and loc_key and rout_key and start_key:
            fact_data.append((p_key, loc_key, rout_key, s_key, start_key, end_key, due_key, wo_id, ord_qty, scrap_qty, res_hrs))

    print(f"-> Menyuntikkan {len(fact_data)} baris ke FactWorkOrderRouting...")
    dw_cur.executemany("""
        INSERT INTO FactWorkOrderRouting 
        (ProductKey, LocationKey, RoutingKey, ScrapReasonKey, StartDateKey, EndDateKey, DueDateKey, WorkOrderID, OrderQty, ScrappedQty, ActualResourceHrs) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        # Bersihkan tabel DW sebelum Initial Load (Opsional untuk testing)
        dw_cur.execute("TRUNCATE TABLE FactWorkOrderRouting, DimProduct, DimLocation, DimRouting, DimScrapReason RESTART IDENTITY CASCADE;")
        # Memasukkan kembali nilai default Scrap yang terhapus saat Truncate
        dw_cur.execute("INSERT INTO DimScrapReason (ScrapReasonKey, ScrapReasonID, ReasonName) VALUES (0, 0, 'No Scrap / Successful')")

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