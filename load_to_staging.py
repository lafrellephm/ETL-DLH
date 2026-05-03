import psycopg2
import io

# ============================================================
# KONFIGURASI KONEKSI
# ============================================================
# Sumber: Database OLTP Lokal kamu
SOURCE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "adventureworks_local", # Sesuaikan jika nama DB-mu production_oltp
    "user": "postgres",
    "password": "postgres"            # Sesuaikan dengan password lokalmu
}

# Target: Database Staging
TARGET_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "production_staging",
    "user": "postgres",
    "password": "postgres"
}

# Tabel yang akan dipindahkan beserta DDL (Data Definition Language)
TABLES = {
    "production": [
        (
            "product",
            """CREATE TABLE IF NOT EXISTS production.product (
                productid INT PRIMARY KEY,
                name VARCHAR(50),
                productnumber VARCHAR(25),
                color VARCHAR(15),
                standardcost DECIMAL(19,4),
                listprice DECIMAL(19,4),
                modifieddate TIMESTAMP
            )"""
        ),
        (
            "location",
            """CREATE TABLE IF NOT EXISTS production.location (
                locationid SMALLINT PRIMARY KEY,
                name VARCHAR(50),
                costrate DECIMAL(10,4),
                availability DECIMAL(8,2),
                modifieddate TIMESTAMP
            )"""
        ),
        (
            "scrapreason",
            """CREATE TABLE IF NOT EXISTS production.scrapreason (
                scrapreasonid SMALLINT PRIMARY KEY,
                name VARCHAR(50),
                modifieddate TIMESTAMP
            )"""
        ),
        (
            "workorder",
            """CREATE TABLE IF NOT EXISTS production.workorder (
                workorderid INT PRIMARY KEY,
                productid INT,
                orderqty INT,
                stockedqty INT,
                scrappedqty SMALLINT,
                startdate TIMESTAMP,
                enddate TIMESTAMP,
                duedate TIMESTAMP,
                scrapreasonid SMALLINT,
                modifieddate TIMESTAMP
            )"""
        ),
        (
            "workorderrouting",
            """CREATE TABLE IF NOT EXISTS production.workorderrouting (
                workorderid INT,
                productid INT,
                operationsequence SMALLINT,
                locationid SMALLINT,
                scheduledstartdate TIMESTAMP,
                scheduledenddate TIMESTAMP,
                actualstartdate TIMESTAMP,
                actualenddate TIMESTAMP,
                actualresourcehrs DECIMAL(9,4),
                plannedcost DECIMAL(19,4),
                actualcost DECIMAL(19,4),
                modifieddate TIMESTAMP,
                PRIMARY KEY (workorderid, productid, operationsequence)
            )"""
        )
    ]
}

def etl_to_staging():
    print("=" * 60)
    print("MEMULAI PROSES TRANSFER KE STAGING AREA (EXTRACT & LOAD)")
    print("=" * 60)

    try:
        # Buka koneksi ke kedua database
        source_conn = psycopg2.connect(**SOURCE_CONFIG)
        target_conn = psycopg2.connect(**TARGET_CONFIG)
        
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()

        for schema, tables in TABLES.items():
            print(f"\nMenyiapkan Schema: {schema}")
            target_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            for table_name, create_sql in tables:
                full_table_name = f"{schema}.{table_name}"
                print(f"-> Memproses tabel {full_table_name}...")

                # 1. Reset tabel di Staging (Truncate & Drop agar selalu fresh)
                target_cur.execute(f"DROP TABLE IF EXISTS {full_table_name} CASCADE;")
                target_cur.execute(create_sql)

                # 2. Extract dari OLTP (Menyedot ke memory dalam bentuk CSV stream)
                # Menggunakan trik StringIO agar tidak perlu membuat file fisik di hardisk
                memory_buffer = io.StringIO()
                copy_out_query = f"COPY {full_table_name} TO STDOUT WITH CSV HEADER"
                source_cur.copy_expert(copy_out_query, memory_buffer)
                
                # Mengembalikan kursor buffer ke baris pertama
                memory_buffer.seek(0)

                # 3. Load ke Staging (Menyuntikkan dari memory ke database)
                copy_in_query = f"COPY {full_table_name} FROM STDIN WITH CSV HEADER"
                target_cur.copy_expert(copy_in_query, memory_buffer)
                
                # Hitung jumlah baris untuk log (dikurangi 1 untuk header CSV)
                row_count = len(memory_buffer.getvalue().splitlines()) - 1
                print(f"   [SUKSES] {row_count} baris berhasil dipindahkan via Stream COPY.")

        # Commit (Simpan permanen) semua perubahan di Staging
        target_conn.commit()
        print("\n" + "=" * 60)
        print("TRANSFER KE STAGING SELESAI DENGAN SEMPURNA!")
        print("=" * 60)

    except Exception as e:
        if target_conn:
            target_conn.rollback() # Batalkan jika ada error
        print(f"\n[GAGAL] Terjadi kesalahan sistem: {e}")
    finally:
        # Tutup semua koneksi demi keamanan
        if 'source_cur' in locals(): source_cur.close()
        if 'target_cur' in locals(): target_cur.close()
        if 'source_conn' in locals(): source_conn.close()
        if 'target_conn' in locals(): target_conn.close()

if __name__ == "__main__":
    etl_to_staging()