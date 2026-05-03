"""
clone_adventureworks.py
========================
Clone hanya tabel-tabel yang dibutuhkan dari AdventureWorks guru
ke database lokal. Supaya tidak bergantung pada server guru.

Schemas yang di-clone:
  - purchasing  (5 tabel)
  - production  (3 tabel)
  - person      (4 tabel)

Cara pakai:
  1. Buat dulu database lokal:
     Di DBeaver connect ke postgres → CREATE DATABASE adventureworks_local;
  2. python clone_adventureworks.py
  3. Update config.py OLTP_CONFIG ke localhost/adventureworks_local
"""

import psycopg2

# ============================================================
# CONFIG — Sesuaikan jika perlu
# ============================================================

# Sumber: DB guru (remote)
REMOTE_CONFIG = {
    "host": "10.183.26.99",
    "port": 5432,
    "dbname": "adventureworks",
    "user": "dlhstudent",
    "password": "passworddlh"
}

# Target: DB lokal
LOCAL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "adventureworks_local",
    "user": "postgres",
    "password": "postgres"
}

# ============================================================
# TABEL-TABEL YANG DIBUTUHKAN
# ============================================================
TABLES_TO_CLONE = {
    # schema: [(table_name, create_sql, select_sql, columns)]

    "purchasing": [
        (
            "purchaseorderheader",
            """CREATE TABLE IF NOT EXISTS purchasing.purchaseorderheader (
                purchaseorderid INT PRIMARY KEY,
                revisionnumber SMALLINT,
                status SMALLINT,
                employeeid INT,
                vendorid INT,
                shipmethodid INT,
                orderdate TIMESTAMP,
                shipdate TIMESTAMP,
                subtotal DECIMAL(19,4),
                taxamt DECIMAL(19,4),
                freight DECIMAL(19,4),
                modifieddate TIMESTAMP
            )""",
            """SELECT purchaseorderid, revisionnumber, status, employeeid,
                      vendorid, shipmethodid, orderdate, shipdate,
                      subtotal, taxamt, freight, modifieddate
               FROM purchasing.purchaseorderheader""",
            ["purchaseorderid", "revisionnumber", "status", "employeeid",
             "vendorid", "shipmethodid", "orderdate", "shipdate",
             "subtotal", "taxamt", "freight", "modifieddate"]
        ),
        (
            "purchaseorderdetail",
            """CREATE TABLE IF NOT EXISTS purchasing.purchaseorderdetail (
                purchaseorderid INT,
                purchaseorderdetailid INT,
                duedate TIMESTAMP,
                orderqty SMALLINT,
                productid INT,
                unitprice DECIMAL(19,4),
                receivedqty DECIMAL(8,2),
                rejectedqty DECIMAL(8,2),
                modifieddate TIMESTAMP,
                PRIMARY KEY (purchaseorderid, purchaseorderdetailid)
            )""",
            """SELECT purchaseorderid, purchaseorderdetailid, duedate,
                      orderqty, productid, unitprice,
                      receivedqty, rejectedqty, modifieddate
               FROM purchasing.purchaseorderdetail""",
            ["purchaseorderid", "purchaseorderdetailid", "duedate",
             "orderqty", "productid", "unitprice",
             "receivedqty", "rejectedqty", "modifieddate"]
        ),
        (
            "vendor",
            """CREATE TABLE IF NOT EXISTS purchasing.vendor (
                businessentityid INT PRIMARY KEY,
                accountnumber VARCHAR(15),
                name VARCHAR(50),
                creditrating SMALLINT,
                preferredvendorstatus BOOLEAN,
                activeflag BOOLEAN,
                purchasingwebserviceurl VARCHAR(1024),
                modifieddate TIMESTAMP
            )""",
            """SELECT businessentityid, accountnumber, name, creditrating,
                      preferredvendorstatus, activeflag,
                      purchasingwebserviceurl, modifieddate
               FROM purchasing.vendor""",
            ["businessentityid", "accountnumber", "name", "creditrating",
             "preferredvendorstatus", "activeflag",
             "purchasingwebserviceurl", "modifieddate"]
        ),
        (
            "shipmethod",
            """CREATE TABLE IF NOT EXISTS purchasing.shipmethod (
                shipmethodid INT PRIMARY KEY,
                name VARCHAR(50),
                shipbase DECIMAL(19,4),
                shiprate DECIMAL(19,4),
                modifieddate TIMESTAMP
            )""",
            """SELECT shipmethodid, name, shipbase, shiprate, modifieddate
               FROM purchasing.shipmethod""",
            ["shipmethodid", "name", "shipbase", "shiprate", "modifieddate"]
        ),
        (
            "productvendor",
            """CREATE TABLE IF NOT EXISTS purchasing.productvendor (
                productid INT,
                businessentityid INT,
                averageleadtime INT,
                standardprice DECIMAL(19,4),
                lastreceiptcost DECIMAL(19,4),
                lastreceiptdate TIMESTAMP,
                minorderqty INT,
                maxorderqty INT,
                onorderqty INT,
                unitmeasurecode VARCHAR(3),
                modifieddate TIMESTAMP,
                PRIMARY KEY (productid, businessentityid)
            )""",
            """SELECT productid, businessentityid, averageleadtime,
                      standardprice, lastreceiptcost, lastreceiptdate,
                      minorderqty, maxorderqty, onorderqty,
                      unitmeasurecode, modifieddate
               FROM purchasing.productvendor""",
            ["productid", "businessentityid", "averageleadtime",
             "standardprice", "lastreceiptcost", "lastreceiptdate",
             "minorderqty", "maxorderqty", "onorderqty",
             "unitmeasurecode", "modifieddate"]
        ),
    ],

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
            )""",
            """SELECT productid, name, productnumber, color, standardcost, listprice, modifieddate
               FROM production.product""",
            ["productid", "name", "productnumber", "color", "standardcost", "listprice", "modifieddate"]
        ),
        (
            "location",
            """CREATE TABLE IF NOT EXISTS production.location (
                locationid SMALLINT PRIMARY KEY,
                name VARCHAR(50),
                costrate DECIMAL(10,4),
                availability DECIMAL(8,2),
                modifieddate TIMESTAMP
            )""",
            """SELECT locationid, name, costrate, availability, modifieddate
               FROM production.location""",
            ["locationid", "name", "costrate", "availability", "modifieddate"]
        ),
        (
            "scrapreason",
            """CREATE TABLE IF NOT EXISTS production.scrapreason (
                scrapreasonid SMALLINT PRIMARY KEY,
                name VARCHAR(50),
                modifieddate TIMESTAMP
            )""",
            """SELECT scrapreasonid, name, modifieddate
               FROM production.scrapreason""",
            ["scrapreasonid", "name", "modifieddate"]
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
            )""",
            # PERBAIKAN DI SINI: Kita hitung manual (orderqty - scrappedqty) sebagai stockedqty
            """SELECT workorderid, productid, orderqty, 
                      (orderqty - COALESCE(scrappedqty, 0)) AS stockedqty, 
                      scrappedqty, startdate, enddate, duedate, scrapreasonid, modifieddate
               FROM production.workorder""",
            ["workorderid", "productid", "orderqty", "stockedqty", "scrappedqty",
             "startdate", "enddate", "duedate", "scrapreasonid", "modifieddate"]
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
            )""",
            """SELECT workorderid, productid, operationsequence, locationid,
                      scheduledstartdate, scheduledenddate, actualstartdate, actualenddate,
                      actualresourcehrs, plannedcost, actualcost, modifieddate
               FROM production.workorderrouting""",
            ["workorderid", "productid", "operationsequence", "locationid",
             "scheduledstartdate", "scheduledenddate", "actualstartdate", "actualenddate",
             "actualresourcehrs", "plannedcost", "actualcost", "modifieddate"]
        )
    ],

    "person": [
        (
            "businessentityaddress",
            """CREATE TABLE IF NOT EXISTS person.businessentityaddress (
                businessentityid INT,
                addressid INT,
                addresstypeid INT,
                modifieddate TIMESTAMP,
                PRIMARY KEY (businessentityid, addressid, addresstypeid)
            )""",
            """SELECT businessentityid, addressid, addresstypeid, modifieddate
               FROM person.businessentityaddress""",
            ["businessentityid", "addressid", "addresstypeid", "modifieddate"]
        ),
        (
            "address",
            """CREATE TABLE IF NOT EXISTS person.address (
                addressid INT PRIMARY KEY,
                addressline1 VARCHAR(60),
                addressline2 VARCHAR(60),
                city VARCHAR(30),
                stateprovinceid INT,
                postalcode VARCHAR(15),
                modifieddate TIMESTAMP
            )""",
            """SELECT addressid, addressline1, addressline2, city,
                      stateprovinceid, postalcode, modifieddate
               FROM person.address""",
            ["addressid", "addressline1", "addressline2", "city",
             "stateprovinceid", "postalcode", "modifieddate"]
        ),
        (
            "stateprovince",
            """CREATE TABLE IF NOT EXISTS person.stateprovince (
                stateprovinceid INT PRIMARY KEY,
                stateprovincecode VARCHAR(3),
                countryregioncode VARCHAR(3),
                isonlystateprovinceflag BOOLEAN,
                name VARCHAR(50),
                territoryid INT,
                modifieddate TIMESTAMP
            )""",
            """SELECT stateprovinceid, stateprovincecode, countryregioncode,
                      isonlystateprovinceflag, name, territoryid, modifieddate
               FROM person.stateprovince""",
            ["stateprovinceid", "stateprovincecode", "countryregioncode",
             "isonlystateprovinceflag", "name", "territoryid", "modifieddate"]
        ),
        (
            "countryregion",
            """CREATE TABLE IF NOT EXISTS person.countryregion (
                countryregioncode VARCHAR(3) PRIMARY KEY,
                name VARCHAR(50),
                modifieddate TIMESTAMP
            )""",
            """SELECT countryregioncode, name, modifieddate
               FROM person.countryregion""",
            ["countryregioncode", "name", "modifieddate"]
        ),
    ],
}


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("CLONE ADVENTUREWORKS (required schemas only)")
    print("=" * 60)
    print(f"Source: {REMOTE_CONFIG['host']}:{REMOTE_CONFIG['port']}/{REMOTE_CONFIG['dbname']}")
    print(f"Target: {LOCAL_CONFIG['host']}:{LOCAL_CONFIG['port']}/{LOCAL_CONFIG['dbname']}")
    print()

    remote_conn = psycopg2.connect(**REMOTE_CONFIG)
    local_conn = psycopg2.connect(**LOCAL_CONFIG)
    remote_cur = remote_conn.cursor()
    local_cur = local_conn.cursor()

    try:
        total_rows = 0

        for schema, tables in TABLES_TO_CLONE.items():
            print(f"\n--- Schema: {schema} ---")

            # Create schema if not exists
            local_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            for table_name, create_sql, select_sql, columns in tables:
                full_name = f"{schema}.{table_name}"

                # Drop & recreate table
                local_cur.execute(f"DROP TABLE IF EXISTS {full_name} CASCADE;")
                local_cur.execute(create_sql)

                # Extract from remote
                remote_cur.execute(select_sql)
                rows = remote_cur.fetchall()

                if rows:
                    placeholders = ", ".join(["%s"] * len(columns))
                    col_names = ", ".join(columns)
                    local_cur.executemany(
                        f"INSERT INTO {full_name} ({col_names}) VALUES ({placeholders})",
                        rows
                    )

                total_rows += len(rows)
                print(f"  {full_name}: {len(rows)} rows cloned")

        local_conn.commit()

        print(f"\n{'=' * 60}")
        print(f"CLONE COMPLETE! Total: {total_rows} rows")
        print(f"{'=' * 60}")
        print(f"\nSekarang update config.py OLTP_CONFIG menjadi:")
        print(f'  "host": "127.0.0.1",')
        print(f'  "dbname": "adventureworks_local",')

    except Exception as e:
        local_conn.rollback()
        print(f"\n[ERROR] {e}")
        raise
    finally:
        remote_cur.close()
        local_cur.close()
        remote_conn.close()
        local_conn.close()


if __name__ == "__main__":
    main()