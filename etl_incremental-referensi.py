"""
etl_incremental.py - Incremental Load (Daily/Periodic)
=======================================================
Dijalankan BERKALA setelah historical load untuk memuat data baru/berubah.

Strategi:
  - Dim_Date       : Skip (sudah generated cukup luas)
  - Dim_ShipMethod : SCD Type 1 (compare + overwrite)
  - Dim_Vendor     : SCD Type 2 (compare tracked → expire+insert)
  - Dim_Product    : SCD Type 2 (compare tracked → expire+insert)
  - Fact           : Delta (watermark-based, delete+re-insert)

Cara pakai:
  python etl_incremental.py
"""

import psycopg2
import time
from datetime import date, datetime
from config import OLTP_CONFIG, STAGING_CONFIG, OLAP_CONFIG


def get_oltp_conn():
    return psycopg2.connect(**OLTP_CONFIG)

def get_staging_conn():
    return psycopg2.connect(**STAGING_CONFIG)

def get_olap_conn():
    return psycopg2.connect(**OLAP_CONFIG)


# ============================================================
# TRANSFORM HELPER
# ============================================================
def null_to_tidak_ada(value):
    """Replace None/NULL with 'Tidak ada' for text fields."""
    return "Tidak ada" if value is None else value


# ============================================================
# HELPER: Get last load timestamp
# ============================================================
def get_last_load_date(olap_cur):
    """Ambil tanggal load terakhir dari fact table sebagai watermark."""
    olap_cur.execute("""
        SELECT MAX(dd.full_date)
        FROM fact_goodsreceiving f
        JOIN dim_date dd ON f.date_key_order = dd.date_key
    """)
    result = olap_cur.fetchone()
    if result and result[0]:
        return result[0]
    return date(1900, 1, 1)


# ============================================================
# PHASE 1: INCREMENTAL EXTRACT (OLTP → Staging)
# ============================================================
def extract_table(oltp_cur, stg_cur, source_query, target_table, columns, params=None):
    """Generic extract helper."""
    if params:
        oltp_cur.execute(source_query, params)
    else:
        oltp_cur.execute(source_query)
    rows = oltp_cur.fetchall()
    if rows:
        ph = ", ".join(["%s"] * len(columns))
        cn = ", ".join(columns)
        stg_cur.executemany(f"INSERT INTO {target_table} ({cn}) VALUES ({ph})", rows)
    print(f"  [EXTRACT] {target_table}: {len(rows)} rows")
    return rows


def run_extract_incremental(last_load_date):
    """Extract delta POs + full dimension sources."""
    print("\n" + "=" * 60)
    print(f"PHASE 1: INCREMENTAL EXTRACT (delta since {last_load_date})")
    print("=" * 60)

    oltp_conn = get_oltp_conn()
    stg_conn = get_staging_conn()
    oltp_cur = oltp_conn.cursor()
    stg_cur = stg_conn.cursor()

    try:
        # Truncate staging
        staging_tables = [
            "stg_purchaseorderheader", "stg_purchaseorderdetail",
            "stg_vendor", "stg_shipmethod", "stg_productvendor",
            "stg_product", "stg_productsubcategory", "stg_productcategory",
            "stg_businessentityaddress", "stg_address",
            "stg_stateprovince", "stg_countryregion",
        ]
        for t in staging_tables:
            stg_cur.execute(f"TRUNCATE TABLE {t} CASCADE;")

        # -- DELTA: PO Header (hanya baru/berubah) --
        po_headers = extract_table(oltp_cur, stg_cur,
            """SELECT purchaseorderid, status, vendorid, shipmethodid,
                      orderdate, shipdate, subtotal, taxamt, freight
               FROM purchasing.purchaseorderheader
               WHERE modifieddate > %s""",
            "stg_purchaseorderheader",
            ["purchaseorderid", "status", "vendorid", "shipmethodid",
             "orderdate", "shipdate", "subtotal", "taxamt", "freight"],
            params=(last_load_date,))

        # -- DELTA: PO Detail (dari PO yg baru) --
        if po_headers:
            po_ids = [r[0] for r in po_headers]
            placeholders = ", ".join(["%s"] * len(po_ids))
            extract_table(oltp_cur, stg_cur,
                f"""SELECT purchaseorderid, purchaseorderdetailid,
                          orderqty, productid, unitprice,
                          (orderqty * unitprice) AS linetotal,
                          receivedqty, rejectedqty,
                          (receivedqty - rejectedqty) AS stockedqty
                   FROM purchasing.purchaseorderdetail
                   WHERE purchaseorderid IN ({placeholders})""",
                "stg_purchaseorderdetail",
                ["purchaseorderid", "purchaseorderdetailid",
                 "orderqty", "productid", "unitprice", "linetotal",
                 "receivedqty", "rejectedqty", "stockedqty"],
                params=po_ids)
        else:
            print(f"  [EXTRACT] stg_purchaseorderdetail: 0 rows (no new POs)")

        # -- FULL: Dimension sources (kecil, perlu compare SCD) --
        extract_table(oltp_cur, stg_cur,
            """SELECT businessentityid, name, creditrating,
                      preferredvendorstatus, activeflag
               FROM purchasing.vendor""",
            "stg_vendor",
            ["businessentityid", "name", "creditrating",
             "preferredvendorstatus", "activeflag"])

        extract_table(oltp_cur, stg_cur,
            "SELECT shipmethodid, name FROM purchasing.shipmethod",
            "stg_shipmethod",
            ["shipmethodid", "name"])

        extract_table(oltp_cur, stg_cur,
            """SELECT productid, businessentityid, standardprice, unitmeasurecode
               FROM purchasing.productvendor""",
            "stg_productvendor",
            ["productid", "businessentityid", "standardprice", "unitmeasurecode"])

        extract_table(oltp_cur, stg_cur,
            """SELECT productid, name, productnumber, color, productsubcategoryid
               FROM production.product""",
            "stg_product",
            ["productid", "name", "productnumber", "color", "productsubcategoryid"])

        extract_table(oltp_cur, stg_cur,
            """SELECT productsubcategoryid, productcategoryid, name
               FROM production.productsubcategory""",
            "stg_productsubcategory",
            ["productsubcategoryid", "productcategoryid", "name"])

        extract_table(oltp_cur, stg_cur,
            "SELECT productcategoryid, name FROM production.productcategory",
            "stg_productcategory",
            ["productcategoryid", "name"])

        extract_table(oltp_cur, stg_cur,
            """SELECT businessentityid, addressid, addresstypeid
               FROM person.businessentityaddress""",
            "stg_businessentityaddress",
            ["businessentityid", "addressid", "addresstypeid"])

        extract_table(oltp_cur, stg_cur,
            "SELECT addressid, city, stateprovinceid FROM person.address",
            "stg_address",
            ["addressid", "city", "stateprovinceid"])

        extract_table(oltp_cur, stg_cur,
            """SELECT stateprovinceid, name, countryregioncode
               FROM person.stateprovince""",
            "stg_stateprovince",
            ["stateprovinceid", "name", "countryregioncode"])

        extract_table(oltp_cur, stg_cur,
            "SELECT countryregioncode, name FROM person.countryregion",
            "stg_countryregion",
            ["countryregioncode", "name"])

        stg_conn.commit()
        print("  [EXTRACT] ✓ Incremental extract complete")
        return len(po_headers)

    except Exception as e:
        stg_conn.rollback()
        raise e
    finally:
        oltp_cur.close()
        stg_cur.close()
        oltp_conn.close()
        stg_conn.close()


# ============================================================
# PHASE 2: INCREMENTAL TRANSFORM DIMENSIONS
# ============================================================

def incr_dim_shipmethod(stg_cur, olap_cur):
    """SCD Type 1: Compare & overwrite."""
    print("\n  [DIM_SHIPMETHOD] Incremental (SCD Type 1)...")
    stg_cur.execute("SELECT shipmethodid, name FROM stg_shipmethod")
    ins, upd = 0, 0
    for sid, sname in stg_cur.fetchall():
        sname = null_to_tidak_ada(sname)
        olap_cur.execute(
            "SELECT ship_method_key, ship_method_name FROM dim_shipmethod WHERE ship_method_id = %s",
            (sid,))
        ex = olap_cur.fetchone()
        if ex is None:
            olap_cur.execute(
                "INSERT INTO dim_shipmethod (ship_method_id, ship_method_name) VALUES (%s, %s)",
                (sid, sname))
            ins += 1
        elif ex[1] != sname:
            olap_cur.execute(
                "UPDATE dim_shipmethod SET ship_method_name = %s WHERE ship_method_id = %s",
                (sname, sid))
            upd += 1
    print(f"  [DIM_SHIPMETHOD] New: {ins}, Updated: {upd}")


def incr_dim_vendor(stg_cur, olap_cur):
    """SCD Type 2: Compare tracked attrs → expire old + insert new if changed."""
    print("\n  [DIM_VENDOR] Incremental (SCD Type 2)...")
    stg_cur.execute("""
        SELECT DISTINCT ON (v.businessentityid)
            v.businessentityid, v.name, v.creditrating,
            v.preferredvendorstatus, v.activeflag,
            a.city, sp.name, cr.name
        FROM stg_vendor v
        LEFT JOIN stg_businessentityaddress bea
            ON v.businessentityid = bea.businessentityid
        LEFT JOIN stg_address a
            ON bea.addressid = a.addressid
        LEFT JOIN stg_stateprovince sp
            ON a.stateprovinceid = sp.stateprovinceid
        LEFT JOIN stg_countryregion cr
            ON sp.countryregioncode = cr.countryregioncode
        ORDER BY v.businessentityid, a.addressid
    """)
    today = date.today()
    ins, scd2, ow = 0, 0, 0

    for row in stg_cur.fetchall():
        (beid, vname, crating, prefstatus, aflag, vcity, vstate, vcountry) = row
        vname = null_to_tidak_ada(vname)
        vcity = null_to_tidak_ada(vcity)
        vstate = null_to_tidak_ada(vstate)
        vcountry = null_to_tidak_ada(vcountry)
        olap_cur.execute("""
            SELECT vendor_key, credit_rating, preferred_vendor_status
            FROM dim_vendor WHERE business_entity_id = %s AND scd_is_current = TRUE
        """, (beid,))
        ex = olap_cur.fetchone()

        if ex is None:
            olap_cur.execute("""
                INSERT INTO dim_vendor (business_entity_id, vendor_name, credit_rating,
                    preferred_vendor_status, active_flag, vendor_city, vendor_state,
                    vendor_country, scd_effective_date, scd_expiry_date, scd_is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
            """, (beid, vname, crating, prefstatus, aflag, vcity, vstate, vcountry, today))
            ins += 1
        else:
            old_key, old_cr, old_ps = ex
            if old_cr != crating or old_ps != prefstatus:
                olap_cur.execute("""
                    UPDATE dim_vendor SET scd_expiry_date = %s, scd_is_current = FALSE
                    WHERE vendor_key = %s
                """, (today, old_key))
                olap_cur.execute("""
                    INSERT INTO dim_vendor (business_entity_id, vendor_name, credit_rating,
                        preferred_vendor_status, active_flag, vendor_city, vendor_state,
                        vendor_country, scd_effective_date, scd_expiry_date, scd_is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
                """, (beid, vname, crating, prefstatus, aflag, vcity, vstate, vcountry, today))
                scd2 += 1
            else:
                olap_cur.execute("""
                    UPDATE dim_vendor SET vendor_name = %s, active_flag = %s,
                        vendor_city = %s, vendor_state = %s, vendor_country = %s
                    WHERE vendor_key = %s
                """, (vname, aflag, vcity, vstate, vcountry, old_key))
                ow += 1

    print(f"  [DIM_VENDOR] New: {ins}, SCD2: {scd2}, Overwrite: {ow}")


def incr_dim_product(stg_cur, olap_cur):
    """SCD Type 2: Compare standard_price → expire old + insert new if changed."""
    print("\n  [DIM_PRODUCT] Incremental (SCD Type 2)...")

    # Check if there are any PO details in staging (for incremental, might be empty)
    stg_cur.execute("SELECT COUNT(*) FROM stg_purchaseorderdetail")
    pod_count = stg_cur.fetchone()[0]

    if pod_count == 0:
        # No new POs, but still check existing products for price changes
        stg_cur.execute("""
            SELECT p.productid, p.name, p.productnumber, p.color,
                   COALESCE(pv.standardprice, 0), pv.unitmeasurecode,
                   psc.name, pc.name
            FROM stg_product p
            LEFT JOIN (
                SELECT productid, MAX(standardprice) AS standardprice,
                       MIN(unitmeasurecode) AS unitmeasurecode
                FROM stg_productvendor GROUP BY productid
            ) pv ON p.productid = pv.productid
            LEFT JOIN stg_productsubcategory psc
                ON p.productsubcategoryid = psc.productsubcategoryid
            LEFT JOIN stg_productcategory pc
                ON psc.productcategoryid = pc.productcategoryid
            WHERE p.productid IN (
                SELECT DISTINCT product_id FROM dim_product WHERE scd_is_current = TRUE
            )
        """)
    else:
        stg_cur.execute("""
            SELECT p.productid, p.name, p.productnumber, p.color,
                   COALESCE(pv.standardprice, 0), pv.unitmeasurecode,
                   psc.name, pc.name
            FROM stg_product p
            LEFT JOIN (
                SELECT productid, MAX(standardprice) AS standardprice,
                       MIN(unitmeasurecode) AS unitmeasurecode
                FROM stg_productvendor GROUP BY productid
            ) pv ON p.productid = pv.productid
            LEFT JOIN stg_productsubcategory psc
                ON p.productsubcategoryid = psc.productsubcategoryid
            LEFT JOIN stg_productcategory pc
                ON psc.productcategoryid = pc.productcategoryid
            WHERE p.productid IN (
                SELECT DISTINCT productid FROM stg_purchaseorderdetail
            )
        """)

    today = date.today()
    ins, scd2, ow = 0, 0, 0

    for row in stg_cur.fetchall():
        (pid, pname, pnum, color, stdprice, umc, subcat, cat) = row
        pname = null_to_tidak_ada(pname)
        pnum = null_to_tidak_ada(pnum)
        color = null_to_tidak_ada(color)
        umc = null_to_tidak_ada(umc)
        subcat = null_to_tidak_ada(subcat)
        cat = null_to_tidak_ada(cat)

        # Need to read from OLAP DB, not staging
        olap_cur.execute("""
            SELECT product_key, standard_price FROM dim_product
            WHERE product_id = %s AND scd_is_current = TRUE
        """, (pid,))
        ex = olap_cur.fetchone()

        if ex is None:
            olap_cur.execute("""
                INSERT INTO dim_product (product_id, product_name, product_number, color,
                    standard_price, unit_measure_code, subcategory_name, category_name,
                    scd_effective_date, scd_expiry_date, scd_is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
            """, (pid, pname, pnum, color, stdprice, umc, subcat, cat, today))
            ins += 1
        else:
            old_key, old_sp = ex
            if old_sp != stdprice:
                olap_cur.execute(
                    "UPDATE dim_product SET scd_expiry_date = %s, scd_is_current = FALSE WHERE product_key = %s",
                    (today, old_key))
                olap_cur.execute("""
                    INSERT INTO dim_product (product_id, product_name, product_number, color,
                        standard_price, unit_measure_code, subcategory_name, category_name,
                        scd_effective_date, scd_expiry_date, scd_is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
                """, (pid, pname, pnum, color, stdprice, umc, subcat, cat, today))
                scd2 += 1
            else:
                olap_cur.execute("""
                    UPDATE dim_product SET product_name = %s, product_number = %s, color = %s,
                        unit_measure_code = %s, subcategory_name = %s, category_name = %s
                    WHERE product_key = %s
                """, (pname, pnum, color, umc, subcat, cat, old_key))
                ow += 1

    print(f"  [DIM_PRODUCT] New: {ins}, SCD2: {scd2}, Overwrite: {ow}")


def run_transform_dimensions_incremental():
    print("\n" + "=" * 60)
    print("PHASE 2: INCREMENTAL TRANSFORM DIMENSIONS")
    print("=" * 60)

    stg_conn = get_staging_conn()
    olap_conn = get_olap_conn()
    stg_cur = stg_conn.cursor()
    olap_cur = olap_conn.cursor()

    try:
        incr_dim_shipmethod(stg_cur, olap_cur)
        incr_dim_vendor(stg_cur, olap_cur)
        incr_dim_product(stg_cur, olap_cur)
        olap_conn.commit()
        print("\n  [DIMENSIONS] ✓ Incremental dimension load complete")
    except Exception as e:
        olap_conn.rollback()
        raise e
    finally:
        stg_cur.close()
        olap_cur.close()
        stg_conn.close()
        olap_conn.close()


# ============================================================
# PHASE 3: INCREMENTAL FACT LOAD
# ============================================================

def run_transform_fact_incremental():
    """Hanya insert fact rows dari PO BARU di staging. Tidak truncate."""
    print("\n" + "=" * 60)
    print("PHASE 3: INCREMENTAL FACT LOAD")
    print("=" * 60)

    stg_conn = get_staging_conn()
    olap_conn = get_olap_conn()
    stg_cur = stg_conn.cursor()
    olap_cur = olap_conn.cursor()

    try:
        stg_cur.execute("SELECT COUNT(*) FROM stg_purchaseorderdetail")
        count = stg_cur.fetchone()[0]
        if count == 0:
            print("  [FACT] No new PO data in staging. Skipped.")
            return

        stg_cur.execute("""
            SELECT
                pod.purchaseorderid,
                poh.orderdate,
                poh.shipdate,
                poh.vendorid,
                poh.shipmethodid,
                poh.status,
                pod.productid,
                pod.orderqty,
                pod.receivedqty,
                pod.rejectedqty,
                pod.stockedqty,
                pod.unitprice,
                pod.linetotal
            FROM stg_purchaseorderdetail pod
            JOIN stg_purchaseorderheader poh
                ON pod.purchaseorderid = poh.purchaseorderid
        """)
        staging_rows = stg_cur.fetchall()

        # Build lookups
        olap_cur.execute("SELECT business_entity_id, vendor_key FROM dim_vendor WHERE scd_is_current = TRUE")
        vendor_lk = {r[0]: r[1] for r in olap_cur.fetchall()}
        olap_cur.execute("SELECT product_id, product_key FROM dim_product WHERE scd_is_current = TRUE")
        product_lk = {r[0]: r[1] for r in olap_cur.fetchall()}
        olap_cur.execute("SELECT ship_method_id, ship_method_key FROM dim_shipmethod")
        shipmethod_lk = {r[0]: r[1] for r in olap_cur.fetchall()}

        # Delete existing rows for these POs (upsert pattern)
        po_ids = list(set(r[0] for r in staging_rows))
        if po_ids:
            placeholders = ", ".join(["%s"] * len(po_ids))
            olap_cur.execute(
                f"DELETE FROM fact_goodsreceiving WHERE purchase_order_id IN ({placeholders})",
                po_ids)
            deleted = olap_cur.rowcount
            if deleted:
                print(f"  [FACT] Deleted {deleted} existing rows for updated POs")

        fact_rows = []
        skipped = 0
        for row in staging_rows:
            (po_id, orderdate, shipdate, vendorid, shipmethodid, status,
             productid, orderqty, receivedqty, rejectedqty, stockedqty,
             unitprice, linetotal) = row

            vk = vendor_lk.get(vendorid)
            pk = product_lk.get(productid)
            smk = shipmethod_lk.get(shipmethodid)
            if not vk or not pk or not smk:
                skipped += 1
                continue

            dk_order = int(orderdate.strftime("%Y%m%d")) if orderdate else None
            dk_ship = int(shipdate.strftime("%Y%m%d")) if shipdate else None

            if dk_order is None:
                skipped += 1
                continue

            fact_rows.append((
                dk_order, dk_ship, vk, pk, smk,
                po_id, status,
                int(orderqty), int(receivedqty), int(rejectedqty), int(stockedqty),
                float(unitprice), float(linetotal)
            ))

        if fact_rows:
            olap_cur.executemany("""
                INSERT INTO fact_goodsreceiving
                (date_key_order, date_key_ship, vendor_key, product_key, ship_method_key,
                 purchase_order_id, po_status,
                 order_qty, received_qty, rejected_qty, stocked_qty, unit_price, line_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, fact_rows)

        olap_conn.commit()
        print(f"  [FACT] {len(fact_rows)} rows inserted (incremental)")
        if skipped:
            print(f"  [FACT] {skipped} rows skipped (lookup failed)")
        print("  [FACT] ✓ Incremental fact load complete")

    except Exception as e:
        olap_conn.rollback()
        raise e
    finally:
        stg_cur.close()
        olap_cur.close()
        stg_conn.close()
        olap_conn.close()


# ============================================================
# MAIN
# ============================================================
def main():
    start = time.time()
    print("=" * 60)
    print("INCREMENTAL LOAD")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Get watermark from OLAP
    olap_conn = get_olap_conn()
    olap_cur = olap_conn.cursor()
    last_load = get_last_load_date(olap_cur)
    olap_cur.close()
    olap_conn.close()
    print(f"Last load date (watermark): {last_load}")

    new_po_count = run_extract_incremental(last_load)
    run_transform_dimensions_incremental()

    if new_po_count > 0:
        run_transform_fact_incremental()
    else:
        print("\n  [FACT] No new POs to load. Skipped.")

    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"INCREMENTAL LOAD COMPLETE! ({elapsed:.2f}s)")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
