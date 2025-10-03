from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/online_sales_dataset.csv"


def run_etl():
    print("ETL started", flush=True)
    hook = PostgresHook(postgres_conn_id="postgres_localhost", schema="test")
    conn = hook.get_conn()
    conn.autocommit = True  
    cur = conn.cursor()

    cur.execute("drop table FactSales")

    ddl = """
    CREATE TABLE IF NOT EXISTS DimProduct(
        DimProduct_key SERIAL PRIMARY KEY,
        StockCode VARCHAR(50) NOT NULL,
        Description VARCHAR(255),
        CONSTRAINT UQ_DimProduct UNIQUE(StockCode, Description)
    );
    CREATE TABLE IF NOT EXISTS DimWarehouseLocation(
        DimWarehouseLocation_key SERIAL PRIMARY KEY,
        WarehouseLocation VARCHAR(100) NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS DimCategory(
        DimCategory_key SERIAL PRIMARY KEY,
        Category VARCHAR(100) NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS DimOrderShipment(
        DimOrderShipment_key SERIAL PRIMARY KEY,
        ReturnStatus VARCHAR(50) NOT NULL,
        ShipmentProvider VARCHAR(100) NOT NULL,
        CONSTRAINT UQ_DimOrderShipment UNIQUE(ReturnStatus, ShipmentProvider)
    );
    CREATE TABLE IF NOT EXISTS DimCustomer(
        DimCustomer_key SERIAL PRIMARY KEY,
        CustomerID VARCHAR(50) NOT NULL,
        Country VARCHAR(100),
        CONSTRAINT UQ_DimCustomer UNIQUE(CustomerID, Country)
    );
    CREATE TABLE IF NOT EXISTS DimOrderInfo(
        DimOrderInfo_key SERIAL PRIMARY KEY,
        OrderPriority VARCHAR(50) NOT NULL,
        PaymentMethod VARCHAR(100) NOT NULL,
        SalesChannel VARCHAR(100) NOT NULL,
        CONSTRAINT UQ_DimOrderInfo UNIQUE(OrderPriority, PaymentMethod, SalesChannel)
    );
    CREATE TABLE IF NOT EXISTS DimInvoiceDate(
        DimInvoiceDate_key SERIAL PRIMARY KEY,
        InvoiceDate DATE NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS FactSales(
        FactSalesID BIGSERIAL PRIMARY KEY,
        DimProduct_key INT NOT NULL REFERENCES DimProduct(DimProduct_key),
        DimOrderInfo_key INT NOT NULL REFERENCES DimOrderInfo(DimOrderInfo_key),
        DimCustomer_key INT NOT NULL REFERENCES DimCustomer(DimCustomer_key),
        DimInvoiceDate_key INT NOT NULL,
        DimOrderShipment_key INT NOT NULL REFERENCES DimOrderShipment(DimOrderShipment_key),
        DimWarehouseLocation_key INT NOT NULL REFERENCES DimWarehouseLocation(DimWarehouseLocation_key),
        DimCategory_key INT NOT NULL REFERENCES DimCategory(DimCategory_key),
        Quantity INT,
        UnitPrice NUMERIC(18,2),
        ShippingCost NUMERIC(18,2),
        InvoiceNo VARCHAR(50),
        Discount NUMERIC(18,2)
    );
    """
    cur.execute(ddl)
    conn.commit()

    df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
    df["InvoiceDate_parsed"] = pd.to_datetime(
        df["InvoiceDate"].str.strip(), dayfirst=True, errors="coerce"
    ).dt.date
    print(f"Loaded {len(df)} rows from CSV", flush=True)

    def to_int(x):
        try:
            return int(x) if isinstance(x, str) and x.strip() else None
        except ValueError:
            return None

    def to_dec(x):
        try:
            return Decimal(x) if isinstance(x, str) and x.strip() else None
        except (InvalidOperation, ValueError):
            return None

    def to_str_or_none(x):
        if x is None:
            return None
        x = x.strip() if isinstance(x, str) else x
        return x or None

    def insert_then_get_id(cur, insert_sql: str, select_sql: str, params: tuple):
        cur.execute(insert_sql, params)
        row = cur.fetchone()  
        if row:
            return row[0]
        cur.execute(select_sql, params)
        return cur.fetchone()[0]


    sql_dim_product_ins = """
        INSERT INTO DimProduct(StockCode, Description)
        VALUES (%s, %s)
        ON CONFLICT (StockCode, Description) DO NOTHING
        RETURNING DimProduct_key;
    """
    sql_dim_product_sel = """
        SELECT DimProduct_key
        FROM DimProduct
        WHERE StockCode=%s AND Description=%s;
    """

    sql_dim_wh_loc_ins = """
        INSERT INTO DimWarehouseLocation(WarehouseLocation)
        VALUES (%s)
        ON CONFLICT (WarehouseLocation) DO NOTHING
        RETURNING DimWarehouseLocation_key;
    """
    sql_dim_wh_loc_sel = """
        SELECT DimWarehouseLocation_key
        FROM DimWarehouseLocation
        WHERE WarehouseLocation=%s;
    """

    sql_dim_category_ins = """
        INSERT INTO DimCategory(Category)
        VALUES (%s)
        ON CONFLICT (Category) DO NOTHING
        RETURNING DimCategory_key;
    """
    sql_dim_category_sel = """
        SELECT DimCategory_key
        FROM DimCategory
        WHERE Category=%s;
    """

    sql_dim_shipper_ins = """
        INSERT INTO DimOrderShipment(ReturnStatus, ShipmentProvider)
        VALUES (%s, %s)
        ON CONFLICT (ReturnStatus, ShipmentProvider) DO NOTHING
        RETURNING DimOrderShipment_key;
    """
    sql_dim_shipper_sel = """
        SELECT DimOrderShipment_key
        FROM DimOrderShipment
        WHERE ReturnStatus=%s AND ShipmentProvider=%s;
    """

    sql_dim_customer_ins = """
        INSERT INTO DimCustomer(CustomerID, Country)
        VALUES (%s, %s)
        ON CONFLICT (CustomerID, Country) DO NOTHING
        RETURNING DimCustomer_key;
    """
    sql_dim_customer_sel = """
        SELECT DimCustomer_key
        FROM DimCustomer
        WHERE CustomerID=%s AND Country=%s;
    """

    sql_dim_order_ins = """
        INSERT INTO DimOrderInfo(OrderPriority, PaymentMethod, SalesChannel)
        VALUES (%s, %s, %s)
        ON CONFLICT (OrderPriority, PaymentMethod, SalesChannel) DO NOTHING
        RETURNING DimOrderInfo_key;
    """
    sql_dim_order_sel = """
        SELECT DimOrderInfo_key
        FROM DimOrderInfo
        WHERE OrderPriority=%s AND PaymentMethod=%s AND SalesChannel=%s;
    """

    sql_dim_invdate_ins = """
        INSERT INTO DimInvoiceDate(InvoiceDate)
        VALUES (%s)
        ON CONFLICT (InvoiceDate) DO NOTHING
        RETURNING DimInvoiceDate_key;
    """
    sql_dim_invdate_sel = """
        SELECT DimInvoiceDate_key
        FROM DimInvoiceDate
        WHERE InvoiceDate=%s;
    """

    insert_fact = """
    INSERT INTO FactSales(
        DimProduct_key, DimOrderInfo_key, DimCustomer_key,
        DimInvoiceDate_key, DimOrderShipment_key,
        DimWarehouseLocation_key, DimCategory_key,
        Quantity, UnitPrice, ShippingCost, InvoiceNo, Discount
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    inserted_facts = 0
    skipped_rows = 0
    errors = 0

    for row in df.itertuples(index=False):
        print(f"Processing row: {row}", flush=True)
        stockcode      = to_str_or_none(row.StockCode)
        description    = to_str_or_none(row.Description)
        quantity       = to_int(row.Quantity)
        unit_price     = to_dec(row.UnitPrice)
        shipping_cost  = to_dec(row.ShippingCost)
        discount       = to_dec(row.Discount)
        invoice_no     = to_str_or_none(row.InvoiceNo)
        customer_id    = to_str_or_none(row.CustomerID)
        country        = to_str_or_none(row.Country)
        payment_method = to_str_or_none(row.PaymentMethod)
        order_priority = to_str_or_none(row.OrderPriority)
        shipper        = to_str_or_none(row.ShipmentProvider)
        channel        = to_str_or_none(row.SalesChannel)
        wh_loc         = to_str_or_none(row.WarehouseLocation)
        return_status  = to_str_or_none(row.ReturnStatus)
        category       = to_str_or_none(row.Category)
        inv_date       = int(row.InvoiceDate_parsed.strftime('%Y%m%d'))

        required = [stockcode, wh_loc, category, return_status, shipper,
                customer_id, order_priority, payment_method, channel, inv_date]
        if not all(required):
            skipped_rows += 1
            print(f"Skipping row with missing required fields: {row}", flush=True)
            continue

       

        try:
            print("Inserting dimension records...", flush=True)
            k_product = insert_then_get_id(cur, sql_dim_product_ins,  sql_dim_product_sel,  (stockcode, description))
            k_whloc   = insert_then_get_id(cur, sql_dim_wh_loc_ins,   sql_dim_wh_loc_sel,   (wh_loc,))
            k_cat     = insert_then_get_id(cur, sql_dim_category_ins, sql_dim_category_sel, (category,))
            k_shipper = insert_then_get_id(cur, sql_dim_shipper_ins,  sql_dim_shipper_sel,  (return_status, shipper))
            k_cust    = insert_then_get_id(cur, sql_dim_customer_ins, sql_dim_customer_sel, (customer_id, country))
            k_order   = insert_then_get_id(cur, sql_dim_order_ins,    sql_dim_order_sel,    (order_priority, payment_method, channel))
            #k_date    = insert_then_get_id(cur, sql_dim_invdate_ins,  sql_dim_invdate_sel,  (inv_date,))

            cur.execute(insert_fact, (
                int(k_product), int(k_order), int(k_cust),
                inv_date, int(k_shipper),
                int(k_whloc), int(k_cat),
                quantity, unit_price, shipping_cost, invoice_no, discount
            ))
            inserted_facts += 1
        except Exception as e:
            errors += 1
            print(f"Error processing row {row}: {e}", flush=True)


            inserted_facts += 1
    

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted facts: {inserted_facts}, skipped (missing fields): {skipped_rows}, errors: {errors}", flush=True)


default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_with_postgres_operator_v01",
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval="0 0 * * *",
    catchup=False, 
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
