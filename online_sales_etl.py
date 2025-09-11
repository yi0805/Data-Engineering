from __future__ import annotations

import pandas as pd
from decimal import Decimal, InvalidOperation
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook

CSV_PATH = "./airflow/data/online_sales_dataset.csv"   
ODBC_CONN_ID = "odbc_mssql"                               

def run_etl():
    hook = OdbcHook(odbc_conn_id=ODBC_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    ddl = r"""
    IF OBJECT_ID('dbo.DimProduct','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimProduct(
        DimProduct_key INT IDENTITY(1,1) PRIMARY KEY,
        StockCode NVARCHAR(50) NOT NULL,
        Description NVARCHAR(255) NULL,
        CONSTRAINT UQ_DimProduct UNIQUE(StockCode, Description)
      );
    END;

    IF OBJECT_ID('dbo.DimWarehouseLocation','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimWarehouseLocation(
        DimWarehouseLocation_key INT IDENTITY(1,1) PRIMARY KEY,
        WarehouseLocation NVARCHAR(100) NOT NULL UNIQUE,
        ReturnStatus NVARCHAR(50) NOT NULL
      );
    END;

    IF OBJECT_ID('dbo.DimCategory','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimCategory(
        DimCategory_key INT IDENTITY(1,1) PRIMARY KEY,
        Category NVARCHAR(100) NOT NULL
      );
    END;

    IF OBJECT_ID('dbo.DimShipmentProvider','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimShipmentProvider(
        DimShipmentProvider_key INT IDENTITY(1,1) PRIMARY KEY,
        ShipmentProvider NVARCHAR(100) NOT NULL UNIQUE
      );
    END;

    IF OBJECT_ID('dbo.DimSalesChannel','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimSalesChannel(
        DimSalesChannel_key INT IDENTITY(1,1) PRIMARY KEY,
        SalesChannel NVARCHAR(100) NOT NULL UNIQUE
      );
    END;

    IF OBJECT_ID('dbo.DimCustomer','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimCustomer(
        DimCustomer_key INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID NVARCHAR(50) NOT NULL,
        Country NVARCHAR(100) NULL,
        CONSTRAINT UQ_DimCustomer UNIQUE(CustomerID, Country)
      );
    END;

    IF OBJECT_ID('dbo.DimPaymentMethod','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimPaymentMethod(
        DimPaymentMethod_key INT IDENTITY(1,1) PRIMARY KEY,
        PaymentMethod NVARCHAR(100) NOT NULL UNIQUE
      );
    END;

    IF OBJECT_ID('dbo.DimOrder','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimOrder(
        DimOrder_key INT IDENTITY(1,1) PRIMARY KEY,
        OrderPriority NVARCHAR(50) NOT NULL UNIQUE
      );
    END;

    IF OBJECT_ID('dbo.DimInvoiceDate','U') IS NULL
    BEGIN
      CREATE TABLE dbo.DimInvoiceDate(
        DimInvoiceDate_key INT IDENTITY(1,1) PRIMARY KEY,
        InvoiceDate DATE NOT NULL UNIQUE
      );
    END;

    IF OBJECT_ID('dbo.FactSales','U') IS NULL
    BEGIN
      CREATE TABLE dbo.FactSales(
        FactSalesID BIGINT IDENTITY(1,1) PRIMARY KEY,
        DimProduct_key INT NOT NULL,
        DimOrder_key INT NOT NULL,
        DimCustomer_key INT NOT NULL,
        DimPaymentMethod_key INT NOT NULL,
        DimInvoiceDate_key INT NOT NULL,
        DimShipmentProvider_key INT NOT NULL,
        DimSalesChannel_key INT NOT NULL,
        DimWarehouseLocation_key INT NOT NULL,
        DimCategory_key INT NOT NULL,
        Quantity INT NULL,
        UnitPrice DECIMAL(18,2) NULL,
        ShippingCost DECIMAL(18,2) NULL,
        InvoiceNo NVARCHAR(50) NULL,
        Discount DECIMAL(18,2) NULL,

        FOREIGN KEY (DimProduct_key)           REFERENCES dbo.DimProduct(DimProduct_key),
        FOREIGN KEY (DimOrder_key)             REFERENCES dbo.DimOrder(DimOrder_key),
        FOREIGN KEY (DimCustomer_key)          REFERENCES dbo.DimCustomer(DimCustomer_key),
        FOREIGN KEY (DimPaymentMethod_key)     REFERENCES dbo.DimPaymentMethod(DimPaymentMethod_key),
        FOREIGN KEY (DimInvoiceDate_key)       REFERENCES dbo.DimInvoiceDate(DimInvoiceDate_key),
        FOREIGN KEY (DimShipmentProvider_key)  REFERENCES dbo.DimShipmentProvider(DimShipmentProvider_key),
        FOREIGN KEY (DimSalesChannel_key)      REFERENCES dbo.DimSalesChannel(DimSalesChannel_key),
        FOREIGN KEY (DimWarehouseLocation_key) REFERENCES dbo.DimWarehouseLocation(DimWarehouseLocation_key),
        FOREIGN KEY (DimCategory_key)          REFERENCES dbo.DimCategory(DimCategory_key)
      );
    END;
    """
    cur.execute(ddl)
    conn.commit()

    df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
    df["InvoiceDate_parsed"] = pd.to_datetime(
        df["InvoiceDate"].str.strip(), dayfirst=True, errors="coerce"
    ).dt.date

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
        if x is None: return None
        x = x.strip() if isinstance(x, str) else x
        return x or None

    def fetch_scalar(sql, params):
        cur.execute(sql, params)
        while cur.description is None:
            if not cur.nextset():
                raise RuntimeError("Statement returned no result set")
        row = cur.fetchone()
        return row[0] if row else None

    sql_dim_product = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimProduct_key FROM dbo.DimProduct
                     WHERE StockCode=? AND ISNULL(Description,'')=ISNULL(?, ''));
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimProduct(StockCode, Description) VALUES (?, ?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_wh_loc = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimWarehouseLocation_key FROM dbo.DimWarehouseLocation WHERE WarehouseLocation=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimWarehouseLocation(WarehouseLocation, ReturnStatus) VALUES (?, ?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_category = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimCategory_key FROM dbo.DimCategory WHERE Category=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimCategory(Category) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_shipper = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimShipmentProvider_key FROM dbo.DimShipmentProvider WHERE ShipmentProvider=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimShipmentProvider(ShipmentProvider) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_channel = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimSalesChannel_key FROM dbo.DimSalesChannel WHERE SalesChannel=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimSalesChannel(SalesChannel) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_customer = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimCustomer_key FROM dbo.DimCustomer
                     WHERE CustomerID=? AND ISNULL(Country,'')=ISNULL(?, ''));
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimCustomer(CustomerID, Country) VALUES (?, ?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_payment = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimPaymentMethod_key FROM dbo.DimPaymentMethod WHERE PaymentMethod=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimPaymentMethod(PaymentMethod) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_order = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimOrder_key FROM dbo.DimOrder WHERE OrderPriority=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimOrder(OrderPriority) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    sql_dim_invdate = r"""SET NOCOUNT ON;
    DECLARE @id INT=(SELECT DimInvoiceDate_key FROM dbo.DimInvoiceDate WHERE InvoiceDate=?);
    IF @id IS NULL BEGIN
      INSERT INTO dbo.DimInvoiceDate(InvoiceDate) VALUES (?);
      SET @id=SCOPE_IDENTITY();
    END
    SELECT @id;"""

    insert_fact = r"""
    INSERT INTO dbo.FactSales(
        DimProduct_key, DimOrder_key, DimCustomer_key, DimPaymentMethod_key,
        DimInvoiceDate_key, DimShipmentProvider_key, DimSalesChannel_key,
        DimWarehouseLocation_key, DimCategory_key,
        Quantity, UnitPrice, ShippingCost, InvoiceNo, Discount
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for row in df.itertuples(index=False):
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
        inv_date       = row.InvoiceDate_parsed

        if not all([stockcode, payment_method, order_priority, shipper, channel,
                    wh_loc, return_status, category, customer_id, inv_date]):
            continue

        try:
            k_product = fetch_scalar(sql_dim_product,  [stockcode, description, stockcode, description])
            k_whloc   = fetch_scalar(sql_dim_wh_loc,   [wh_loc,   wh_loc, return_status])
            k_cat     = fetch_scalar(sql_dim_category, [category, category])
            k_shipper = fetch_scalar(sql_dim_shipper,  [shipper,  shipper])
            k_chan    = fetch_scalar(sql_dim_channel,  [channel,  channel])
            k_cust    = fetch_scalar(sql_dim_customer, [customer_id, country, customer_id, country])
            k_pay     = fetch_scalar(sql_dim_payment,  [payment_method, payment_method])
            k_order   = fetch_scalar(sql_dim_order,    [order_priority, order_priority])
            k_date    = fetch_scalar(sql_dim_invdate,  [inv_date, inv_date])

            cur.execute(insert_fact, [
                int(k_product), int(k_order), int(k_cust), int(k_pay),
                int(k_date), int(k_shipper), int(k_chan),
                int(k_whloc), int(k_cat),
                quantity, unit_price, shipping_cost, invoice_no, discount
            ])
        except Exception as e:
            print(f"Error processing row {row}: {e}")


    conn.commit()

tz = pendulum.timezone("Pacific/Auckland")
with DAG(
    dag_id="online_sales_etl_10am",
    start_date=datetime(2024, 1, 1, tzinfo=tz),
    schedule="0 10 * * *",   
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "you"},
    tags=["sqlserver", "etl"],
):
    PythonOperator(task_id="run_etl", python_callable=run_etl)
