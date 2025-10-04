from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/raw_finance_transactions_daily.csv"
CSV_PATHGl = "/opt/airflow/data/gl_account_hierarchy.csv"
CSV_PATHCost = "/opt/airflow/data/cost_center_hierarchy.csv"
CSV_PATHProfit = "/opt/airflow/data/profit_center_hierarchy.csv"


def run_etl():
    print("ETL started", flush=True)
    hook = PostgresHook(postgres_conn_id="postgres_localhost", schema="test")
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS FactFinancialTransaction;")
    cur.execute("DROP TABLE IF EXISTS DimDate;")
    cur.execute("DROP TABLE IF EXISTS DimProfitCenter;")
    cur.execute("DROP TABLE IF EXISTS DimGLAccount;")
    cur.execute("DROP TABLE IF EXISTS DimCostCenter;")

    ddl = """
    CREATE TABLE IF NOT EXISTS DimCostCenter(
       CostCenterKey SERIAL PRIMARY KEY,
       CostCenterCode VARCHAR(255) UNIQUE,
       CostCenterName VARCHAR(255),
       level1Name VARCHAR(255),
       Level2Name VARCHAR(255),
       Level3Name VARCHAR(255),
       Level1Type VARCHAR(255),
       Level2Type VARCHAR(255),
       Level3Type VARCHAR(255),
       ValidFlag BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS DimGLAccount(
       GlAccountKey SERIAL PRIMARY KEY,
       GlCode VARCHAR(255) UNIQUE,
       GlName VARCHAR(255),
       Currency VARCHAR(255),
       level1Name VARCHAR(255),
       Level2Name VARCHAR(255),
       Level3Name VARCHAR(255),
       Level1Type VARCHAR(255),
       Level2Type VARCHAR(255),
       Level3Type VARCHAR(255),
       ValidFlag BOOLEAN DEFAULT TRUE

    );

    CREATE TABLE IF NOT EXISTS DimProfitCenter(
       ProfitCenterKey SERIAL PRIMARY KEY,
       ProfitCenterCode VARCHAR(255) UNIQUE,
       ProfitCenterName VARCHAR(255),
       Region VARCHAR(255),
       City VARCHAR(255),
       ServiceLine VARCHAR(255),
       Level1Name VARCHAR(255),
       Level2Name VARCHAR(255),
       Level3Name VARCHAR(255),
       Level4Name VARCHAR(255),
       Level5Name VARCHAR(255),
       Level1Type VARCHAR(255),
       Level2Type VARCHAR(255),
       Level3Type VARCHAR(255),
       Level4Type VARCHAR(255),
       Level5Type VARCHAR(255),
       ValidFlag BOOLEAN DEFAULT TRUE

    );

    CREATE TABLE IF NOT EXISTS DimDate(
       DateKey SERIAL PRIMARY KEY,
       Date DATE UNIQUE

    );

    CREATE TABLE IF NOT EXISTS FactFinancialTransaction(
        ProfitCenterKey INT NOT NULL REFERENCES DimProfitCenter(ProfitCenterKey),
        CostCenterKey   INT NOT NULL REFERENCES DimCostCenter(CostCenterKey),
        GLAccountKey    INT NOT NULL REFERENCES DimGLAccount(GLAccountKey),
        DateKey         INT NOT NULL REFERENCES DimDate(DateKey),
        Amount          NUMERIC(18,2) NOT NULL,
        CONSTRAINT unique_transaction_test UNIQUE (ProfitCenterKey, CostCenterKey, GLAccountKey, DateKey)
    );

    """
    cur.execute(ddl)
    conn.commit()

    df = pd.read_csv(CSV_PATH, parse_dates=["date"], dayfirst=True).fillna("")
    dfGl = pd.read_csv(CSV_PATHGl).fillna("")
    dfCost = pd.read_csv(CSV_PATHCost).fillna("")
    dfProfit = pd.read_csv(CSV_PATHProfit).fillna("")

    dfGl["parent_gl_code"] = (
        dfGl["parent_gl_code"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    cur.execute(
        """
        CREATE TEMP TABLE stg_transaction (
            date DATE,
            profit_center_code VARCHAR(50) NOT NULL,
            cost_center_code VARCHAR(50) NOT NULL,
            gl_account_code VARCHAR(50) NOT NULL,
            amount NUMERIC NOT NULL  CHECK (amount >= 0),
            currency VARCHAR(10) NOT NULL  CHECK (currency = 'NZD'),
            region VARCHAR(50) NOT NULL  CHECK (region IN ('NZ','AU')),
            city VARCHAR(50) NOT NULL  CHECK (city IN ('Auckland', 'Wellington', 'Sydney', 'Melbourne')),
            service_line VARCHAR(50) NOT NULL  CHECK (service_line IN ('Electricity', 'Water', 'Waste', 'Gas'))

      );
    """
    )

    cur.execute(
        """
        CREATE TEMP TABLE stg_gl_account_hierarchy (
            node_code VARCHAR(50) PRIMARY KEY,
            node_name VARCHAR(255) NOT NULL,
            node_type VARCHAR(50) NOT NULL,
            parent_code VARCHAR(50)
      );
    """
    )

    cur.execute(
        """
        CREATE TEMP TABLE stg_cost_center_hierarchy (
            node_code VARCHAR(50) PRIMARY KEY,
            node_name VARCHAR(255) NOT NULL,
            level VARCHAR(50) NOT NULL,
            parent_code VARCHAR(50)
        );
    """
    )

    cur.execute(
        """
        CREATE TEMP TABLE stg_profit_center_hierarchy (
            node_code VARCHAR(50) PRIMARY KEY,
            node_name VARCHAR(255) NOT NULL,
            level VARCHAR(50) NOT NULL,
            parent_code VARCHAR(50),
            valid_bit BOOLEAN DEFAULT TRUE
        );
    """
    )

    cols = [
        "date",
        "profit_center_code",
        "cost_center_code",
        "gl_account_code",
        "amount",
        "currency",
        "region",
        "city",
        "service_line",
    ]

    colsGl = ["gl_code", "gl_name", "node_type", "parent_gl_code"]

    colsCost = ["node_code", "node_name", "level", "parent_code"]

    colsProfit = ["node_code", "node_name", "level", "parent_code"]

    buf = StringIO()
    df[cols].to_csv(buf, index=False, header=False)
    buf.seek(0)

    bufGl = StringIO()
    dfGl[colsGl].to_csv(bufGl, index=False, header=False)
    bufGl.seek(0)

    bufCost = StringIO()
    dfCost[colsCost].to_csv(bufCost, index=False, header=False)
    bufCost.seek(0)

    bufProfit = StringIO()
    dfProfit[colsProfit].to_csv(bufProfit, index=False, header=False)
    bufProfit.seek(0)

    cur.copy_expert(
        """
      COPY stg_transaction (
        date, profit_center_code, cost_center_code, gl_account_code,
        amount, currency, region, city, service_line

      ) FROM STDIN WITH CSV NULL ''
    """,
        buf,
    )

    cur.copy_expert(
        """
        COPY stg_gl_account_hierarchy (
            node_code, node_name, node_type, parent_code
      ) FROM STDIN WITH CSV NULL ''
    """,
        bufGl,
    )

    cur.copy_expert(
        """
        COPY stg_cost_center_hierarchy (
            node_code, node_name, level, parent_code
      ) FROM STDIN WITH CSV NULL ''
    """,
        bufCost,
    )

    cur.copy_expert(
        """
        COPY stg_profit_center_hierarchy (
            node_code, node_name, level, parent_code
      ) FROM STDIN WITH CSV NULL ''
    """,
        bufProfit,
    )

    '''
    cur.execute("""
        UPDATE stg_gl_account_hierarchy
        SET parent_code = NULL
        WHERE parent_code = '';

        UPDATE stg_cost_center_hierarchy
        SET parent_code = NULL
        WHERE parent_code = '';

        UPDATE stg_profit_center_hierarchy
        SET parent_code = NULL
        WHERE parent_code = '';

        UPDATE stg_gl_account_hierarchy
        SET node_code = TRIM(node_code), parent_code = TRIM(parent_code);

        UPDATE stg_cost_center_hierarchy
        SET node_code = TRIM(node_code), parent_code = TRIM(parent_code);

        UPDATE stg_profit_center_hierarchy
        SET node_code = TRIM(node_code), parent_code = TRIM(parent_code);

        UPDATE stg_transaction
        SET gl_account_code = TRIM(gl_account_code),
            cost_center_code = TRIM(cost_center_code),
            profit_center_code = TRIM(profit_center_code);

    """)

    '''

    cur.execute(
        """
      WITH RECURSIVE gl_hierarchy AS (
          SELECT node_code, node_name, node_type, parent_code, node_name::text AS path_names, node_type::text AS path_types, 1 AS level
          FROM stg_gl_account_hierarchy
          WHERE parent_code IS NULL OR TRIM(parent_code) = ''
          UNION ALL
          SELECT s.node_code, s.node_name, s.node_type, s.parent_code, g.path_names || ' > ' || s.node_name, g.path_types || ' > ' || s.node_type, g.level + 1
          FROM stg_gl_account_hierarchy s
          INNER JOIN gl_hierarchy g ON TRIM(s.parent_code) = TRIM(g.node_code)
      ),
      leafs AS (
         SELECT h.*
         FROM gl_hierarchy h
         WHERE NOT EXISTS (
                SELECT 1
                FROM stg_gl_account_hierarchy s
                WHERE TRIM(s.parent_code) = TRIM(h.node_code)
            )
      ),
      split_levels AS (
            SELECT l.node_code, l.node_name, l.node_type,l.parent_code,
            split_part(l.path_names, ' > ', 1) AS level1Name, split_part(l.path_names, ' > ', 2) AS level2Name, split_part(l.path_names, ' > ', 3) AS level3Name,
            split_part(l.path_types, ' > ', 1) AS level1Type, split_part(l.path_types, ' > ', 2) AS level2Type, split_part(l.path_types, ' > ', 3) AS level3Type
            FROM leafs l
        ),
        with_currency AS (
         SELECT DISTINCT sl.node_code, sl.node_name, sl.level1Name, sl.level2Name, sl.level3Name, sl.level1Type, sl.level2Type, sl.level3Type, t.currency
         FROM split_levels sl
         JOIN (
            SELECT DISTINCT  TRIM(gl_account_code) AS gl_account_code, currency
            FROM stg_transaction
         ) t ON TRIM(sl.node_code) = t.gl_account_code
        )

        INSERT INTO DimGLAccount(GlCode, GlName, Currency, level1Name, level2Name, level3Name, level1Type, level2Type, level3Type, ValidFlag)
        SELECT node_code, node_name, currency, level1Name, level2Name, level3Name, level1Type, level2Type, level3Type, TRUE
        FROM with_currency
        ON CONFLICT (GlCode) DO NOTHING;
    """
    )

    cur.execute(
        """
      WITH RECURSIVE cost_center_hierarchy AS (
            SELECT node_code, node_name, level, parent_code, node_name::text AS path_names, level::text AS path_levels, 1 AS level_num
            FROM stg_cost_center_hierarchy
            WHERE parent_code IS NULL OR TRIM(parent_code) = ''
            UNION ALL
            SELECT s.node_code, s.node_name, s.level, s.parent_code, c.path_names || ' > ' || s.node_name, c.path_levels || ' > ' || s.level, c.level_num + 1
            FROM stg_cost_center_hierarchy s
            INNER JOIN cost_center_hierarchy c ON TRIM(s.parent_code) = TRIM(c.node_code)
        ),
        leafs AS (
            SELECT h.*
            FROM cost_center_hierarchy h
            WHERE NOT EXISTS (
                SELECT 1
                FROM stg_cost_center_hierarchy s
                WHERE s.parent_code = h.node_code
            )
        ),
        split_levels AS (
            SELECT l.node_code, l.node_name, l.level, l.parent_code,
            split_part(l.path_names, ' > ', 1) AS level1Name, split_part(l.path_names, ' > ', 2) AS level2Name, split_part(l.path_names, ' > ', 3) AS level3Name,
            split_part(l.path_levels, ' > ', 1) AS level1Type, split_part(l.path_levels, ' > ', 2) AS level2Type, split_part(l.path_levels, ' > ', 3) AS level3Type
            FROM leafs l
        )
        INSERT INTO DimCostCenter(CostCenterCode,CostCenterName, level1Name, level2Name, level3Name, level1Type, level2Type, level3Type, ValidFlag)
        SELECT node_code, node_name, level1Name, level2Name, level3Name, level1Type, level2Type, level3Type, TRUE
        FROM split_levels
        ON CONFLICT (CostCenterCode) DO NOTHING;
    """
    )

    cur.execute(
        """
        WITH RECURSIVE profit_center_hierarchy AS (
            SELECT node_code, node_name, level, parent_code, node_name::text AS path_names, level::text AS path_levels, 1 AS level_num
            FROM stg_profit_center_hierarchy
            WHERE parent_code IS NULL OR TRIM(parent_code) = ''
            UNION ALL
            SELECT s.node_code, s.node_name, s.level, s.parent_code, p.path_names || ' > ' || s.node_name, p.path_levels || ' > ' || s.level, p.level_num + 1
            FROM stg_profit_center_hierarchy s
            INNER JOIN profit_center_hierarchy p ON TRIM(s.parent_code) = TRIM(p.node_code)
        ),
        leafs AS (
            SELECT h.*
            FROM profit_center_hierarchy h
            WHERE NOT EXISTS (
                SELECT 1
                FROM stg_profit_center_hierarchy s
                WHERE s.parent_code = h.node_code
            )
        ),
        split_levels AS (
            SELECT l.node_code, l.node_name, l.level, l.parent_code,
            split_part(l.path_names, ' > ', 1) AS level1Name, split_part(l.path_names, ' > ', 2) AS level2Name, split_part(l.path_names, ' > ', 3) AS level3Name,
            split_part(l.path_names, ' > ', 4) AS level4Name, split_part(l.path_names, ' > ', 5) AS level5Name,
            split_part(l.path_levels, ' > ', 1) AS level1Type, split_part(l.path_levels, ' > ', 2) AS level2Type, split_part(l.path_levels, ' > ', 3) AS level3Type,
            split_part(l.path_levels, ' > ', 4) AS level4Type, split_part(l.path_levels, ' > ', 5) AS level5Type
            FROM leafs l
        ),
        join_with_transaction AS (
            SELECT DISTINCT sl.node_code, sl.node_name, sl.level1Name, sl.level2Name, sl.level3Name, sl.level4Name, sl.level5Name, sl.level1Type, sl.level2Type, sl.level3Type, sl.level4Type, sl.level5Type, t.region, t.city, t.service_line
            FROM split_levels sl
            JOIN stg_transaction t ON TRIM(sl.node_code) = TRIM(t.profit_center_code)
        )
        INSERT INTO DimProfitCenter(ProfitCenterCode, ProfitCenterName, region, city, ServiceLine, level1Name, level2Name, level3Name, level4Name, level5Name, level1Type, level2Type, level3Type, level4Type, level5Type, ValidFlag)
        SELECT node_code, node_name, region, city, service_line, level1Name, level2Name, level3Name, level4Name, level5Name, level1Type, level2Type, level3Type, level4Type, level5Type, TRUE
        FROM join_with_transaction
        ON CONFLICT (ProfitCenterCode) DO NOTHING;

    """
    )

    cur.execute(
        """
        INSERT INTO DimDate (date)
        SELECT DISTINCT date
        FROM stg_transaction
        ON CONFLICT (date) DO NOTHING;
    """
    )

    cur.execute(
        """
        INSERT INTO FactFinancialTransaction (ProfitCenterKey, CostCenterKey, GlAccountKey, DateKey, amount)
        SELECT dp.ProfitCenterKey, dc.CostCenterKey, dg.GlAccountKey, dd.DateKey,
        CASE
            WHEN dg.GlCode = '8200' THEN -1 * st.amount
            When dg.Level1Name = 'Revenue' OR dg.Level1Name = 'Other Income' THEN st.amount
            ELSE -1 * st.amount
        END AS amount
        FROM stg_transaction st
        JOIN DimProfitCenter dp ON st.profit_center_code = dp.ProfitCenterCode AND dp.ValidFlag = TRUE
        JOIN DimCostCenter dc ON st.cost_center_code = dc.CostCenterCode AND dc.ValidFlag = TRUE
        JOIN DimGLAccount dg ON st.gl_account_code = dg.GlCode  AND dg.ValidFlag = TRUE
        JOIN DimDate dd ON st.date = dd.date
        ON CONFLICT ON CONSTRAINT unique_transaction_test DO NOTHING;
    """
    )


default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Project_2",
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
