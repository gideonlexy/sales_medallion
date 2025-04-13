CREATE KEYSPACE IF NOT EXISTS sales_medallion 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS sales_medallion.bronze_sales (
    order_id TEXT PRIMARY KEY,
    region TEXT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    order_date TEXT,
    ship_date TEXT,
    units_sold INT,
    unit_price DECIMAL,
    unit_cost DECIMAL,
    total_revenue DECIMAL,
    total_cost DECIMAL,
    total_profit DECIMAL,
    raw_data TEXT,
    ingestion_timestamp TIMESTAMP
);