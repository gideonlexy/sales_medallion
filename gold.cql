-- Sales by Region and Item Type
CREATE TABLE IF NOT EXISTS sales_medallion.gold_sales_by_region_item (
    region TEXT,
    item_type TEXT,
    total_revenue DECIMAL,
    total_profit DECIMAL,
    order_count INT,
    avg_profit_margin DECIMAL,
    PRIMARY KEY ((region), item_type)
);

-- Customer Order Performance
CREATE TABLE IF NOT EXISTS sales_medallion.gold_customer_performance (
    country TEXT,
    sales_channel TEXT,
    total_revenue DECIMAL,
    total_profit DECIMAL,
    avg_order_value DECIMAL,
    order_count INT,
    PRIMARY KEY ((country), sales_channel)
);

-- Shipping Efficiency
CREATE TABLE IF NOT EXISTS sales_medallion.gold_shipping_efficiency (
    order_priority TEXT,
    avg_days_to_ship DECIMAL,
    min_days_to_ship INT,
    max_days_to_ship INT,
    order_count INT,
    PRIMARY KEY (order_priority)
);