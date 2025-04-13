from astrapy import DataAPIClient
from config import ASTRA_DB_TOKEN, ASTRA_DB_API_ENDPOINT, KEYSPACE
import pandas as pd
import numpy as np

# Initialize the client
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

# Create gold collections 
for collection in ["gold_sales_by_region_item", "gold_customer_performance", "gold_shipping_efficiency"]:
    try:
        db.create_collection(collection)
        print(f"Created {collection} collection")
    except Exception as e:
        print(f"Collection {collection} may already exist: {str(e)}")

# Get all valid silver sales records
silver_sales = list(db.silver_sales.find({"is_valid": True}))

if not silver_sales:
    print("No valid records found in silver_sales collection")
    exit()

# Convert to DataFrame for easier processing
df_silver = pd.DataFrame(silver_sales)

# Helper function to safely convert values
def safe_convert(value):
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    if isinstance(value, (np.float64, np.float32)):
        return float(value)
    return value

## 1. Sales by Region and Item Type
region_item_groups = df_silver.groupby(['region', 'item_type'])
for (region, item_type), group in region_item_groups:
    total_revenue = safe_convert(group['total_revenue'].sum())
    total_profit = safe_convert(group['total_profit'].sum())
    avg_profit_margin = safe_convert(group['profit_margin'].mean())
    order_count = safe_convert(len(group))
    
    db.gold_sales_by_region_item.insert_one({
        "region": region,
        "item_type": item_type,
        "total_revenue": total_revenue,
        "total_profit": total_profit,
        "order_count": order_count,
        "avg_profit_margin": avg_profit_margin
    })

## 2. Customer Order Performance
customer_groups = df_silver.groupby(['country', 'sales_channel'])
for (country, channel), group in customer_groups:
    total_revenue = safe_convert(group['total_revenue'].sum())
    total_profit = safe_convert(group['total_profit'].sum())
    avg_order_value = safe_convert(group['total_revenue'].mean())
    order_count = safe_convert(len(group))
    
    db.gold_customer_performance.insert_one({
        "country": country,
        "sales_channel": channel,
        "total_revenue": total_revenue,
        "total_profit": total_profit,
        "avg_order_value": avg_order_value,
        "order_count": order_count
    })

## 3. Shipping Efficiency
# Filter out records with no shipping data
shipping_data = df_silver[df_silver['days_to_ship'].notna()]
shipping_groups = shipping_data.groupby('order_priority')

for priority, group in shipping_groups:
    avg_days = safe_convert(group['days_to_ship'].mean())
    min_days = safe_convert(group['days_to_ship'].min())
    max_days = safe_convert(group['days_to_ship'].max())
    order_count = safe_convert(len(group))
    
    db.gold_shipping_efficiency.insert_one({
        "order_priority": priority,
        "avg_days_to_ship": avg_days,
        "min_days_to_ship": min_days,
        "max_days_to_ship": max_days,
        "order_count": order_count
    })

print("Gold layer data marts created successfully!")