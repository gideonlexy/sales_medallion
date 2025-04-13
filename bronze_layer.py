from astrapy import DataAPIClient
from config import ASTRA_DB_TOKEN, ASTRA_DB_API_ENDPOINT, KEYSPACE
import pandas as pd
from datetime import datetime

# Initialize the client
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

# Create bronze collection 
try:
    db.create_collection("bronze_sales")
    print("Created bronze_sales collection")
except Exception as e:
    print(f"Collection may already exist: {str(e)}")

# Load CSV data
df = pd.read_csv('sales_100.csv')

# Insert data into bronze collection
for _, row in df.iterrows():
    document = {
        "order_id": str(row['Order ID']),
        "region": row['Region'],
        "country": row['Country'],
        "item_type": row['Item Type'],
        "sales_channel": row['Sales Channel'],
        "order_priority": row['Order Priority'],
        "order_date": row['Order Date'],
        "ship_date": row['Ship Date'],
        "units_sold": int(row['UnitsSold']),
        "unit_price": float(row['UnitPrice']),
        "unit_cost": float(row['UnitCost']),
        "total_revenue": float(row['TotalRevenue']),
        "total_cost": float(row['TotalCost']),
        "total_profit": float(row['TotalProfit']),
        "raw_data": str(row.to_dict()),
        "ingestion_timestamp": datetime.utcnow().isoformat()
    }
    db.bronze_sales.insert_one(document)

print("Bronze layer data loaded successfully!")