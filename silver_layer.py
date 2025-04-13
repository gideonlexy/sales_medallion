from astrapy import DataAPIClient
from config import ASTRA_DB_TOKEN, ASTRA_DB_API_ENDPOINT, KEYSPACE
from datetime import datetime

# Initialize the client
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

# Create silver collection 
try:
    db.create_collection("silver_sales")
    print("Created silver_sales collection")
except Exception as e:
    print(f"Collection may already exist: {str(e)}")

# Function to parse date
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').date().isoformat()
    except:
        return None

# Process data from bronze to silver
bronze_docs = db.bronze_sales.find({})
for doc in bronze_docs:
    try:
        order_date = parse_date(doc["order_date"])
        ship_date = parse_date(doc["ship_date"])
        days_to_ship = None
        
        if order_date and ship_date:
            order_dt = datetime.fromisoformat(order_date)
            ship_dt = datetime.fromisoformat(ship_date)
            days_to_ship = (ship_dt - order_dt).days
            
        total_revenue = float(doc["total_revenue"])
        total_cost = float(doc["total_cost"])
        profit_margin = (total_revenue - total_cost) / total_revenue if total_revenue else 0
        
        silver_doc = {
            "order_id": doc["order_id"],
            "region": doc["region"],
            "country": doc["country"],
            "item_type": doc["item_type"],
            "sales_channel": doc["sales_channel"],
            "order_priority": doc["order_priority"],
            "order_date": order_date,
            "ship_date": ship_date,
            "days_to_ship": days_to_ship,
            "units_sold": doc["units_sold"],
            "unit_price": doc["unit_price"],
            "unit_cost": doc["unit_cost"],
            "total_revenue": total_revenue,
            "total_cost": total_cost,
            "total_profit": doc["total_profit"],
            "profit_margin": profit_margin,
            "is_valid": True,
            "processing_timestamp": datetime.utcnow().isoformat()
        }
        
        db.silver_sales.insert_one(silver_doc)
    except Exception as e:
        print(f"Error processing document {doc['order_id']}: {str(e)}")

print("Silver layer data processed successfully!")