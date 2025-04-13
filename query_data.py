from astrapy import DataAPIClient
from config import ASTRA_DB_TOKEN, ASTRA_DB_API_ENDPOINT, KEYSPACE
from pprint import pprint

client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

print("Gold Sales by Region and Item Type:")
for doc in db.gold_sales_by_region_item.find().limit(5):
    pprint(doc)

print("\nGold Customer Performance:")
for doc in db.gold_customer_performance.find().limit(5):
    pprint(doc)

print("\nGold Shipping Efficiency:")
for doc in db.gold_shipping_efficiency.find().limit(5):
    pprint(doc)