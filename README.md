# Cassandra Medallion Architecture Project

This project demonstrates building a Medallion Architecture using Apache Cassandra and Python.


## Architecture Layers

- **Bronze**: Raw JSON data from CSV.
- **Silver**: Structured data.
- **Gold**:
  - Sales by Region and Item type 
  - Customer Performance
  - Shipping Efficiency