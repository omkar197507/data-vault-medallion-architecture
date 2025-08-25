# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Silver Layer - Data Vault Implementation
# MAGIC 
# MAGIC ## Data Vault 2.0 Modeling:
# MAGIC - **Hubs**: Business Keys
# MAGIC - **Links**: Relationships 
# MAGIC - **Satellites**: Descriptive Attributes with History

# COMMAND ----------



# Paths
bronze_path = f"wasbs://bronze@{storage_account}.blob.core.windows.net/"
silver_path = f"wasbs://silver@{storage_account}.blob.core.windows.net/"

print("Data Vault Configuration Ready!")
print(f"Bronze Layer: {bronze_path}")
print(f"Silver Layer: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Layer Data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import hashlib

def load_bronze_table(table_name):
    """Load table from bronze layer"""
    try:
        table_path = f"{bronze_path}{table_name}"
        df = spark.read.format("delta").load(table_path)
        print(f"✓ Loaded {table_name}: {df.count()} rows")
        return df
    except Exception as e:
        print(f"✗ Error loading {table_name}: {e}")
        return None

# Load all bronze tables
customers_bronze = load_bronze_table("customers")
products_bronze = load_bronze_table("products")
orders_bronze = load_bronze_table("orders")
order_items_bronze = load_bronze_table("order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utility Functions for Data Vault

# COMMAND ----------

def generate_hash_key(business_key):
    """Generate hash key for business keys"""
    if business_key is None:
        return None
    return hashlib.sha256(str(business_key).encode()).hexdigest()

# Register as UDF
generate_hash_key_udf = udf(generate_hash_key, StringType())

def create_hub_table(df, business_key_column, hub_name):
    """Create Hub table from business key"""
    print(f"Creating Hub: {hub_name}")
    
    # Select distinct business keys
    hub_df = df.select(business_key_column).distinct()
    
    # Add hash key and metadata
    hub_final = hub_df \
        .withColumn("hash_key", generate_hash_key_udf(col(business_key_column))) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("record_source", lit("bronze_layer")) \
        .withColumnRenamed(business_key_column, f"{hub_name}_key")
    
    print(f"✓ Hub {hub_name}: {hub_final.count()} business keys")
    return hub_final

def write_delta_table(df, table_name, layer_path):
    """Write DataFrame to Delta table"""
    try:
        output_path = f"{layer_path}{table_name}"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        print(f"✓ Written: {table_name}")
        return True
    except Exception as e:
        print(f"✗ Error writing {table_name}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Hub Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Hub Customer

# COMMAND ----------

# Create Hub Customer
hub_customer = create_hub_table(customers_bronze, "customer_id", "customer")
hub_customer.show(5)

# Write to silver layer
write_delta_table(hub_customer, "hub_customer", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Hub Product

# COMMAND ----------

# Create Hub Product
hub_product = create_hub_table(products_bronze, "product_id", "product")
hub_product.show(5)

# Write to silver layer
write_delta_table(hub_product, "hub_product", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Hub Order

# COMMAND ----------

# Create Hub Order
hub_order = create_hub_table(orders_bronze, "order_id", "order")
hub_order.show(5)

# Write to silver layer
write_delta_table(hub_order, "hub_order", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Link Tables

# COMMAND ----------

def create_link_table(df, hub_keys, link_name):
    """Create Link table for relationships"""
    print(f"Creating Link: {link_name}")
    
    # Select relationship keys
    link_df = df.select(*hub_keys).distinct()
    
    # Generate hash for the relationship
    combined_key = concat(*[col(key).cast("string") for key in hub_keys])
    link_final = link_df \
        .withColumn("hash_key", generate_hash_key_udf(combined_key)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("record_source", lit("bronze_layer"))
    
    print(f"✓ Link {link_name}: {link_final.count()} relationships")
    return link_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Link Customer Order

# COMMAND ----------

# Create Link Customer Order
link_customer_order = create_link_table(
    orders_bronze, 
    ["customer_id", "order_id"], 
    "customer_order"
)
link_customer_order.show(5)

write_delta_table(link_customer_order, "link_customer_order", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Link Order Product

# COMMAND ----------

# Create Link Order Product
link_order_product = create_link_table(
    order_items_bronze,
    ["order_id", "product_id"],
    "order_product"
)
link_order_product.show(5)

write_delta_table(link_order_product, "link_order_product", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Satellite Tables

# COMMAND ----------

def create_satellite_table(df, business_key_column, satellite_name, descriptive_columns):
    """Create Satellite table with descriptive attributes"""
    print(f"Creating Satellite: {satellite_name}")
    
    # Select descriptive columns
    sat_df = df.select(business_key_column, *descriptive_columns)
    
    # Add hash key and metadata
    sat_final = sat_df \
        .withColumn("hash_key", generate_hash_key_udf(col(business_key_column))) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("record_source", lit("bronze_layer")) \
        .withColumn("effective_date", current_timestamp())
    
    print(f"✓ Satellite {satellite_name}: {sat_final.count()} records")
    return sat_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Satellite Customer Details

# COMMAND ----------

# Satellite Customer Details
sat_customer_details = create_satellite_table(
    customers_bronze,
    "customer_id",
    "customer_details",
    ["first_name", "last_name", "email", "phone", "address", "city", "state", "zip_code", "created_date", "updated_date"]
)
sat_customer_details.show(5)

write_delta_table(sat_customer_details, "sat_customer_details", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Satellite Product Details

# COMMAND ----------

# Satellite Product Details
sat_product_details = create_satellite_table(
    products_bronze,
    "product_id",
    "product_details",
    ["product_name", "category", "price", "cost", "created_date", "updated_date"]
)
sat_product_details.show(5)

write_delta_table(sat_product_details, "sat_product_details", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Satellite Order Details

# COMMAND ----------

# Satellite Order Details
sat_order_details = create_satellite_table(
    orders_bronze,
    "order_id",
    "order_details",
    ["customer_id", "order_date", "total_amount", "status", "created_date"]
)
sat_order_details.show(5)

write_delta_table(sat_order_details, "sat_order_details", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Data Vault Model

# COMMAND ----------

def verify_data_vault_model():
    """Verify the complete Data Vault model"""
    print("Verifying Data Vault Model...")
    print("=" * 50)
    
    # List all Data Vault tables
    dv_tables = [
        "hub_customer", "hub_product", "hub_order",
        "link_customer_order", "link_order_product",
        "sat_customer_details", "sat_product_details", "sat_order_details"
    ]
    
    for table in dv_tables:
        try:
            table_path = f"{silver_path}{table}"
            df = spark.read.format("delta").load(table_path)
            print(f"✓ {table}: {df.count()} rows")
            df.show(1)
        except Exception as e:
            print(f"✗ {table}: Error - {e}")
    
    print("=" * 50)

# Verify Data Vault model
verify_data_vault_model()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Vault Relationships Analysis

# COMMAND ----------

def analyze_dv_relationships():
    """Analyze relationships in Data Vault model"""
    print("Analyzing Data Vault Relationships...")
    print("=" * 50)
    
    # Load Data Vault tables
    hub_customer_df = spark.read.format("delta").load(f"{silver_path}hub_customer")
    hub_order_df = spark.read.format("delta").load(f"{silver_path}hub_order")
    link_customer_order_df = spark.read.format("delta").load(f"{silver_path}link_customer_order")
    
    # Analyze relationships
    print("Customer-Order Relationships:")
    customer_order_analysis = link_customer_order_df \
        .groupBy("customer_id") \
        .agg(count("order_id").alias("order_count")) \
        .orderBy(desc("order_count"))
    
    customer_order_analysis.show(10)
    
    print(f"Total unique customers with orders: {customer_order_analysis.count()}")
    print(f"Average orders per customer: {customer_order_analysis.agg(avg('order_count')).collect()[0][0]:.2f}")
    
    print("=" * 50)

# Analyze relationships
analyze_dv_relationships()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Vault Summary

# COMMAND ----------

def data_vault_summary():
    """Generate Data Vault implementation summary"""
    print("DATA VAULT IMPLEMENTATION SUMMARY")
    print("=" * 60)
    
    # Hub tables
    hub_tables = ["hub_customer", "hub_product", "hub_order"]
    hub_total = 0
    for table in hub_tables:
        df = spark.read.format("delta").load(f"{silver_path}{table}")
        count = df.count()
        hub_total += count
        print(f"🔑 {table.upper():<16}: {count:>4} business keys")
    
    # Link tables  
    link_tables = ["link_customer_order", "link_order_product"]
    link_total = 0
    for table in link_tables:
        df = spark.read.format("delta").load(f"{silver_path}{table}")
        count = df.count()
        link_total += count
        print(f"🔗 {table.upper():<16}: {count:>4} relationships")
    
    # Satellite tables
    sat_tables = ["sat_customer_details", "sat_product_details", "sat_order_details"]
    sat_total = 0
    for table in sat_tables:
        df = spark.read.format("delta").load(f"{silver_path}{table}")
        count = df.count()
        sat_total += count
        print(f"📊 {table.upper():<16}: {count:>4} attribute records")
    
    print("-" * 60)
    print(f"📦 TOTAL HUBS:      {hub_total} business keys")
    print(f"🔗 TOTAL LINKS:      {link_total} relationships") 
    print(f"📊 TOTAL SATELLITES: {sat_total} attribute records")
    print("=" * 60)
    print("✅ DATA VAULT IMPLEMENTATION COMPLETE!")
    print("📋 Next step: Gold Layer - Dimensional Modeling")

# Generate summary
data_vault_summary()
