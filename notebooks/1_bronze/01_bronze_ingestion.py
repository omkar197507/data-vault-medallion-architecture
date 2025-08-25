# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC ## Objectives:
# MAGIC 1. ✅ Ingest raw data from sample-data container
# MAGIC 2. ✅ Add metadata columns for auditability
# MAGIC 3. ✅ Store in Delta format in bronze layer
# MAGIC 4. ✅ Validate data quality

# COMMAND ----------




# Paths
sample_data_path = f"wasbs://sample-data@{storage_account}.blob.core.windows.net/"
bronze_path = f"wasbs://bronze@{storage_account}.blob.core.windows.net/"

print("Bronze Layer Configuration Ready!")
print(f"Sample Data: {sample_data_path}")
print(f"Bronze Layer: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Utility Functions for Data Ingestion

# COMMAND ----------

from pyspark.sql.functions import *

def read_csv_from_source(file_name):
    """Read CSV file from sample-data container"""
    try:
        file_path = f"{sample_data_path}{file_name}"
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        print(f"✓ Read {file_name}: {df.count()} rows")
        return df
        
    except Exception as e:
        print(f"✗ Error reading {file_name}: {e}")
        return None

def write_delta_to_bronze(df, table_name):
    """Write DataFrame to bronze layer in Delta format"""
    try:
        output_path = f"{bronze_path}{table_name}"
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        print(f"✓ Written to bronze: {table_name}")
        return True
        
    except Exception as e:
        print(f"✗ Error writing {table_name}: {e}")
        return False

def add_metadata_columns(df, source_file):
    """Add metadata columns for data lineage"""
    return df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("sample_data")) \
        .withColumn("source_file", lit(source_file)) \
        .withColumn("load_id", lit(f"load_{current_timestamp().cast('string')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Customers Data

# COMMAND ----------

def ingest_customers():
    """Ingest customers data to bronze layer"""
    print("Ingesting customers data...")
    print("-" * 40)
    
    # Read from source
    customers_df = read_csv_from_source("customers.csv")
    if customers_df is None:
        return False
    
    # Add metadata
    customers_bronze = add_metadata_columns(customers_df, "customers.csv")
    
    # Write to bronze
    success = write_delta_to_bronze(customers_bronze, "customers")
    
    if success:
        print("✅ Customers ingestion completed!")
        customers_bronze.show(2)
        return True
    else:
        return False

# Execute customers ingestion
ingest_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Products Data

# COMMAND ----------

def ingest_products():
    """Ingest products data to bronze layer"""
    print("Ingesting products data...")
    print("-" * 40)
    
    # Read from source
    products_df = read_csv_from_source("products.csv")
    if products_df is None:
        return False
    
    # Add metadata
    products_bronze = add_metadata_columns(products_df, "products.csv")
    
    # Write to bronze
    success = write_delta_to_bronze(products_bronze, "products")
    
    if success:
        print("✅ Products ingestion completed!")
        products_bronze.show(2)
        return True
    else:
        return False

# Execute products ingestion
ingest_products()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Orders Data

# COMMAND ----------

def ingest_orders():
    """Ingest orders data to bronze layer"""
    print("Ingesting orders data...")
    print("-" * 40)
    
    # Read from source
    orders_df = read_csv_from_source("orders.csv")
    if orders_df is None:
        return False
    
    # Add metadata
    orders_bronze = add_metadata_columns(orders_df, "orders.csv")
    
    # Write to bronze
    success = write_delta_to_bronze(orders_bronze, "orders")
    
    if success:
        print("✅ Orders ingestion completed!")
        orders_bronze.show(2)
        return True
    else:
        return False

# Execute orders ingestion
ingest_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest Order Items Data

# COMMAND ----------

def ingest_order_items():
    """Ingest order items data to bronze layer"""
    print("Ingesting order items data...")
    print("-" * 40)
    
    # Read from source
    order_items_df = read_csv_from_source("order_items.csv")
    if order_items_df is None:
        return False
    
    # Add metadata
    order_items_bronze = add_metadata_columns(order_items_df, "order_items.csv")
    
    # Write to bronze
    success = write_delta_to_bronze(order_items_bronze, "order_items")
    
    if success:
        print("✅ Order items ingestion completed!")
        order_items_bronze.show(2)
        return True
    else:
        return False

# Execute order items ingestion
ingest_order_items()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingest All Tables

# COMMAND ----------

def ingest_all_tables():
    """Ingest all tables to bronze layer"""
    print("Starting ingestion of all tables...")
    print("=" * 50)
    
    ingestion_functions = [
        ingest_customers,
        ingest_products, 
        ingest_orders,
        ingest_order_items
    ]
    
    all_success = True
    
    for ingest_func in ingestion_functions:
        success = ingest_func()
        if not success:
            all_success = False
        print("\n")
    
    if all_success:
        print("🎉 ALL TABLES INGESTED SUCCESSFULLY!")
    else:
        print("❌ Some tables failed ingestion.")
    
    return all_success

# Execute all ingestions
ingest_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Bronze Layer Data

# COMMAND ----------

def verify_bronze_layer():
    """Verify data in bronze layer"""
    print("Verifying bronze layer data...")
    print("=" * 50)
    
    tables = ["customers", "products", "orders", "order_items"]
    
    for table in tables:
        try:
            table_path = f"{bronze_path}{table}"
            df = spark.read.format("delta").load(table_path)
            
            print(f"✓ {table}: {df.count()} rows, {len(df.columns)} columns")
            print(f"   Metadata columns: {[col for col in df.columns if col in ['ingestion_timestamp', 'source_system', 'source_file', 'load_id']]}")
            df.select("ingestion_timestamp", "source_system", "source_file").show(1, truncate=False)
            
        except Exception as e:
            print(f"✗ {table}: Error - {e}")
    
    print("=" * 50)

# Verify bronze data
verify_bronze_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Quality Checks

# COMMAND ----------

def perform_data_quality_checks():
    """Perform basic data quality checks"""
    print("Performing data quality checks...")
    print("=" * 50)
    
    tables = {
        "customers": ["customer_id", "email"],
        "products": ["product_id", "price"], 
        "orders": ["order_id", "customer_id"],
        "order_items": ["order_item_id", "order_id", "product_id"]
    }
    
    for table, key_columns in tables.items():
        try:
            table_path = f"{bronze_path}{table}"
            df = spark.read.format("delta").load(table_path)
            
            print(f"\n📊 {table.upper()} Data Quality:")
            print(f"   Total rows: {df.count()}")
            
            # Check for nulls in key columns
            for col in key_columns:
                null_count = df.filter(col(col).isNull()).count()
                print(f"   Null values in {col}: {null_count}")
            
            # Check for duplicates
            duplicate_count = df.groupBy(key_columns).count().filter("count > 1").count()
            print(f"   Duplicate records: {duplicate_count}")
            
        except Exception as e:
            print(f"✗ Error checking {table}: {e}")
    
    print("=" * 50)

# Run data quality checks
perform_data_quality_checks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Bronze Layer Summary

# COMMAND ----------

def bronze_layer_summary():
    """Generate summary of bronze layer"""
    print("BRONZE LAYER SUMMARY")
    print("=" * 60)
    
    tables = ["customers", "products", "orders", "order_items"]
    total_rows = 0
    
    for table in tables:
        try:
            table_path = f"{bronze_path}{table}"
            df = spark.read.format("delta").load(table_path)
            row_count = df.count()
            total_rows += row_count
            print(f"📦 {table.upper():<12}: {row_count:>4} rows")
            
        except Exception as e:
            print(f"✗ {table}: Error - {e}")
    
    print("-" * 60)
    print(f"📊 TOTAL RECORDS: {total_rows} rows")
    print("=" * 60)
    print("✅ BRONZE LAYER IMPLEMENTATION COMPLETE!")
    print("📋 Next step: Silver Layer (Data Vault Modeling)")

# Generate summary
bronze_layer_summary()


