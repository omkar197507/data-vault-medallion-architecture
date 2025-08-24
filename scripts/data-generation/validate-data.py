#!/usr/bin/env python3
"""
Data Validation Script for Sample Data
Validates data quality and relationships
"""

import pandas as pd
import numpy as np

def validate_sample_data():
    """Validate all sample data files"""
    print("Validating sample data...")
    print("=" * 50)
    
    try:
        # Load all data files
        customers = pd.read_csv('../sample-data/customers.csv')
        products = pd.read_csv('../sample-data/products.csv')
        orders = pd.read_csv('../sample-data/orders.csv')
        order_items = pd.read_csv('../sample-data/order_items.csv')
        
        # Basic validation
        print("📊 Data Volume:")
        print(f"   Customers: {len(customers):,} records")
        print(f"   Products: {len(products):,} records")
        print(f"   Orders: {len(orders):,} records")
        print(f"   Order Items: {len(order_items):,} records")
        
        # Data quality checks
        print("\n✅ Data Quality Checks:")
        
        # Check for null values
        for name, df in [('Customers', customers), ('Products', products), 
                        ('Orders', orders), ('Order Items', order_items)]:
            null_count = df.isnull().sum().sum()
            print(f"   {name}: {'No null values' if null_count == 0 else f'{null_count} null values'}")
        
        # Check relationships
        print("\n🔗 Relationship Validation:")
        
        # Orders should have valid customers
        valid_orders = orders[orders['customer_id'].isin(customers['customer_id'])]
        print(f"   Orders with valid customers: {len(valid_orders):,}/{len(orders):,}")
        
        # Order items should have valid orders and products
        valid_order_items_orders = order_items[order_items['order_id'].isin(orders['order_id'])]
        valid_order_items_products = order_items[order_items['product_id'].isin(products['product_id'])]
        print(f"   Order items with valid orders: {len(valid_order_items_orders):,}/{len(order_items):,}")
        print(f"   Order items with valid products: {len(valid_order_items_products):,}/{len(order_items):,}")
        
        # Check data ranges
        print("\n📈 Data Range Validation:")
        print(f"   Customer IDs: {customers['customer_id'].min()} - {customers['customer_id'].max()}")
        print(f"   Product IDs: {products['product_id'].min()} - {products['product_id'].max()}")
        print(f"   Order IDs: {orders['order_id'].min()} - {orders['order_id'].max()}")
        
        # Check order amounts
        print(f"   Order amounts: ${orders['total_amount'].min():.2f} - ${orders['total_amount'].max():.2f}")
        print(f"   Product prices: ${products['price'].min():.2f} - ${products['price'].max():.2f}")
        
        # Check date ranges
        print(f"   Order dates: {orders['order_date'].min()} to {orders['order_date'].max()}")
        
        print("\n🎉 All validation checks completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        return False

if __name__ == "__main__":
    validate_sample_data()