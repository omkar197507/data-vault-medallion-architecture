#!/usr/bin/env python3
"""
Sample Data Generation Script for Data Vault Medallion Architecture
Generates realistic sample data for customers, products, orders, and order items
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import argparse
import os

# Initialize faker for realistic data
fake = Faker()

def generate_customers(num_records=1000):
    """Generate sample customer data"""
    print(f"Generating {num_records} customer records...")
    
    customers = []
    for i in range(1, num_records + 1):
        created_date = fake.date_between(start_date='-2y', end_date='-1y')
        updated_date = fake.date_between(start_date=created_date, end_date='today')
        
        customers.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': f"555-{random.randint(1000, 9999)}",
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'created_date': created_date.strftime('%Y-%m-%d'),
            'updated_date': updated_date.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(customers)
    df.to_csv('../sample-data/customers.csv', index=False)
    print(f"✓ Generated {len(customers)} customer records")
    return df

def generate_products(num_records=100):
    """Generate sample product data"""
    print(f"Generating {num_records} product records...")
    
    categories = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes'],
        'Home': ['Furniture', 'Decor', 'Kitchenware', 'Bedding', 'Lighting'],
        'Books': ['Fiction', 'Non-Fiction', 'Textbook', 'Children', 'Cookbook'],
        'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories', 'Nutrition']
    }
    
    products = []
    for i in range(1, num_records + 1):
        category = random.choice(list(categories.keys()))
        product_name = random.choice(categories[category])
        full_product_name = f"{product_name} {fake.word().capitalize()}"
        
        base_price = random.randint(20, 2000)
        cost = round(base_price * random.uniform(0.4, 0.7), 2)
        price = round(base_price * random.uniform(1.2, 2.0), 2)
        
        created_date = fake.date_between(start_date='-3y', end_date='-6m')
        updated_date = fake.date_between(start_date=created_date, end_date='today')
        
        products.append({
            'product_id': 1000 + i,
            'product_name': full_product_name,
            'category': category,
            'price': price,
            'cost': cost,
            'created_date': created_date.strftime('%Y-%m-%d'),
            'updated_date': updated_date.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(products)
    df.to_csv('../sample-data/products.csv', index=False)
    print(f"✓ Generated {len(products)} product records")
    return df

def generate_orders(num_records=5000, customers_df=None):
    """Generate sample order data"""
    print(f"Generating {num_records} order records...")
    
    if customers_df is None:
        customers_df = pd.read_csv('../sample-data/customers.csv')
    
    customer_ids = customers_df['customer_id'].tolist()
    statuses = ['COMPLETED', 'PENDING', 'CANCELLED', 'PROCESSING']
    status_weights = [0.7, 0.1, 0.1, 0.1]  # Weighted probabilities
    
    orders = []
    for i in range(1, num_records + 1):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        status = random.choices(statuses, weights=status_weights, k=1)[0]
        
        # Base order amount
        total_amount = round(random.uniform(20, 2000), 2)
        
        orders.append({
            'order_id': 10000 + i,
            'customer_id': customer_id,
            'order_date': order_date.strftime('%Y-%m-%d'),
            'total_amount': total_amount,
            'status': status,
            'created_date': order_date.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(orders)
    df.to_csv('../sample-data/orders.csv', index=False)
    print(f"✓ Generated {len(orders)} order records")
    return df

def generate_order_items(orders_df=None, products_df=None):
    """Generate sample order items data"""
    print("Generating order items records...")
    
    if orders_df is None:
        orders_df = pd.read_csv('../sample-data/orders.csv')
    if products_df is None:
        products_df = pd.read_csv('../sample-data/products.csv')
    
    order_items = []
    item_id = 1
    
    for _, order in orders_df.iterrows():
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        order_products = products_df.sample(n=num_items)
        
        for _, product in order_products.iterrows():
            quantity = random.randint(1, 3)
            unit_price = product['price']
            total_price = round(quantity * unit_price, 2)
            
            order_items.append({
                'order_item_id': item_id,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price,
                'created_date': order['order_date']
            })
            item_id += 1
    
    df = pd.DataFrame(order_items)
    df.to_csv('../sample-data/order_items.csv', index=False)
    print(f"✓ Generated {len(order_items)} order item records")
    return df

def generate_all_data():
    """Generate all sample data"""
    print("Starting sample data generation...")
    print("=" * 50)
    
    # Create sample-data directory if it doesn't exist
    os.makedirs('../sample-data', exist_ok=True)
    
    # Generate data in sequence
    customers_df = generate_customers(1000)
    products_df = generate_products(100)
    orders_df = generate_orders(5000, customers_df)
    order_items_df = generate_order_items(orders_df, products_df)
    
    # Generate data summary
    print("\n" + "=" * 50)
    print("DATA GENERATION SUMMARY:")
    print("=" * 50)
    print(f"Customers: {len(customers_df)} records")
    print(f"Products: {len(products_df)} records")
    print(f"Orders: {len(orders_df)} records")
    print(f"Order Items: {len(order_items_df)} records")
    print(f"Total Records: {len(customers_df) + len(products_df) + len(orders_df) + len(order_items_df)}")
    print("=" * 50)
    
    return customers_df, products_df, orders_df, order_items_df

def validate_data_relationships():
    """Validate data relationships and quality"""
    print("\nValidating data relationships...")
    
    # Load generated data
    customers = pd.read_csv('../sample-data/customers.csv')
    products = pd.read_csv('../sample-data/products.csv')
    orders = pd.read_csv('../sample-data/orders.csv')
    order_items = pd.read_csv('../sample-data/order_items.csv')
    
    # Check foreign key relationships
    orders_with_valid_customers = orders[orders['customer_id'].isin(customers['customer_id'])]
    order_items_with_valid_orders = order_items[order_items['order_id'].isin(orders['order_id'])]
    order_items_with_valid_products = order_items[order_items['product_id'].isin(products['product_id'])]
    
    print(f"✓ Orders with valid customers: {len(orders_with_valid_customers)}/{len(orders)}")
    print(f"✓ Order items with valid orders: {len(order_items_with_valid_orders)}/{len(order_items)}")
    print(f"✓ Order items with valid products: {len(order_items_with_valid_products)}/{len(order_items)}")
    
    # Check for duplicates
    print(f"✓ Unique customers: {customers['customer_id'].nunique()}")
    print(f"✓ Unique products: {products['product_id'].nunique()}")
    print(f"✓ Unique orders: {orders['order_id'].nunique()}")
    
    # Basic data quality checks
    print(f"✓ Customers without null values: {not customers.isnull().any().any()}")
    print(f"✓ Products without null values: {not products.isnull().any().any()}")
    print(f"✓ Orders without null values: {not orders.isnull().any().any()}")
    print(f"✓ Order items without null values: {not order_items.isnull().any().any()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sample data for Data Vault project')
    parser.add_argument('--customers', type=int, default=1000, help='Number of customers to generate')
    parser.add_argument('--products', type=int, default=100, help='Number of products to generate')
    parser.add_argument('--orders', type=int, default=5000, help='Number of orders to generate')
    parser.add_argument('--validate', action='store_true', help='Run data validation after generation')
    
    args = parser.parse_args()
    
    try:
        generate_all_data()
        
        if args.validate:
            validate_data_relationships()
            
        print("\n🎉 Sample data generation completed successfully!")
        print("Files saved to sample-data/ directory")
        
    except Exception as e:
        print(f"❌ Error generating data: {e}")
        raise