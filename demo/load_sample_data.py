#!/usr/bin/env python3
"""
Load sample data into PostgreSQL, MinIO, and Kafka for Sparkle demo.

This script generates realistic synthetic data and loads it into the demo environment.
"""

import os
import sys
import time
import json
import psycopg2
from psycopg2.extras import execute_batch

# Add parent directory to path
sys.path.insert(0, '/opt/sparkle')

from demo.data_generators import (
    generate_customers,
    generate_orders,
    generate_returns,
    generate_salesforce_export,
    generate_ga4_export,
    generate_mainframe_copybook_data,
)


def wait_for_postgres(host, port, database, user, password, max_retries=30):
    """Wait for PostgreSQL to be ready"""
    print(f"Waiting for PostgreSQL at {host}:{port}...")

    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            conn.close()
            print("✓ PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError:
            print(f"  Retry {i+1}/{max_retries}...")
            time.sleep(2)

    print("✗ PostgreSQL not ready after maximum retries")
    return False


def load_customers_to_postgres(conn, num_customers=50000):
    """Load customers into PostgreSQL"""
    print(f"\nGenerating {num_customers} customers...")
    customers = generate_customers(num_customers)

    print("Loading customers into PostgreSQL...")
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO customers (
        name, email, phone, address, city, state, zip_code, country,
        signup_date, last_activity_date, tier, is_active,
        lifetime_value, total_orders, avg_order_value
    ) VALUES (
        %(name)s, %(email)s, %(phone)s, %(address)s, %(city)s, %(state)s,
        %(zip_code)s, %(country)s, %(signup_date)s, %(last_activity_date)s,
        %(tier)s, %(is_active)s, %(lifetime_value)s, %(total_orders)s,
        %(avg_order_value)s
    )
    """

    execute_batch(cursor, insert_query, customers, page_size=1000)
    conn.commit()

    print(f"✓ Loaded {len(customers)} customers")


def load_orders_to_postgres(conn, num_orders=150000, num_customers=50000):
    """Load orders into PostgreSQL"""
    print(f"\nGenerating {num_orders} orders...")
    orders = generate_orders(num_orders, num_customers)

    print("Loading orders into PostgreSQL...")
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO orders (
        customer_id, order_date, order_amount, tax_amount,
        shipping_amount, total_amount, status, payment_method, items_count
    ) VALUES (
        %(customer_id)s, %(order_date)s, %(order_amount)s, %(tax_amount)s,
        %(shipping_amount)s, %(total_amount)s, %(status)s, %(payment_method)s,
        %(items_count)s
    )
    """

    execute_batch(cursor, insert_query, orders, page_size=1000)
    conn.commit()

    print(f"✓ Loaded {len(orders)} orders")


def load_returns_to_postgres(conn, num_returns=5000, num_orders=150000):
    """Load returns into PostgreSQL"""
    print(f"\nGenerating {num_returns} returns...")
    returns = generate_returns(num_returns, num_orders)

    print("Loading returns into PostgreSQL...")
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO returns (
        order_id, return_date, return_amount, reason, status, refund_issued
    ) VALUES (
        %(order_id)s, %(return_date)s, %(return_amount)s, %(reason)s,
        %(status)s, %(refund_issued)s
    )
    """

    execute_batch(cursor, insert_query, returns, page_size=1000)
    conn.commit()

    print(f"✓ Loaded {len(returns)} returns")


def main():
    """Main function to load all sample data"""
    print("="*60)
    print("Sparkle Demo - Sample Data Loader")
    print("="*60)

    # Get configuration from environment
    postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
    postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
    postgres_db = os.getenv('POSTGRES_DB', 'sparkle_crm')
    postgres_user = os.getenv('POSTGRES_USER', 'sparkle')
    postgres_password = os.getenv('POSTGRES_PASSWORD', 'sparkle123')

    # Wait for PostgreSQL
    if not wait_for_postgres(
        postgres_host, postgres_port, postgres_db,
        postgres_user, postgres_password
    ):
        sys.exit(1)

    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )

    try:
        # Load data
        load_customers_to_postgres(conn, num_customers=50000)
        load_orders_to_postgres(conn, num_orders=150000, num_customers=50000)
        load_returns_to_postgres(conn, num_returns=5000, num_orders=150000)

        # Get final counts
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM returns")
        return_count = cursor.fetchone()[0]

        print("\n" + "="*60)
        print("✓ Sample data loaded successfully!")
        print("="*60)
        print(f"  Customers: {customer_count:,}")
        print(f"  Orders:    {order_count:,}")
        print(f"  Returns:   {return_count:,}")
        print("="*60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
