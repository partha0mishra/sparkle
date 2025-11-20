#!/usr/bin/env python3
"""
Kafka event generator for Sparkle demo.

Generates realistic streaming customer events (clickstream, purchases, etc.)
"""

import os
import sys
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def create_customer_event():
    """Generate a realistic customer event"""
    event_types = [
        "page_view", "product_view", "add_to_cart",
        "remove_from_cart", "checkout_start", "checkout_complete",
        "search", "filter_applied", "wishlist_add"
    ]

    event = {
        "event_id": fake.uuid4(),
        "customer_id": random.randint(1, 50000),
        "event_type": random.choice(event_types),
        "event_time": datetime.now().isoformat(),
        "page": random.choice(["/", "/products", "/cart", "/checkout"]),
        "session_id": fake.uuid4()[:8],
        "device": random.choice(["desktop", "mobile", "tablet"]),
        "value": round(random.uniform(10, 500), 2)
    }

    return event


def main():
    """Generate and send events to Kafka"""
    print("="*60)
    print("Sparkle Demo - Kafka Event Generator")
    print("="*60)

    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'customer-events')
    events_per_second = int(os.getenv('EVENTS_PER_SECOND', 10))

    print(f"\nKafka Bootstrap: {kafka_bootstrap}")
    print(f"Topic: {kafka_topic}")
    print(f"Events/sec: {events_per_second}")

    # Wait for Kafka
    print("\nWaiting for Kafka...")
    time.sleep(10)

    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        print("\n✓ Connected to Kafka")
        print(f"✓ Generating {events_per_second} events/second...")
        print("\nPress Ctrl+C to stop\n")

        event_count = 0
        while True:
            for _ in range(events_per_second):
                event = create_customer_event()
                producer.send(kafka_topic, value=event)
                event_count += 1

                if event_count % 100 == 0:
                    print(f"  Sent {event_count} events...")

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n\n✓ Stopped. Total events sent: {event_count}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
