"""
Synthetic data generators for Sparkle demo.

Generates realistic CRM data: customers, orders, returns, events.
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_customers(num_customers: int = 50000) -> List[Dict[str, Any]]:
    """
    Generate synthetic customer data.

    Args:
        num_customers: Number of customers to generate

    Returns:
        List of customer dictionaries
    """
    customers = []
    tiers = ["Basic", "Premium", "Enterprise"]
    tier_weights = [0.6, 0.3, 0.1]  # Most are Basic

    for i in range(1, num_customers + 1):
        signup_date = fake.date_between(start_date="-3y", end_date="today")
        last_activity = fake.date_between(start_date=signup_date, end_date="today")

        customer = {
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": fake.country_code(),
            "signup_date": signup_date.isoformat(),
            "last_activity_date": last_activity.isoformat(),
            "tier": random.choices(tiers, weights=tier_weights)[0],
            "is_active": random.random() > 0.1,  # 90% active
            "lifetime_value": round(random.uniform(100, 50000), 2),
            "total_orders": random.randint(1, 100),
            "avg_order_value": round(random.uniform(50, 500), 2),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

        customers.append(customer)

    return customers


def generate_orders(num_orders: int = 150000, num_customers: int = 50000) -> List[Dict[str, Any]]:
    """
    Generate synthetic order data.

    Args:
        num_orders: Number of orders to generate
        num_customers: Number of customers (for foreign key)

    Returns:
        List of order dictionaries
    """
    orders = []
    statuses = ["completed", "pending", "cancelled", "refunded"]
    status_weights = [0.7, 0.15, 0.1, 0.05]

    for i in range(1, num_orders + 1):
        customer_id = random.randint(1, num_customers)
        order_date = fake.date_time_between(start_date="-2y", end_date="now")

        order = {
            "order_id": i,
            "customer_id": customer_id,
            "order_date": order_date.isoformat(),
            "order_amount": round(random.uniform(10, 1000), 2),
            "tax_amount": round(random.uniform(0, 100), 2),
            "shipping_amount": round(random.uniform(0, 50), 2),
            "total_amount": 0,  # Will calculate
            "status": random.choices(statuses, weights=status_weights)[0],
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
            "items_count": random.randint(1, 10),
            "created_at": order_date.isoformat(),
            "updated_at": order_date.isoformat(),
        }

        # Calculate total
        order["total_amount"] = round(
            order["order_amount"] + order["tax_amount"] + order["shipping_amount"],
            2
        )

        orders.append(order)

    return orders


def generate_returns(num_returns: int = 5000, num_orders: int = 150000) -> List[Dict[str, Any]]:
    """
    Generate synthetic return data.

    Args:
        num_returns: Number of returns to generate
        num_orders: Number of orders (for foreign key)

    Returns:
        List of return dictionaries
    """
    returns = []
    reasons = [
        "defective",
        "wrong_item",
        "not_as_described",
        "changed_mind",
        "late_delivery",
        "quality_issues"
    ]

    for i in range(1, num_returns + 1):
        order_id = random.randint(1, num_orders)
        return_date = fake.date_time_between(start_date="-18m", end_date="now")

        return_item = {
            "return_id": i,
            "order_id": order_id,
            "return_date": return_date.isoformat(),
            "return_amount": round(random.uniform(10, 500), 2),
            "reason": random.choice(reasons),
            "status": random.choice(["approved", "pending", "rejected"]),
            "refund_issued": random.random() > 0.3,  # 70% get refund
            "created_at": return_date.isoformat(),
        }

        returns.append(return_item)

    return returns


def generate_customer_events(num_events: int = 100000, num_customers: int = 50000) -> List[Dict[str, Any]]:
    """
    Generate synthetic customer event data (clickstream, app events).

    Args:
        num_events: Number of events to generate
        num_customers: Number of customers

    Returns:
        List of event dictionaries
    """
    events = []
    event_types = [
        "page_view",
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_start",
        "checkout_complete",
        "search",
        "filter_applied",
        "wishlist_add",
        "review_posted"
    ]

    pages = [
        "/",
        "/products",
        "/products/electronics",
        "/products/clothing",
        "/cart",
        "/checkout",
        "/account",
        "/orders",
        "/support"
    ]

    for i in range(1, num_events + 1):
        customer_id = random.randint(1, num_customers)
        event_time = fake.date_time_between(start_date="-30d", end_date="now")

        event = {
            "event_id": i,
            "customer_id": customer_id,
            "event_type": random.choice(event_types),
            "event_time": event_time.isoformat(),
            "page": random.choice(pages),
            "session_id": fake.uuid4(),
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
            "referrer": random.choice(["google", "facebook", "direct", "email", "affiliate"]),
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
        }

        events.append(event)

    return events


def generate_salesforce_export() -> List[Dict[str, Any]]:
    """
    Generate synthetic Salesforce export data (Accounts, Opportunities).

    Returns:
        List of Salesforce account dictionaries
    """
    accounts = []

    for i in range(1, 1000):
        created_date = fake.date_between(start_date="-5y", end_date="-1y")
        last_modified = fake.date_between(start_date=created_date, end_date="today")

        account = {
            "Id": fake.uuid4(),
            "Name": fake.company(),
            "Type": random.choice(["Customer", "Prospect", "Partner", "Competitor"]),
            "Industry": random.choice(["Technology", "Finance", "Healthcare", "Retail", "Manufacturing"]),
            "AnnualRevenue": random.randint(100000, 100000000),
            "NumberOfEmployees": random.randint(10, 10000),
            "BillingStreet": fake.street_address(),
            "BillingCity": fake.city(),
            "BillingState": fake.state(),
            "BillingPostalCode": fake.zipcode(),
            "BillingCountry": fake.country(),
            "Phone": fake.phone_number(),
            "Website": fake.url(),
            "CreatedDate": created_date.isoformat(),
            "LastModifiedDate": last_modified.isoformat(),
        }

        accounts.append(account)

    return accounts


def generate_ga4_export() -> List[Dict[str, Any]]:
    """
    Generate synthetic Google Analytics 4 export data.

    Returns:
        List of GA4 event dictionaries
    """
    ga4_events = []

    for i in range(1, 10000):
        event_timestamp = fake.date_time_between(start_date="-7d", end_date="now")

        event = {
            "event_date": event_timestamp.strftime("%Y%m%d"),
            "event_timestamp": int(event_timestamp.timestamp() * 1000000),
            "event_name": random.choice([
                "page_view",
                "scroll",
                "click",
                "purchase",
                "add_to_cart",
                "begin_checkout"
            ]),
            "event_params": {
                "page_location": random.choice([
                    "https://example.com/",
                    "https://example.com/products",
                    "https://example.com/cart",
                    "https://example.com/checkout"
                ]),
                "ga_session_id": str(random.randint(1000000000, 9999999999)),
                "engagement_time_msec": random.randint(1000, 300000),
            },
            "user_pseudo_id": fake.uuid4(),
            "user_properties": {
                "user_id": str(random.randint(1, 50000)),
            },
            "device": {
                "category": random.choice(["desktop", "mobile", "tablet"]),
                "mobile_brand_name": random.choice(["Apple", "Samsung", "Google", "", ""]),
                "operating_system": random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            },
            "geo": {
                "country": fake.country_code(),
                "city": fake.city(),
            },
            "traffic_source": {
                "source": random.choice(["google", "facebook", "direct", "(direct)"]),
                "medium": random.choice(["organic", "cpc", "social", "(none)"]),
            },
        }

        ga4_events.append(event)

    return ga4_events


def generate_mainframe_copybook_data() -> List[str]:
    """
    Generate synthetic mainframe EBCDIC-style fixed-width data.

    Returns:
        List of fixed-width formatted strings
    """
    records = []

    for i in range(1, 1000):
        # Fixed-width format: ID(10), NAME(50), BALANCE(12), DATE(8)
        record = (
            f"{i:010d}"  # Customer ID (10 chars, zero-padded)
            f"{fake.name()[:50]:<50}"  # Name (50 chars, left-aligned)
            f"{random.randint(0, 999999999):012d}"  # Balance (12 chars, zero-padded)
            f"{fake.date_this_decade().strftime('%Y%m%d')}"  # Date (YYYYMMDD)
        )

        records.append(record)

    return records


def generate_hl7_message() -> str:
    """
    Generate a synthetic HL7 ADT message (healthcare).

    Returns:
        HL7 message string
    """
    message_id = fake.uuid4()[:20]
    patient_id = str(random.randint(10000, 99999))
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    hl7 = f"""MSH|^~\\&|HIS|HOSPITAL|EKG|EKG|{timestamp}||ADT^A01|{message_id}|P|2.5
PID|1||{patient_id}||{fake.last_name()}^{fake.first_name()}^||{fake.date_of_birth().strftime('%Y%m%d')}|{"M" if random.random() > 0.5 else "F"}|||{fake.street_address()}^^{fake.city()}^{fake.state_abbr()}^{fake.zipcode()}
PV1|1|I|ICU^101^01||||123456^{fake.name()}^MD|||SUR||||ADM||A0||123456^{fake.name()}^MD|MED|{patient_id}|||||||||||||||||||||||||{timestamp}
"""

    return hl7


def generate_x12_edi_837() -> str:
    """
    Generate a synthetic X12 EDI 837 message (healthcare claim).

    Returns:
        X12 EDI string
    """
    control_number = str(random.randint(100000000, 999999999))
    timestamp = datetime.now().strftime("%y%m%d")
    time = datetime.now().strftime("%H%M")

    x12 = f"""ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *{timestamp}*{time}*U*00401*{control_number}*0*P*:~
GS*HC*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{time}*1*X*004010X098A1~
ST*837*0001~
BHT*0019*00*{control_number}*{datetime.now().strftime('%Y%m%d')}*{time}*CH~
NM1*41*2*{fake.company()}*****46*{str(random.randint(1000000000, 9999999999))}~
PER*IC*{fake.name()}*TE*{fake.phone_number()}~
NM1*40*2*{fake.company()}*****46*{str(random.randint(1000000000, 9999999999))}~
HL*1**20*1~
NM1*85*2*{fake.name()}*****XX*{str(random.randint(1000000000, 9999999999))}~
SE*9*0001~
GE*1*1~
IEA*1*{control_number}~
"""

    return x12
