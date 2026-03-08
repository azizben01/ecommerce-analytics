import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Burkina Faso locations
cities = [
    {"city": "Ouagadougou", "state": "Kadiogo", "country": "BF"},
    {"city": "Bobo-Dioulasso", "state": "Houet", "country": "BF"},
    {"city": "Koudougou", "state": "Boulkiemdé", "country": "BF"},
    {"city": "Ouahigouya", "state": "Yatenga", "country": "BF"},
    {"city": "Banfora", "state": "Comoé", "country": "BF"}
]

# ---------------------------------
# PARAMETERS
# ---------------------------------

NUM_CATEGORIES = 8
SUBCATEGORIES_PER_CAT = 3
NUM_PRODUCTS = 200
NUM_USERS = 120
MAX_SESSIONS_PER_USER = 6

START_DATE = datetime.now() - timedelta(days=90)
END_DATE = datetime.now()

# ---------------------------------
# GENERATE CATEGORIES
# ---------------------------------

categories = []

for c in range(NUM_CATEGORIES):
    cat_id = f"cat_{str(c).zfill(3)}"

    subcats = []

    for s in range(SUBCATEGORIES_PER_CAT):
        sub_id = f"sub_{str(c).zfill(3)}_{str(s).zfill(2)}"

        subcats.append({
            "subcategory_id": sub_id,
            "name": fake.word().title(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)
        })

    categories.append({
        "category_id": cat_id,
        "name": fake.word().title(),
        "subcategories": subcats
    })

# ---------------------------------
# GENERATE PRODUCTS
# ---------------------------------

products = []

for i in range(NUM_PRODUCTS):

    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])

    product_id = f"prod_{str(i).zfill(5)}"

    creation_date = fake.date_time_between(
        start_date=START_DATE,
        end_date=END_DATE
    )

    base_price = round(random.uniform(5, 300), 2)

    products.append({
        "product_id": product_id,
        "name": fake.word().title() + " Product",
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": base_price,
        "current_stock": random.randint(0, 200),
        "is_active": True,
        "price_history": [
            {
                "price": base_price + random.randint(5, 40),
                "date": creation_date.isoformat()
            },
            {
                "price": base_price,
                "date": (creation_date + timedelta(days=20)).isoformat()
            }
        ],
        "creation_date": creation_date.isoformat()
    })

# ---------------------------------
# GENERATE USERS
# ---------------------------------

users = []

for i in range(NUM_USERS):

    location = random.choice(cities)

    registration = fake.date_time_between(
        start_date=START_DATE,
        end_date=END_DATE
    )

    user_id = f"user_{str(i).zfill(6)}"

    users.append({
        "user_id": user_id,
        "geo_data": location,
        "registration_date": registration.isoformat(),
        "last_active": fake.date_time_between(
            start_date=registration,
            end_date=END_DATE
        ).isoformat()
    })

# ---------------------------------
# GENERATE SESSIONS
# ---------------------------------

sessions = []
transactions = []

for user in users:

    session_count = random.randint(1, MAX_SESSIONS_PER_USER)

    for _ in range(session_count):

        session_id = "sess_" + uuid.uuid4().hex[:10]

        start = fake.date_time_between(
            start_date=START_DATE,
            end_date=END_DATE
        )

        duration = random.randint(120, 1500)
        end = start + timedelta(seconds=duration)

        viewed_products = random.sample(products, random.randint(1, 5))

        viewed_ids = [p["product_id"] for p in viewed_products]

        page_views = []

        timestamp = start

        for p in viewed_products:

            page_views.append({
                "timestamp": timestamp.isoformat(),
                "page_type": "product_detail",
                "product_id": p["product_id"],
                "category_id": p["category_id"],
                "view_duration": random.randint(20, 120)
            })

            timestamp += timedelta(seconds=random.randint(30, 90))

        cart = {}

        if random.random() < 0.4:

            product = random.choice(viewed_products)

            quantity = random.randint(1, 3)

            cart[product["product_id"]] = {
                "quantity": quantity,
                "price": product["base_price"]
            }

        conversion = "converted" if cart else "not_converted"

        sessions.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "duration_seconds": duration,
            "geo_data": user["geo_data"],
            "device_profile": {
                "type": random.choice(["mobile", "desktop"]),
                "os": random.choice(["Android", "iOS", "Windows"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox"])
            },
            "viewed_products": viewed_ids,
            "page_views": page_views,
            "cart_contents": cart,
            "conversion_status": conversion,
            "referrer": random.choice(["direct", "search_engine", "social_media"])
        })

        # ---------------------------------
        # TRANSACTIONS
        # ---------------------------------

        if cart:

            items = []
            subtotal = 0

            for pid, item in cart.items():

                subtotal += item["quantity"] * item["price"]

                items.append({
                    "product_id": pid,
                    "quantity": item["quantity"],
                    "unit_price": item["price"],
                    "subtotal": item["quantity"] * item["price"]
                })

            discount = round(subtotal * random.uniform(0.05, 0.15), 2)

            total = subtotal - discount

            transactions.append({
                "transaction_id": "txn_" + uuid.uuid4().hex[:12],
                "session_id": session_id,
                "user_id": user["user_id"],
                "timestamp": end.isoformat(),
                "items": items,
                "subtotal": round(subtotal, 2),
                "discount": discount,
                "total": round(total, 2),
                "payment_method": random.choice([
                    "mobile_money",
                    "credit_card",
                    "bank_transfer"
                ]),
                "status": "completed"
            })

# ---------------------------------
# SAVE FILES
# ---------------------------------

with open("categories.json", "w") as f:
    json.dump(categories, f, indent=2)

with open("products.json", "w") as f:
    json.dump(products, f, indent=2)

with open("users.json", "w") as f:
    json.dump(users, f, indent=2)

with open("sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)

with open("transactions.json", "w") as f:
    json.dump(transactions, f, indent=2)

print("Dataset generated successfully.")