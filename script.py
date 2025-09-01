"""
Fake Customer Data Generator for Elasticsearch

INSTALLATION:
pip install faker elasticsearch requests

ELASTICSEARCH URL EXAMPLES:
- Local: http://localhost:9200
- Docker: http://localhost:9200
- Cloud: https://your-deployment.es.region.cloud.es.io:443
- Custom: http://your-server-ip:9200

USAGE:
1. Update ELASTICSEARCH_URL in main() function
2. Add username/password if authentication required
3. Run: python script.py
"""

import json
import random
from datetime import datetime, timedelta
from faker import Faker
from typing import List, Dict, Any
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

fake = Faker()

class CustomerDataGenerator:
    def __init__(self, elasticsearch_url: str = "http://localhost:9200",
                 username: str = None, password: str = None):
        # Elasticsearch configuration
        self.elasticsearch_url = elasticsearch_url
        self.username = username
        self.password = password
        self.es_client = None

        # Data generation settings
        self.customer_statuses = ["ACTIVE", "INACTIVE", "PREMIUM", "SUSPENDED", "PENDING"]
        self.address_types = ["HOME", "WORK", "BILLING", "SHIPPING", "OTHER"]
        self.preference_categories = ["COMMUNICATION", "SHOPPING", "PRIVACY", "MARKETING", "NOTIFICATIONS"]
        self.preference_keys = {
            "COMMUNICATION": ["EMAIL_NOTIFICATIONS", "SMS_NOTIFICATIONS", "PHONE_CALLS", "PUSH_NOTIFICATIONS"],
            "SHOPPING": ["PREFERRED_CATEGORY", "AUTO_REORDER", "WISHLIST_SHARING", "PRICE_ALERTS"],
            "PRIVACY": ["DATA_SHARING", "PROFILE_VISIBILITY", "TRACKING_CONSENT", "ANALYTICS_OPT_IN"],
            "MARKETING": ["PROMOTIONAL_EMAILS", "SPECIAL_OFFERS", "NEWSLETTER", "PRODUCT_UPDATES"],
            "NOTIFICATIONS": ["ORDER_UPDATES", "DELIVERY_ALERTS", "PAYMENT_REMINDERS", "ACCOUNT_SECURITY"]
        }
        self.shopping_categories = ["ELECTRONICS", "FASHION", "BOOKS", "HOME_GARDEN", "SPORTS", "BEAUTY", "TOYS"]
        self.languages = ["EN", "ES", "FR", "DE", "IT", "PT", "ZH", "JA"]

        # Counters for unique IDs
        self.customer_id_counter = 1
        self.address_id_counter = 1
        self.preference_id_counter = 1

    def generate_customer_addresses(self, customer_id: int, num_addresses: int = None) -> List[Dict[str, Any]]:
        """Generate nested address items for a customer"""
        if num_addresses is None:
            num_addresses = random.randint(1, 3)

        addresses = []
        address_types_used = []

        for i in range(num_addresses):
            # Ensure we don't duplicate address types for the same customer
            available_types = [t for t in self.address_types if t not in address_types_used]
            if not available_types:
                available_types = self.address_types

            address_type = random.choice(available_types)
            address_types_used.append(address_type)

            address = {
                "address_id": self.address_id_counter,
                "customer_id": customer_id,
                "address_type": address_type,
                "street_address": fake.street_address(),
                "city": fake.city(),
                "state_province": fake.state_abbr(),
                "postal_code": fake.postcode(),
                "country": fake.country_code(representation="alpha-3"),
                "is_default": "Y" if i == 0 else random.choice(["Y", "N"]),
                "created_date": fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d")
            }

            addresses.append(address)
            self.address_id_counter += 1

        return addresses

    def generate_customer_preferences(self, customer_id: int, num_preferences: int = None) -> List[Dict[str, Any]]:
        """Generate nested preference items for a customer"""
        if num_preferences is None:
            num_preferences = random.randint(2, 6)

        preferences = []
        used_combinations = set()

        for _ in range(num_preferences):
            category = random.choice(self.preference_categories)
            available_keys = [k for k in self.preference_keys[category]
                              if (category, k) not in used_combinations]

            if not available_keys:
                continue

            key = random.choice(available_keys)
            used_combinations.add((category, key))

            # Generate appropriate values based on the key
            if "NOTIFICATIONS" in key or "EMAIL" in key or "SMS" in key:
                value = random.choice(["true", "false"])
            elif "CATEGORY" in key:
                value = random.choice(self.shopping_categories)
            elif "SHARING" in key or "CONSENT" in key or "OPT" in key:
                value = random.choice(["true", "false", "partial"])
            else:
                value = random.choice(["true", "false", "enabled", "disabled"])

            preference = {
                "preference_id": self.preference_id_counter,
                "customer_id": customer_id,
                "preference_category": category,
                "preference_key": key,
                "preference_value": value,
                "created_date": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d")
            }

            preferences.append(preference)
            self.preference_id_counter += 1

        return preferences

    def generate_customer(self) -> Dict[str, Any]:
        """Generate a complete customer record with nested addresses and preferences"""
        customer_id = self.customer_id_counter
        registration_date = fake.date_between(start_date="-6y", end_date="-1y")

        # Generate realistic login date (within last 30 days for active users)
        status = random.choice(self.customer_statuses)
        if status == "ACTIVE":
            last_login = fake.date_time_between(start_date="-30d", end_date="now")
        elif status == "PREMIUM":
            last_login = fake.date_time_between(start_date="-7d", end_date="now")
        else:
            last_login = fake.date_time_between(start_date="-6m", end_date="-1m")

        # Generate realistic spending based on status
        if status == "PREMIUM":
            total_orders = random.randint(20, 100)
            total_spent = random.randint(2000, 15000)
            loyalty_points = int(total_spent * random.uniform(0.3, 0.7))
        elif status == "ACTIVE":
            total_orders = random.randint(5, 30)
            total_spent = random.randint(200, 5000)
            loyalty_points = int(total_spent * random.uniform(0.2, 0.5))
        else:
            total_orders = random.randint(0, 10)
            total_spent = random.randint(0, 1000)
            loyalty_points = int(total_spent * random.uniform(0.1, 0.3))

        customer = {
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
            "gender": random.choice(["M", "F", "O"]),
            "registration_date": registration_date.strftime("%Y-%m-%d"),
            "last_login_date": last_login.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "customer_status": status,
            "total_orders": total_orders,
            "total_spent": total_spent,
            "loyalty_points": loyalty_points,
            "preferred_language": random.choice(self.languages),
            "marketing_consent": random.choice(["Y", "N"]),
            "created_by": random.choice(["system", "admin", "import"]),
            "created_date": registration_date.strftime("%Y-%m-%d"),
            "modified_by": random.choice(["system", "admin", "user"]),
            "modified_date": fake.date_between(start_date=registration_date, end_date="today").strftime("%Y-%m-%d"),
            "customer_addresses_items": self.generate_customer_addresses(customer_id),
            "customer_preferences_items": self.generate_customer_preferences(customer_id)
        }

        self.customer_id_counter += 1
        return customer

    def generate_customers(self, num_customers: int) -> List[Dict[str, Any]]:
        """Generate multiple customers"""
        return [self.generate_customer() for _ in range(num_customers)]

    def generate_elasticsearch_bulk_insert(self, customers: List[Dict[str, Any]], index_name: str = "customers") -> str:
        """Generate Elasticsearch bulk insert format"""
        bulk_data = []
        for customer in customers:
            # Add the index action
            action = {"index": {"_index": index_name, "_id": customer["customer_id"]}}
            bulk_data.append(json.dumps(action))
            bulk_data.append(json.dumps(customer))

        return "\n".join(bulk_data) + "\n"

    def generate_elasticsearch_individual_inserts(self, customers: List[Dict[str, Any]], index_name: str = "customers") -> str:
        """Generate individual POST commands for Elasticsearch"""
        commands = []
        for customer in customers:
            command = f"POST /{index_name}/_doc/{customer['customer_id']}\n{json.dumps(customer, indent=2)}\n"
            commands.append(command)

        return "\n".join(commands)

    def connect_to_elasticsearch(self) -> bool:
        """Connect to Elasticsearch cluster"""
        try:
            if self.username and self.password:
                self.es_client = Elasticsearch(
                    [self.elasticsearch_url],
                    http_auth=(self.username, self.password),
                    verify_certs=False,
                    timeout=30
                )
            else:
                self.es_client = Elasticsearch([self.elasticsearch_url])

            # Test connection
            if self.es_client.ping():
                print(f"✅ Connected to Elasticsearch at {self.elasticsearch_url}")
                return True
            else:
                print(f"❌ Failed to connect to Elasticsearch at {self.elasticsearch_url}")
                return False

        except Exception as e:
            print(f"❌ Elasticsearch connection error: {str(e)}")
            return False



    def upload_customers_to_elasticsearch(self, customers: List[Dict[str, Any]],
                                          index_name: str = "customers") -> bool:
        """Upload customers to Elasticsearch using bulk API"""
        if not self.es_client:
            print("❌ Not connected to Elasticsearch. Call connect_to_elasticsearch() first.")
            return False

        try:
            # Prepare documents for bulk upload
            actions = []
            for customer in customers:
                action = {
                    "_index": index_name,
                    "_id": customer["customer_id"],
                    "_source": customer
                }
                actions.append(action)

            # Upload using bulk API
            success, failed = bulk(self.es_client, actions, index=index_name, chunk_size=100)

            print(f"✅ Successfully uploaded {success} customers to '{index_name}'")
            if failed:
                print(f"⚠️  Failed to upload {len(failed)} customers")

            return len(failed) == 0

        except Exception as e:
            print(f"❌ Error uploading customers: {str(e)}")
            return False

    def upload_single_customer(self, customer: Dict[str, Any], index_name: str = "customers") -> bool:
        """Upload a single customer to Elasticsearch"""
        if not self.es_client:
            print("❌ Not connected to Elasticsearch. Call connect_to_elasticsearch() first.")
            return False

        try:
            response = self.es_client.index(
                index=index_name,
                id=customer["customer_id"],
                body=customer
            )
            print(f"✅ Uploaded customer {customer['customer_id']} ({customer['first_name']} {customer['last_name']})")
            return True

        except Exception as e:
            print(f"❌ Error uploading customer {customer['customer_id']}: {str(e)}")
            return False


def main():
    """Example usage with Elasticsearch integration"""

    # =====================================
    # CONFIGURATION - UPDATE THESE VALUES
    # =====================================

    # Option 1: Local Elasticsearch (default)
    ELASTICSEARCH_URL = "http://192.168.1.27:9200"
    USERNAME = None  # No auth for local
    PASSWORD = None

    # Option 2: Elasticsearch Cloud
    # ELASTICSEARCH_URL = "https://your-deployment.es.region.cloud.es.io:443"
    # USERNAME = "elastic"
    # PASSWORD = "your-password"

    # Option 3: Custom Elasticsearch server
    # ELASTICSEARCH_URL = "http://your-server:9200"
    # USERNAME = "your-username"
    # PASSWORD = "your-password"

    # =====================================
    # GENERATE DATA
    # =====================================

    print("🚀 Starting Customer Data Generation...")

    # Create generator with Elasticsearch connection
    generator = CustomerDataGenerator(
        elasticsearch_url=ELASTICSEARCH_URL,
        username=USERNAME,
        password=PASSWORD
    )

    # Generate fake customers
    print("📊 Generating fake customer data...")
    customers = generator.generate_customers(100)  # Generate 20 customers
    print(f"✅ Generated {len(customers)} customers")

    # =====================================
    # SAVE TO FILES (Always works)
    # =====================================

    print("\n💾 Saving data to files...")



    # Save as bulk insert format
    with open("customers_bulk.ndjson", "w") as f:
        f.write(generator.generate_elasticsearch_bulk_insert(customers))

    # Save as JSON
    with open("customers.json", "w") as f:
        json.dump(customers, f, indent=2)

    print("✅ Files saved:")
    print("   - customers_individual.txt (Ready for Kibana Dev Tools)")
    print("   - customers_bulk.ndjson (Ready for bulk API)")
    print("   - customers.json (JSON array format)")

    # =====================================
    # UPLOAD TO ELASTICSEARCH (Optional)
    # =====================================

    print(f"\n🔌 Attempting to connect to Elasticsearch at {ELASTICSEARCH_URL}...")

    if generator.connect_to_elasticsearch():
        print("\n🏗️  Creating index with mapping...")


        print(f"\n📤 Uploading {len(customers)} customers to Elasticsearch...")
        if generator.upload_customers_to_elasticsearch(customers, "customers"):
            print("🎉 All customers uploaded successfully!")

                # Test with a search query
            print("\n🔍 Testing with a sample search...")
            try:
                search_body = {
                        "query": {"match_all": {}},
                        "size": 3
                    }
                results = generator.es_client.search(index="customers", body=search_body)
                print(f"✅ Found {results['hits']['total']['value']} total customers")
                print("📋 Sample customers:")
                for hit in results['hits']['hits']:
                    customer = hit['_source']
                    print(f"   - {customer['first_name']} {customer['last_name']} ({customer['email']})")

            except Exception as e:
                    print(f"❌ Search test failed: {str(e)}")
            else:
                print("⚠️  Some customers failed to upload")
        else:
            print("❌ Failed to create index")


def demo_advanced_usage():
    """Advanced usage examples"""
    print("🔧 Advanced Usage Examples:")
    print("="*50)

    # Example 1: Generate and upload specific customer types
    generator = CustomerDataGenerator()

    # Generate only premium customers
    premium_customers = []
    for _ in range(5):
        customer = generator.generate_customer()
        customer['customer_status'] = 'PREMIUM'
        customer['total_spent'] = random.randint(5000, 20000)
        customer['loyalty_points'] = int(customer['total_spent'] * 0.5)
        premium_customers.append(customer)

    print(f"Generated {len(premium_customers)} premium customers")

    # Example 2: Generate customers for specific regions
    east_coast_customers = []
    east_coast_states = ['NY', 'NJ', 'PA', 'MA', 'FL', 'GA', 'NC', 'SC', 'VA', 'MD']

    for _ in range(10):
        customer = generator.generate_customer()
        # Update all addresses to be East Coast
        for address in customer['customer_addresses_items']:
            address['state_province'] = random.choice(east_coast_states)
        east_coast_customers.append(customer)

    print(f"Generated {len(east_coast_customers)} East Coast customers")


if __name__ == "__main__":
    # Run main example
    main()

    print("\n" + "="*60 + "\n")

    # Show advanced usage
    demo_advanced_usage()