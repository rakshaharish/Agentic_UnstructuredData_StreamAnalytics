from pymongo import MongoClient
from datetime import datetime

def seed_mongo_documents():
    print("🚀 Connecting to MongoDB...")
    client = MongoClient("mongodb://localhost:27017/")
    db = client["enterprise_db"]
    collection = db["orders"]
    
    # Drop existing collections to maintain strict idempotency
    collection.drop()
    
    # Insert structured orders matching our user IDs
    mock_orders = [
        {
            "order_id": "ORD-2026-001",
            "customer_id": "CUST_9901A",
            "transaction": {
                "amount": 250.75,
                "currency": "USD",
                "payment_method": "Credit Card"
            },
            "line_items": [
                {"sku": "ITEM_SKU_88", "quantity": 1, "price": 200.00},
                {"sku": "ITEM_SKU_12", "quantity": 2, "price": 25.37}
            ],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "fulfillment_warehouse": "NJ-EAST-02"
            }
        },
        {
            "order_id": "ORD-2026-002",
            "customer_id": "CUST_7702B",
            "transaction": {
                "amount": 1200.00,
                "currency": "USD",
                "payment_method": "Wire Transfer"
            },
            "line_items": [
                {"sku": "ITEM_SKU_99", "quantity": 1, "price": 1200.00}
            ],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "fulfillment_warehouse": "CA-WEST-01"
            }
        }
    ]
    
    result = collection.insert_many(mock_orders)
    print(f"✅ MongoDB documents generated successfully. Inserted IDs: {result.inserted_ids}")

if __name__ == "__main__":
    seed_mongo_documents()
