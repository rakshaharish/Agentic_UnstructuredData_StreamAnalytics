import json
import redis

def seed_redis_cache():
    print("🚀 Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # Clear out any existing entries to make execution idempotent
    r.flushdb()
    
    # Active Session 1
    session_1 = {
        "customer_id": "CUST_9901A",
        "ip_address": "192.168.1.45",
        "device_id": "DEV_IPHONE_X12",
        "current_page": "/checkout/payment",
        "items_in_cart": json.dumps(["ITEM_SKU_88", "ITEM_SKU_12"])
    }
    
    # Active Session 2 (Fraud Ring Linkage)
    session_2 = {
        "customer_id": "CUST_7702B",
        "ip_address": "192.168.1.45",  # Shared IP with CUST_9901A
        "device_id": "DEV_MACBOOK_PRO",
        "current_page": "/homepage",
        "items_in_cart": json.dumps(["ITEM_SKU_99"])
    }

    # Write keys as structured strings
    r.set("session:active:CUST_9901A", json.dumps(session_1))
    r.set("session:active:CUST_7702B", json.dumps(session_2))
    
    print("✅ Redis data generated successfully.")

if __name__ == "__main__":
    seed_redis_cache()