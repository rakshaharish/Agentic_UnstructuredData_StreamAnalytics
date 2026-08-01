from neo4j import GraphDatabase

def seed_neo4j_graph():
    print("🚀 Connecting to Neo4j Graph Database...")
    uri = "bolt://localhost:7687"
    auth = ("neo4j", "password123")
    
    driver = GraphDatabase.driver(uri, auth=auth)
    
    cypher_clean = "MATCH (n) DETACH DELETE n"
    
    cypher_seed = """
    // Create Customers
    CREATE (c1:Customer {id: 'CUST_9901A', name: 'John Doe', risk_score: 0.1})
    CREATE (c2:Customer {id: 'CUST_7702B', name: 'Jane Smith', risk_score: 0.85})
    
    // Create Infrastructure Fingerprints
    CREATE (ip:IPAddress {address: '192.168.1.45'})
    CREATE (dev:Device {hardware_id: 'DEV_IPHONE_X12'})
    
    // Model Graph Relationships
    CREATE (c1)-[:ACCESSED_FROM {timestamp: '2026-07-28T10:00:00'}]->(ip)
    CREATE (c1)-[:REGISTERED_DEVICE]->(dev)
    
    // Fraud linkage scenario: Second account accessed from the identical IP address
    CREATE (c2)-[:ACCESSED_FROM {timestamp: '2026-07-28T10:05:00'}]->(ip)
    """
    
    with driver.session() as session:
        # Step 1: Purge existing nodes
        session.run(cypher_clean)
        # Step 2: Write structural relationships
        session.run(cypher_seed)
        
    driver.close()
    print("✅ Neo4j relationship graph generated successfully.")


if __name__ == "__main__":
    seed_neo4j_graph()