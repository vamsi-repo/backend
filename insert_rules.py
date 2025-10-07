"""
Script to insert default validation rules into SQL Fabric database
Run this independently: python insert_rules.py
"""

import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def insert_default_rules():
    """Insert default validation rules into validation_rule_types table"""
    
    try:
        # Get Azure credentials from environment
        client_id = os.getenv('AZURE_CLIENT_ID')
        client_secret = os.getenv('AZURE_CLIENT_SECRET')
        tenant_id = os.getenv('AZURE_TENANT_ID')
        server = os.getenv('FABRIC_SERVER')
        database = os.getenv('FABRIC_DATABASE')
        
        # Build connection string
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={server};"
            f"Database={database};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={client_id};"
            f"PWD={client_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        print("Connecting to SQL Fabric...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✓ Connected successfully!")
        
        # Check if rules already exist
        cursor.execute("SELECT COUNT(*) FROM validation_rule_types")
        count = cursor.fetchone()[0]
        print(f"\nCurrent rules in database: {count}")
        
        if count > 0:
            print("\nRules already exist. Do you want to:")
            print("1. Skip insertion (keep existing rules)")
            print("2. Delete all and re-insert")
            choice = input("Enter choice (1 or 2): ").strip()
            
            if choice == "2":
                print("\nDeleting existing rules...")
                # Delete in correct order (child tables first)
                cursor.execute("DELETE FROM column_validation_rules")
                cursor.execute("DELETE FROM validation_rule_types")
                conn.commit()
                print("✓ Existing rules deleted")
            else:
                print("\nSkipping insertion. Existing rules kept.")
                cursor.close()
                conn.close()
                return
        
        # Define default validation rules
        default_rules = [
            (1, 'Required', 'Field must not be empty', None, 'Basic', 1),
            (2, 'Text', 'Field accepts any text value', None, 'Basic', 1),
            (3, 'Int', 'Field must be an integer number', None, 'Basic', 1),
            (4, 'Float', 'Field must be a decimal/floating point number', None, 'Basic', 1),
            (5, 'Email', 'Field must be a valid email address', '{"pattern": "email"}', 'Basic', 1),
            (6, 'Date', 'Field must be a valid date', '{"format": "date"}', 'Basic', 1),
            (7, 'Alphanumeric', 'Field must contain only letters and numbers', '{"pattern": "alphanumeric"}', 'Basic', 1),
            (8, 'Boolean', 'Field must be true/false or yes/no', '{"values": [true, false]}', 'Basic', 1),
            (9, 'Phone', 'Field must be a valid phone number', '{"pattern": "phone"}', 'Extended', 1),
            (10, 'URL', 'Field must be a valid URL', '{"pattern": "url"}', 'Extended', 1),
        ]
        
        print(f"\nInserting {len(default_rules)} default validation rules...")
        
        for rule in default_rules:
            rule_id, name, description, parameters, category, is_active = rule
            
            cursor.execute("""
                INSERT INTO validation_rule_types 
                (rule_type_id, rule_name, description, parameters, rule_category, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, GETDATE())
            """, rule_id, name, description, parameters, category, is_active)
            
            print(f"  ✓ Inserted: {name}")
        
        conn.commit()
        print(f"\n✓ Successfully inserted {len(default_rules)} validation rules!")
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM validation_rule_types")
        final_count = cursor.fetchone()[0]
        print(f"\nTotal rules in database: {final_count}")
        
        # Display all rules
        print("\n" + "="*60)
        print("All Validation Rules:")
        print("="*60)
        cursor.execute("""
            SELECT rule_type_id, rule_name, rule_description, rule_category 
            FROM validation_rule_types 
            ORDER BY rule_type_id
        """)
        
        for row in cursor.fetchall():
            print(f"{row[0]:2d}. {row[1]:15s} - {row[2]} [{row[3]}]")
        
        cursor.close()
        conn.close()
        print("\n✓ Complete!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("="*60)
    print(" INSERT DEFAULT VALIDATION RULES")
    print("="*60)
    insert_default_rules()
    input("\nPress Enter to exit...")
