"""
Script to insert default validation rules into SQL Fabric database
Run this independently: python insert_rules_fixed.py

FIXED VERSION: Uses correct column names matching database schema
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
        
        print("="*70)
        print(" FIXED: INSERT DEFAULT VALIDATION RULES")
        print("="*70)
        print("\nConnecting to SQL Fabric...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✓ Connected successfully!")
        
        # Check existing table structure to ensure we use correct columns
        print("\nValidating table structure...")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'validation_rule_types'
            ORDER BY ORDINAL_POSITION
        """)
        columns = cursor.fetchall()
        
        if not columns:
            print("✗ Error: validation_rule_types table does not exist!")
            print("Please ensure the table is created first by running app_complete.py")
            cursor.close()
            conn.close()
            return
        
        column_names = [col[0] for col in columns]
        print(f"✓ Table columns found: {column_names}")
        
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
                try:
                    cursor.execute("DELETE FROM column_validation_rules WHERE rule_type_id IN (SELECT rule_type_id FROM validation_rule_types)")
                    cursor.execute("DELETE FROM validation_rule_types")
                    conn.commit()
                    print("✓ Existing rules deleted")
                except Exception as e:
                    print(f"Warning during deletion: {e}")
                    conn.rollback()
            else:
                print("\nSkipping insertion. Existing rules kept.")
                cursor.close()
                conn.close()
                return
        
        # Define default validation rules
        default_rules = [
            # (rule_id, name, description, parameters, category, is_active)
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
        
        print(f"\n{'='*70}")
        print(f"Inserting {len(default_rules)} default validation rules...")
        print(f"{'='*70}\n")
        
        success_count = 0
        # Insert rules with correct column names as they exist in the table
        for rule in default_rules:
            rule_id, name, description, parameters, category, is_active = rule
            
            try:
                # CRITICAL: Use EXACT column names from database
                # The error showed: rule_description, rule_category, created_at
                cursor.execute("""
                    SET IDENTITY_INSERT validation_rule_types ON;
                    
                    INSERT INTO validation_rule_types 
                    (rule_type_id, rule_name, rule_description, rule_category, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, GETDATE());
                    
                    SET IDENTITY_INSERT validation_rule_types OFF;
                """, rule_id, name, description, category, is_active)
                
                print(f"  ✓ Inserted: {name:15s} (ID: {rule_id}, Category: {category})")
                success_count += 1
            except Exception as e:
                print(f"  ✗ Failed to insert {name}: {e}")
                conn.rollback()
                # Continue with other rules
                continue
        
        conn.commit()
        print(f"\n{'='*70}")
        print(f"✓ Successfully inserted {success_count}/{len(default_rules)} validation rules!")
        print(f"{'='*70}\n")
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM validation_rule_types")
        final_count = cursor.fetchone()[0]
        print(f"Total rules in database: {final_count}\n")
        
        # Display all rules
        print("="*70)
        print("All Validation Rules in Database:")
        print("="*70)
        cursor.execute("""
            SELECT rule_type_id, rule_name, rule_description, rule_category
            FROM validation_rule_types 
            ORDER BY rule_type_id
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]:2d}. {row[1]:15s} - {row[2]:45s} [{row[3]}]")
        
        cursor.close()
        conn.close()
        print("\n" + "="*70)
        print("✓ COMPLETE - Rules Successfully Inserted!")
        print("="*70)
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"✗ ERROR: {e}")
        print(f"{'='*70}\n")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_default_rules()
    input("\nPress Enter to exit...")
