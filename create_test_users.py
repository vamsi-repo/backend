"""
Create Test Users for Admin/Tenant/User Workflow Testing
This script creates multiple test users to demonstrate the multi-tenant system
"""

import pyodbc
import bcrypt
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_test_users():
    """Create test users for demonstration"""
    
    # Get database credentials
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    server = os.getenv('FABRIC_SERVER')
    database = os.getenv('FABRIC_DATABASE')
    
    print("=" * 70)
    print("Creating Test Users for Multi-Tenant System")
    print("=" * 70)
    print(f"\nServer: {server}")
    print(f"Database: {database}\n")
    
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
    
    # Test users to create
    test_users = [
        {
            'email': 'admin@datasyncai.com',
            'password': 'Admin@123',
            'first_name': 'System',
            'last_name': 'Administrator',
            'role': 'admin',
            'is_approved': 1,
            'domain': 'datasyncai.com',
            'mobile': '9999999999'
        },
        {
            'email': 'tenant@company.com',
            'password': 'Tenant@123',
            'first_name': 'Company',
            'last_name': 'Manager',
            'role': 'tenant',
            'is_approved': 1,
            'domain': 'company.com',
            'mobile': '8888888888'
        },
        {
            'email': 'user1@company.com',
            'password': 'User@123',
            'first_name': 'John',
            'last_name': 'Doe',
            'role': 'user',
            'is_approved': 1,
            'domain': 'company.com',
            'mobile': '7777777777'
        },
        {
            'email': 'user2@company.com',
            'password': 'User@123',
            'first_name': 'Jane',
            'last_name': 'Smith',
            'role': 'user',
            'is_approved': 0,  # Pending approval
            'domain': 'company.com',
            'mobile': '6666666666'
        },
        {
            'email': 'tenant@techcorp.io',
            'password': 'Tenant@123',
            'first_name': 'Tech',
            'last_name': 'Admin',
            'role': 'tenant',
            'is_approved': 1,
            'domain': 'techcorp.io',
            'mobile': '5555555555'
        },
        {
            'email': 'user1@techcorp.io',
            'password': 'User@123',
            'first_name': 'Alice',
            'last_name': 'Johnson',
            'role': 'user',
            'is_approved': 1,
            'domain': 'techcorp.io',
            'mobile': '4444444444'
        },
        {
            'email': 'user2@techcorp.io',
            'password': 'User@123',
            'first_name': 'Bob',
            'last_name': 'Williams',
            'role': 'user',
            'is_approved': 0,  # Pending approval
            'domain': 'techcorp.io',
            'mobile': '3333333333'
        }
    ]
    
    try:
        # Connect to database
        print("Connecting to SQL Fabric...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✓ Connected successfully!\n")
        
        created_count = 0
        updated_count = 0
        skipped_count = 0
        
        print("Creating/Updating test users...\n")
        print("-" * 70)
        
        for user_data in test_users:
            email = user_data['email']
            
            # Check if user exists
            cursor.execute("SELECT id, role, is_approved FROM login_details WHERE email = ?", email)
            existing = cursor.fetchone()
            
            # Hash password
            password_hash = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            if existing:
                # Update existing user
                cursor.execute("""
                    UPDATE login_details 
                    SET password_hash = ?,
                        first_name = ?,
                        last_name = ?,
                        mobile = ?,
                        role = ?,
                        is_approved = ?,
                        domain = ?,
                        updated_at = GETDATE()
                    WHERE email = ?
                """, password_hash, user_data['first_name'], user_data['last_name'],
                user_data['mobile'], user_data['role'], user_data['is_approved'],
                user_data['domain'], email)
                
                print(f"✓ UPDATED: {email:<30} | Role: {user_data['role']:<8} | Approved: {user_data['is_approved']}")
                updated_count += 1
            else:
                # Insert new user
                cursor.execute("""
                    INSERT INTO login_details 
                    (email, password_hash, first_name, last_name, mobile, role, is_approved, domain, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                """, email, password_hash, user_data['first_name'], user_data['last_name'],
                user_data['mobile'], user_data['role'], user_data['is_approved'], user_data['domain'])
                
                print(f"✓ CREATED: {email:<30} | Role: {user_data['role']:<8} | Approved: {user_data['is_approved']}")
                created_count += 1
        
        conn.commit()
        
        print("-" * 70)
        print(f"\nSummary:")
        print(f"  Created: {created_count} users")
        print(f"  Updated: {updated_count} users")
        print(f"  Total:   {created_count + updated_count} users processed")
        
        # Show all users grouped by domain
        print("\n" + "=" * 70)
        print("TEST USERS BY DOMAIN")
        print("=" * 70)
        
        cursor.execute("""
            SELECT domain, COUNT(*) as user_count
            FROM login_details
            GROUP BY domain
            ORDER BY domain
        """)
        
        domains = cursor.fetchall()
        
        for domain, count in domains:
            print(f"\n📁 Domain: {domain or 'N/A'}")
            print("-" * 70)
            
            cursor.execute("""
                SELECT email, first_name, last_name, role, is_approved
                FROM login_details
                WHERE domain = ? OR (domain IS NULL AND ? IS NULL)
                ORDER BY role DESC, email
            """, domain, domain)
            
            domain_users = cursor.fetchall()
            
            for email, first_name, last_name, role, is_approved in domain_users:
                name = f"{first_name} {last_name}"
                approved_str = "✓ APPROVED" if is_approved else "⏳ PENDING"
                print(f"  {email:<30} | {name:<25} | {role.upper():<8} | {approved_str}")
        
        # Show login credentials
        print("\n" + "=" * 70)
        print("LOGIN CREDENTIALS FOR TESTING")
        print("=" * 70)
        
        print("\n🔴 ADMIN USER:")
        print("  Email:    admin@datasyncai.com")
        print("  Password: Admin@123")
        print("  Role:     ADMIN (Full system access)")
        
        print("\n🟡 TENANT USERS:")
        print("  Email:    tenant@company.com")
        print("  Password: Tenant@123")
        print("  Role:     TENANT (Manages company.com domain)")
        print()
        print("  Email:    tenant@techcorp.io")
        print("  Password: Tenant@123")
        print("  Role:     TENANT (Manages techcorp.io domain)")
        
        print("\n🟢 REGULAR USERS:")
        print("  Email:    user1@company.com")
        print("  Password: User@123")
        print("  Role:     USER (Approved)")
        print()
        print("  Email:    user2@company.com")
        print("  Password: User@123")
        print("  Role:     USER (⏳ Pending - needs tenant approval)")
        print()
        print("  Email:    user1@techcorp.io")
        print("  Password: User@123")
        print("  Role:     USER (Approved)")
        print()
        print("  Email:    user2@techcorp.io")
        print("  Password: User@123")
        print("  Role:     USER (⏳ Pending - needs tenant approval)")
        
        print("\n" + "=" * 70)
        print("✓ Test users created successfully!")
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    create_test_users()
