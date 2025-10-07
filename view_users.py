"""
View All Users in SQL Fabric Database
This script shows all users in the login_details table
"""

import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def view_all_users():
    """View all users in SQL Fabric database"""
    
    # Get database credentials from environment
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    server = os.getenv('FABRIC_SERVER')
    database = os.getenv('FABRIC_DATABASE')
    
    print("=" * 90)
    print("SQL FABRIC DATABASE - ALL USERS")
    print("=" * 90)
    print(f"Server: {server}")
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
    
    try:
        # Connect to database
        print("Connecting to SQL Fabric...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✓ Connected successfully!\n")
        
        # Get all users
        cursor.execute("""
            SELECT 
                id, 
                email, 
                ISNULL(first_name, 'N/A') as first_name,
                ISNULL(last_name, 'N/A') as last_name,
                ISNULL(mobile, 'N/A') as mobile,
                ISNULL(role, 'user') as role,
                ISNULL(is_approved, 0) as is_approved,
                ISNULL(domain, 'N/A') as domain,
                ISNULL(CONVERT(VARCHAR, updated_at, 120), 'N/A') as updated_at
            FROM login_details
            ORDER BY id
        """)
        
        users = cursor.fetchall()
        
        if not users:
            print("⚠ NO USERS FOUND IN DATABASE!")
            print("Run 'create_admin_user.py' to create an admin user.")
        else:
            print(f"Total Users: {len(users)}\n")
            print("=" * 90)
            
            # Print header
            print(f"{'ID':<5} {'Email':<30} {'Name':<25} {'Role':<10} {'Approved':<10}")
            print("-" * 90)
            
            # Print users
            for user in users:
                user_id, email, first_name, last_name, mobile, role, is_approved, domain, updated_at = user
                name = f"{first_name} {last_name}"
                approved_str = "✓ YES" if is_approved else "✗ NO"
                
                print(f"{user_id:<5} {email:<30} {name:<25} {role:<10} {approved_str:<10}")
            
            print("=" * 90)
            
            # Show detailed info for each user
            print("\n\nDETAILED USER INFORMATION")
            print("=" * 90)
            
            for user in users:
                user_id, email, first_name, last_name, mobile, role, is_approved, domain, updated_at = user
                
                print(f"\nUser ID: {user_id}")
                print(f"  Email:        {email}")
                print(f"  Name:         {first_name} {last_name}")
                print(f"  Mobile:       {mobile}")
                print(f"  Role:         {role.upper()}")
                print(f"  Approved:     {'✓ YES' if is_approved else '✗ NO (Pending)'}")
                print(f"  Domain:       {domain}")
                print(f"  Updated:      {updated_at}")
                print("-" * 90)
            
            # Show statistics
            print("\n\nUSER STATISTICS")
            print("=" * 90)
            
            cursor.execute("SELECT ISNULL(role, 'user') as role, COUNT(*) as count FROM login_details GROUP BY ISNULL(role, 'user')")
            role_stats = cursor.fetchall()
            
            print("Users by Role:")
            for role, count in role_stats:
                print(f"  {role.upper():<10} : {count} users")
            
            cursor.execute("SELECT COUNT(*) FROM login_details WHERE is_approved = 1")
            approved = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM login_details WHERE ISNULL(is_approved, 0) = 0")
            pending = cursor.fetchone()[0]
            
            print(f"\nApproval Status:")
            print(f"  APPROVED : {approved} users")
            print(f"  PENDING  : {pending} users")
            
            print("=" * 90)
        
        cursor.close()
        conn.close()
        print("\n✓ Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    view_all_users()
