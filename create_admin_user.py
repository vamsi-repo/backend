"""
Create Admin User in SQL Fabric Database
This script adds an admin user to the login_details table
"""

import pyodbc
import bcrypt
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def create_admin_user():
    """Create admin user in SQL Fabric database"""
    
    # Get database credentials from environment
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    server = os.getenv('FABRIC_SERVER')
    database = os.getenv('FABRIC_DATABASE')
    
    print("=" * 70)
    print("Creating Admin User in SQL Fabric Database")
    print("=" * 70)
    print(f"\nConnecting to: {server}")
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
        
        # Admin user details
        admin_email = 'admin@datasyncai.com'
        admin_password = 'Admin@123'  # Strong password
        admin_first_name = 'System'
        admin_last_name = 'Administrator'
        admin_domain = 'datasyncai.com'
        
        # Check if admin already exists
        print(f"Checking if admin user exists: {admin_email}")
        cursor.execute("SELECT id, email, role FROM login_details WHERE email = ?", admin_email)
        existing_admin = cursor.fetchone()
        
        if existing_admin:
            print(f"⚠ Admin user already exists: {existing_admin[1]} (ID: {existing_admin[0]}, Role: {existing_admin[2]})")
            
            # Update to ensure admin role and approval
            print("\nUpdating existing admin user...")
            cursor.execute("""
                UPDATE login_details 
                SET role = 'admin', 
                    is_approved = 1,
                    updated_at = GETDATE()
                WHERE email = ?
            """, admin_email)
            conn.commit()
            print("✓ Admin user updated successfully!")
            
        else:
            # Hash password
            print("\nHashing password...")
            password_hash = bcrypt.hashpw(admin_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            print("✓ Password hashed")
            
            # Insert admin user
            print(f"\nInserting admin user: {admin_email}")
            cursor.execute("""
                INSERT INTO login_details 
                (email, password_hash, first_name, last_name, role, is_approved, domain, updated_at)
                VALUES (?, ?, ?, ?, 'admin', 1, ?, GETDATE())
            """, admin_email, password_hash, admin_first_name, admin_last_name, admin_domain)
            
            conn.commit()
            print("✓ Admin user created successfully!")
        
        # Verify admin user
        print("\nVerifying admin user...")
        cursor.execute("""
            SELECT id, email, first_name, last_name, role, is_approved, domain 
            FROM login_details 
            WHERE email = ?
        """, admin_email)
        
        admin = cursor.fetchone()
        
        if admin:
            print("\n" + "=" * 70)
            print("ADMIN USER DETAILS")
            print("=" * 70)
            print(f"ID:           {admin[0]}")
            print(f"Email:        {admin[1]}")
            print(f"Name:         {admin[2]} {admin[3]}")
            print(f"Role:         {admin[4]}")
            print(f"Approved:     {bool(admin[5])}")
            print(f"Domain:       {admin[6]}")
            print("=" * 70)
            print("\n✓ ADMIN USER READY TO USE!")
            print("\nLogin Credentials:")
            print(f"  Email:    {admin_email}")
            print(f"  Password: {admin_password}")
            print("=" * 70)
        
        # Show all users in database
        print("\n\nAll Users in Database:")
        print("-" * 70)
        cursor.execute("""
            SELECT id, email, ISNULL(first_name, 'N/A'), ISNULL(last_name, 'N/A'), 
                   ISNULL(role, 'user'), ISNULL(is_approved, 0), domain
            FROM login_details
            ORDER BY id
        """)
        
        all_users = cursor.fetchall()
        print(f"{'ID':<5} {'Email':<30} {'Name':<25} {'Role':<10} {'Approved':<10} {'Domain':<20}")
        print("-" * 70)
        
        for user in all_users:
            user_id, email, first_name, last_name, role, is_approved, domain = user
            name = f"{first_name} {last_name}"
            approved_str = "Yes" if is_approved else "No"
            print(f"{user_id:<5} {email:<30} {name:<25} {role:<10} {approved_str:<10} {domain or 'N/A':<20}")
        
        print(f"\nTotal Users: {len(all_users)}")
        print("=" * 70)
        
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
    create_admin_user()
