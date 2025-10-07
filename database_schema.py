"""
Database Schema Creation for Data Sync AI Enterprise
Creates all necessary tables in SQL Fabric
"""

def create_database_schema(fabric_service):
    """Create all necessary tables for Data Sync AI"""
    
    tables = [
        # Users table with multi-tenancy
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            email NVARCHAR(255) UNIQUE NOT NULL,
            password_hash NVARCHAR(255) NOT NULL,
            role NVARCHAR(20) NOT NULL CHECK (role IN ('admin', 'tenant', 'user')),
            name NVARCHAR(255),
            tenant_id NVARCHAR(36),
            status NVARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'pending')),
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            updated_at DATETIME2 DEFAULT GETUTCDATE()
        )
        """,
        
        # Templates for rule configuration
        """
        CREATE TABLE IF NOT EXISTS excel_templates (
            template_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            name NVARCHAR(255) NOT NULL,
            created_by NVARCHAR(36) NOT NULL,
            tenant_id NVARCHAR(36),
            column_count INT,
            file_size BIGINT,
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            updated_at DATETIME2 DEFAULT GETUTCDATE(),
            FOREIGN KEY (created_by) REFERENCES users(user_id)
        )
        """,
        
        # Template columns
        """
        CREATE TABLE IF NOT EXISTS template_columns (
            column_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            template_id NVARCHAR(36) NOT NULL,
            column_name NVARCHAR(255) NOT NULL,
            data_type NVARCHAR(50) NOT NULL,
            is_required BIT DEFAULT 0,
            column_order INT,
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE
        )
        """,
        
        # Validation rules
        """
        CREATE TABLE IF NOT EXISTS column_validation_rules (
            rule_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            column_id NVARCHAR(36) NOT NULL,
            rule_type NVARCHAR(50) NOT NULL,
            rule_value NVARCHAR(500),
            error_message NVARCHAR(500),
            is_active BIT DEFAULT 1,
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            FOREIGN KEY (column_id) REFERENCES template_columns(column_id) ON DELETE CASCADE
        )
        """,
        
        # Validation history
        """
        CREATE TABLE IF NOT EXISTS validation_history (
            validation_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            template_id NVARCHAR(36) NOT NULL,
            user_id NVARCHAR(36) NOT NULL,
            tenant_id NVARCHAR(36),
            original_filename NVARCHAR(500),
            total_rows INT,
            error_count INT DEFAULT 0,
            status NVARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
            lakehouse_original_path NVARCHAR(1000),
            lakehouse_corrected_path NVARCHAR(1000),
            processing_start DATETIME2,
            processing_end DATETIME2,
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            FOREIGN KEY (template_id) REFERENCES excel_templates(template_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
        """,
        
        # Validation errors and corrections
        """
        CREATE TABLE IF NOT EXISTS validation_corrections (
            correction_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
            validation_id NVARCHAR(36) NOT NULL,
            row_number INT NOT NULL,
            column_name NVARCHAR(255) NOT NULL,
            original_value NVARCHAR(MAX),
            corrected_value NVARCHAR(MAX),
            rule_type NVARCHAR(50),
            error_message NVARCHAR(500),
            correction_type NVARCHAR(20) DEFAULT 'auto' CHECK (correction_type IN ('auto', 'manual', 'suggested')),
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            FOREIGN KEY (validation_id) REFERENCES validation_history(validation_id) ON DELETE CASCADE
        )
        """
    ]
    
    # Create indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)",
        "CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_templates_user ON excel_templates(created_by)",
        "CREATE INDEX IF NOT EXISTS idx_validation_user ON validation_history(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_validation_template ON validation_history(template_id)",
        "CREATE INDEX IF NOT EXISTS idx_corrections_validation ON validation_corrections(validation_id)"
    ]
    
    try:
        # Create tables
        for table_sql in tables:
            fabric_service.execute_query(table_sql)
        
        # Create indexes
        for index_sql in indexes:
            try:
                fabric_service.execute_query(index_sql)
            except:
                pass  # Index might already exist
        
        # Insert default admin user
        admin_exists = fabric_service.execute_query(
            "SELECT COUNT(*) FROM users WHERE email = ?",
            ('admin@datasync.ai',),
            fetch='one'
        )[0]
        
        if admin_exists == 0:
            fabric_service.execute_query(
                """INSERT INTO users (email, password_hash, role, name, status) 
                   VALUES (?, ?, 'admin', 'System Administrator', 'active')""",
                ('admin@datasync.ai', 'hashed_admin123')  # In real app, use bcrypt
            )
        
        return True
    except Exception as e:
        print(f"Database schema creation error: {str(e)}")
        return False
