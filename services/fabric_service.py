"""
Enterprise SQL Fabric Service for Data Sync AI
Handles all database operations with your existing patterns but using Azure SQL Fabric
"""
import pyodbc
import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd
from functools import wraps
import time
from .app_redis import AppRedis

logger = logging.getLogger('data_sync_ai.fabric')

class CachedFabricService:
    """
    Enterprise SQL Fabric service following your existing MySQL patterns
    Enhanced with multi-tenancy and enterprise-level caching
    """
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, 
                 server: str, database: str, onelake_url: str, cache: AppRedis):
        """Initialize Fabric connection with enterprise configuration"""
        
        self.connection_string = (
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
        
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.onelake_url = onelake_url
        self.cache = cache
        self.connection_pool = []
        self._test_connection()
        
        logger.info(f"Fabric connection initialized for database: {database}")
    
    def _test_connection(self):
        """Test Fabric connection on initialization"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            logger.info("Fabric connection test successful")
        except Exception as e:
            logger.error(f"Fabric connection test failed: {str(e)}")
            raise
    
    def get_db_connection(self):
        """Get database connection (replacing your MySQL get_db_connection)"""
        try:
            conn = pyodbc.connect(self.connection_string)
            logger.debug("Fabric connection established")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Fabric: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: tuple = (), fetch: str = 'none') -> Any:
        """Execute query with enterprise error handling"""
        start_time = time.time()
        
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = None
                if fetch == 'one':
                    result = cursor.fetchone()
                elif fetch == 'all':
                    result = cursor.fetchall()
                elif fetch == 'many':
                    result = cursor.fetchmany(1000)  # Reasonable batch size
                
                conn.commit()
                
                execution_time = time.time() - start_time
                logger.debug(f"Query executed in {execution_time:.3f}s")
                
                return result
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def _try_ultra_fast_cached_select(self, cache_key: str, query: str, params: tuple = ()) -> Optional[List]:
        """Ultra-fast cached select following your pattern"""
        
        # Try cache first
        cached_result = self.cache.get_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for: {cache_key}")
            return cached_result
        
        # Execute query if not cached
        try:
            result = self.execute_query(query, params, fetch='all')
            
            # Convert to list of dicts for caching
            if result:
                columns = [desc[0] for desc in result.cursor.description] if hasattr(result, 'cursor') else []
                dict_result = []
                
                if hasattr(result, '__iter__'):
                    for row in result:
                        if hasattr(row, '_asdict'):
                            dict_result.append(row._asdict())
                        else:
                            dict_result.append(dict(zip(columns, row)))
                
                # Cache result
                self.cache.set_cache(cache_key, dict_result, ttl=3600)
                logger.debug(f"Cached result for: {cache_key}")
                return dict_result
            
            return []
            
        except Exception as e:
            logger.error(f"Ultra-fast cached select failed: {str(e)}")
            return None
    
    def bulk_insert_file_data_cached(self, table_name: str, data_list: List[Dict], 
                                   tenant_id: Optional[int] = None) -> bool:
        """
        ULTRA-FAST INSERT following your existing pattern
        Enhanced for SQL Fabric with enterprise logging
        """
        if not data_list:
            logger.warning("No data provided for bulk insert")
            return False
        
        start_time = time.time()
        batch_size = 1000
        total_inserted = 0
        
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get table schema
                cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (table_name,))
                columns = cursor.fetchall()
                
                if not columns:
                    raise Exception(f"Table {table_name} not found or has no columns")
                
                column_names = [col[0] for col in columns]
                placeholders = ','.join(['?' for _ in column_names])
                
                insert_query = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders})"
                
                # Process in batches
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i + batch_size]
                    
                    # Prepare batch data
                    batch_values = []
                    for record in batch:
                        row_values = []
                        for col_name in column_names:
                            value = record.get(col_name)
                            # Handle special cases
                            if value is None:
                                row_values.append(None)
                            elif col_name.lower() == 'tenant_id' and tenant_id:
                                row_values.append(tenant_id)
                            elif isinstance(value, (dict, list)):
                                row_values.append(json.dumps(value))
                            else:
                                row_values.append(value)
                        batch_values.append(tuple(row_values))
                    
                    # Execute batch insert
                    cursor.executemany(insert_query, batch_values)
                    total_inserted += len(batch_values)
                    
                    logger.debug(f"Inserted batch {i//batch_size + 1}: {len(batch_values)} records")
                
                conn.commit()
                
                execution_time = time.time() - start_time
                logger.info(f"ULTRA-FAST INSERT: {total_inserted} records to {table_name} in {execution_time:.3f}s")
                
                # Invalidate related caches
                self._invalidate_table_cache(table_name, tenant_id)
                
                return True
                
        except Exception as e:
            logger.error(f"Bulk insert failed for table {table_name}: {str(e)}")
            return False
    
    def _try_ultra_fast_pandas_insert(self, df: pd.DataFrame, table_name: str, 
                                    if_exists: str = 'append') -> bool:
        """Ultra-fast pandas insert to SQL Fabric"""
        start_time = time.time()
        
        try:
            # Use SQLAlchemy for pandas integration with SQL Fabric
            from sqlalchemy import create_engine
            from urllib.parse import quote_plus
            
            # Create SQLAlchemy connection string
            connection_string_encoded = quote_plus(self.connection_string)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string_encoded}")
            
            # Insert DataFrame
            rows_inserted = df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            execution_time = time.time() - start_time
            logger.info(f"ULTRA-FAST PANDAS INSERT: {len(df)} records to {table_name} in {execution_time:.3f}s")
            
            return True
            
        except Exception as e:
            logger.error(f"Pandas insert failed for table {table_name}: {str(e)}")
            return False
    
    def _invalidate_table_cache(self, table_name: str, tenant_id: Optional[int] = None):
        """Invalidate caches related to a table"""
        patterns = [f"*{table_name}*"]
        if tenant_id:
            patterns.append(f"*tenant:{tenant_id}*")
        
        for pattern in patterns:
            self.cache.delete_pattern(pattern)
        
        logger.debug(f"Invalidated cache for table: {table_name}")
    
    # Following your existing database methods pattern
    
    def init_database_schema(self):
        """Initialize database schema following your app.py pattern"""
        logger.info("Initializing database schema...")
        
        tables = [
            # Login details with multi-tenancy
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='login_details' AND xtype='U')
            CREATE TABLE login_details (
                id INT IDENTITY(1,1) PRIMARY KEY,
                first_name NVARCHAR(100),
                last_name NVARCHAR(100),
                email NVARCHAR(255) UNIQUE,
                mobile NVARCHAR(10),
                password NVARCHAR(255),
                role NVARCHAR(20) DEFAULT 'USER' CHECK (role IN ('ADMIN', 'TENANT', 'USER')),
                domain NVARCHAR(255),
                tenant_id INT,
                approved_by INT,
                status NVARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
                created_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (tenant_id) REFERENCES login_details(id),
                FOREIGN KEY (approved_by) REFERENCES login_details(id)
            )
            """,
            
            # Excel templates with Fabric enhancements
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='excel_templates' AND xtype='U')
            CREATE TABLE excel_templates (
                template_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                template_name NVARCHAR(255) NOT NULL,
                created_at DATETIME2 DEFAULT GETDATE(),
                updated_at DATETIME2 DEFAULT GETDATE(),
                user_id INT NOT NULL,
                tenant_id INT,
                sheet_name NVARCHAR(255),
                headers NVARCHAR(MAX), -- JSON
                duckdb_schema NVARCHAR(MAX), -- JSON for DuckDB table schema
                lakehouse_original_path NVARCHAR(512),
                lakehouse_processed_path NVARCHAR(512),
                status NVARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'PROCESSING')),
                is_corrected BIT DEFAULT 0,
                validation_frequency NVARCHAR(20) CHECK (validation_frequency IN ('WEEKLY', 'MONTHLY', 'YEARLY')),
                remote_file_path NVARCHAR(512),
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE,
                FOREIGN KEY (tenant_id) REFERENCES login_details(id)
            )
            """,
            
            # Template columns
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='template_columns' AND xtype='U')
            CREATE TABLE template_columns (
                column_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                template_id BIGINT NOT NULL,
                column_name NVARCHAR(255) NOT NULL,
                column_position INT NOT NULL,
                data_type NVARCHAR(50),
                is_validation_enabled BIT DEFAULT 0,
                is_selected BIT DEFAULT 0,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                UNIQUE (template_id, column_name)
            )
            """,
            
            # Validation rule types
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='validation_rule_types' AND xtype='U')
            CREATE TABLE validation_rule_types (
                rule_type_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                rule_name NVARCHAR(255) NOT NULL,
                description NVARCHAR(MAX),
                parameters NVARCHAR(MAX),
                is_active BIT DEFAULT 1,
                is_custom BIT DEFAULT 0,
                created_by_user_id INT,
                tenant_id INT,
                column_name NVARCHAR(255),
                template_id BIGINT,
                created_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                FOREIGN KEY (created_by_user_id) REFERENCES login_details(id),
                FOREIGN KEY (tenant_id) REFERENCES login_details(id)
            )
            """,
            
            # Column validation rules
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='column_validation_rules' AND xtype='U')
            CREATE TABLE column_validation_rules (
                column_validation_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                column_id BIGINT NOT NULL,
                rule_type_id BIGINT NOT NULL,
                rule_config NVARCHAR(MAX), -- JSON
                created_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (column_id) REFERENCES template_columns(column_id) ON DELETE CASCADE,
                FOREIGN KEY (rule_type_id) REFERENCES validation_rule_types(rule_type_id),
                UNIQUE (column_id, rule_type_id)
            )
            """,
            
            # Validation history with Lakehouse paths
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='validation_history' AND xtype='U')
            CREATE TABLE validation_history (
                history_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                template_id BIGINT NOT NULL,
                template_name NVARCHAR(255) NOT NULL,
                error_count INT NOT NULL,
                corrected_at DATETIME2 DEFAULT GETDATE(),
                original_lakehouse_path NVARCHAR(512),
                corrected_lakehouse_path NVARCHAR(512),
                processing_metadata NVARCHAR(MAX), -- JSON
                user_id INT NOT NULL,
                tenant_id INT,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE,
                FOREIGN KEY (tenant_id) REFERENCES login_details(id)
            )
            """,
            
            # Validation corrections
            """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='validation_corrections' AND xtype='U')
            CREATE TABLE validation_corrections (
                correction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                history_id BIGINT NOT NULL,
                row_index INT NOT NULL,
                column_name NVARCHAR(255) NOT NULL,
                original_value NVARCHAR(MAX),
                corrected_value NVARCHAR(MAX),
                rule_failed NVARCHAR(255),
                confidence_score DECIMAL(3,2) DEFAULT 1.00,
                FOREIGN KEY (history_id) REFERENCES validation_history(history_id) ON DELETE CASCADE
            )
            """
        ]
        
        try:
            for table_sql in tables:
                self.execute_query(table_sql)
                
            logger.info("Database schema initialization completed successfully")
            
            # Create default admin user
            self._create_default_admin()
            
            # Create default validation rules
            self._create_default_validation_rules()
            
        except Exception as e:
            logger.error(f"Database schema initialization failed: {str(e)}")
            raise
    
    def _create_default_admin(self):
        """Create default admin user"""
        import bcrypt
        
        try:
            # Check if admin exists
            existing_admin = self.execute_query(
                "SELECT id FROM login_details WHERE email = ?",
                ('admin@datasyncai.com',),
                fetch='one'
            )
            
            if not existing_admin:
                # Create admin user
                admin_password = bcrypt.hashpw('DataSyncAI@2024'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                
                self.execute_query("""
                    INSERT INTO login_details (first_name, last_name, email, mobile, password, role, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    'System', 'Administrator', 'admin@datasyncai.com', '0000000000',
                    admin_password, 'ADMIN', 'APPROVED'
                ))
                
                logger.info("Default admin user created successfully")
            else:
                logger.info("Admin user already exists")
                
        except Exception as e:
            logger.error(f"Failed to create default admin user: {str(e)}")
    
    def _create_default_validation_rules(self):
        """Create default validation rules following your pattern"""
        try:
            # Clear existing default rules
            self.execute_query("DELETE FROM validation_rule_types WHERE is_custom = 0")
            
            default_rules = [
                ("Required", "Ensures the field is not null", '{"allow_null": false}'),
                ("Int", "Validates integer format", '{"format": "integer"}'),
                ("Float", "Validates number format (integer or decimal)", '{"format": "float"}'),
                ("Text", "Allows text with quotes and parentheses", '{"allow_special": false}'),
                ("Email", "Validates email format", '{"regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"}'),
                ("Date", "Validates date format", '{"format": "%d-%m-%Y"}'),
                ("Boolean", "Validates boolean format (true/false or 0/1)", '{"format": "boolean"}'),
                ("Alphanumeric", "Validates alphanumeric format", '{"format": "alphanumeric"}')
            ]
            
            for rule_name, description, parameters in default_rules:
                self.execute_query("""
                    INSERT INTO validation_rule_types (rule_name, description, parameters, is_custom)
                    VALUES (?, ?, ?, ?)
                """, (rule_name, description, parameters, 0))
            
            logger.info("Default validation rules created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create default validation rules: {str(e)}")
            raise

def with_db_connection(func):
    """Decorator to handle database connections"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.error(f"Database operation failed in {func.__name__}: {str(e)}")
            raise
    return wrapper
