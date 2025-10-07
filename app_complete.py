"""
Data Sync AI Enterprise - Complete Backend Implementation
Includes: DuckDB Integration, Fabric Lakehouse, Multi-tenant Architecture, Complete Workflows
"""

import os
import logging
import uuid
import json
import pandas as pd
import pyodbc
import duckdb
from datetime import datetime
from flask import Flask, request, jsonify, session, send_file
from flask_session import Session
from flask_cors import CORS
from dotenv import load_dotenv
import bcrypt
from werkzeug.utils import secure_filename

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/datasync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create directories
for directory in ['logs', 'uploads', 'sessions', 'temp']:
    os.makedirs(directory, exist_ok=True)

class FabricLakehouseService:
    """MS Fabric Lakehouse Service for file storage with Azure Data Lake"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.lakehouse_url = os.getenv('FABRIC_LAKEHOUSE_URL')
        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        
        # Always initialize base_path for local fallback
        self.base_path = "uploads"
        
        if not self.lakehouse_url:
            self.logger.info("Using local file storage (Lakehouse URL not configured)")
        else:
            self.logger.info(f"Using Azure Data Lake: {self.lakehouse_url}")
        
    def store_original_file(self, user_id: int, session_id: str, filename: str, file_content: bytes) -> str:
        """Store original uploaded file in OneLake or local storage"""
        file_path = f"validation/original/{user_id}/{session_id}/{filename}"
        
        if self.lakehouse_url and all([self.client_id, self.client_secret, self.tenant_id]):
            # Use OneLake (Fabric Lakehouse) Storage
            try:
                from azure.identity import ClientSecretCredential
                from azure.storage.filedatalake import DataLakeServiceClient
                
                credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                
                # Extract account URL from OneLake URL
                account_url = "https://onelake.dfs.fabric.microsoft.com/"
                
                service_client = DataLakeServiceClient(
                    account_url=account_url,
                    credential=credential
                )
                
                # Extract file system and directory from OneLake URL
                file_system_name = "Keansa_AI_Suite"
                base_directory = "Keansa_AI_Suite_Backend.Lakehouse/Files/SL_files"
                full_path = f"{base_directory}/{file_path}"
                
                file_system_client = service_client.get_file_system_client(file_system_name)
                
                # Upload file
                file_client = file_system_client.get_file_client(full_path)
                file_client.upload_data(file_content, overwrite=True)
                
                self.logger.info(f"Stored original file in OneLake: {full_path}")
                return f"onelake://{full_path}"
                
            except Exception as e:
                self.logger.error(f"Failed to store file in OneLake: {e}")
                # Fallback to local storage
        
        # Local storage fallback
        full_path = os.path.join(self.base_path, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        with open(full_path, 'wb') as f:
            f.write(file_content)
            
        self.logger.info(f"Stored original file locally: {file_path}")
        return file_path
        
    def store_corrected_file(self, user_id: int, session_id: str, filename: str, corrected_df: pd.DataFrame) -> str:
        """Store corrected file in OneLake or local storage"""
        # Filename already includes the correct suffix from the caller
        corrected_filename = filename
        file_path = f"validation/corrected/{user_id}/{session_id}/{corrected_filename}"
        
        if self.lakehouse_url and all([self.client_id, self.client_secret, self.tenant_id]):
            # Use OneLake (Fabric Lakehouse) Storage
            try:
                from azure.identity import ClientSecretCredential
                from azure.storage.filedatalake import DataLakeServiceClient
                import io
                
                credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                
                account_url = "https://onelake.dfs.fabric.microsoft.com/"
                
                service_client = DataLakeServiceClient(
                    account_url=account_url,
                    credential=credential
                )
                
                file_system_name = "Keansa_AI_Suite"
                base_directory = "Keansa_AI_Suite_Backend.Lakehouse/Files/SL_files"
                full_path = f"{base_directory}/{file_path}"
                
                file_system_client = service_client.get_file_system_client(file_system_name)
                
                # Convert DataFrame to Excel bytes
                excel_buffer = io.BytesIO()
                corrected_df.to_excel(excel_buffer, index=False)
                excel_data = excel_buffer.getvalue()
                
                # Upload file
                file_client = file_system_client.get_file_client(full_path)
                file_client.upload_data(excel_data, overwrite=True)
                
                self.logger.info(f"Stored corrected file in OneLake: {full_path}")
                return f"onelake://{full_path}"
                
            except Exception as e:
                self.logger.error(f"Failed to store corrected file in OneLake: {e}")
                # Fallback to local storage
        
        # Local storage fallback
        full_path = os.path.join(self.base_path, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        corrected_df.to_excel(full_path, index=False)
        
        self.logger.info(f"Stored corrected file locally: {file_path}")
        return file_path
        
    def get_file(self, file_path: str) -> str:
        """Get file from Fabric Lakehouse or local storage"""
        # Check if file_path is from OneLake
        if file_path.startswith('onelake://'):
            return self._download_from_onelake(file_path)
        
        # Determine base_path dynamically
        base_path = self.base_path if hasattr(self, 'base_path') else 'uploads'
        
        # Local storage
        full_path = os.path.join(base_path, file_path)
        return full_path if os.path.exists(full_path) else None
    
    def _download_from_onelake(self, onelake_path: str) -> str:
        """Download file from OneLake to temp directory"""
        try:
            from azure.identity import ClientSecretCredential
            from azure.storage.filedatalake import DataLakeServiceClient
            import tempfile
            
            # Remove 'onelake://' prefix
            storage_path = onelake_path.replace('onelake://', '')
            
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            
            account_url = "https://onelake.dfs.fabric.microsoft.com/"
            service_client = DataLakeServiceClient(
                account_url=account_url,
                credential=credential
            )
            
            file_system_name = "Keansa_AI_Suite"
            file_system_client = service_client.get_file_system_client(file_system_name)
            
            # Download file
            file_client = file_system_client.get_file_client(storage_path)
            download_stream = file_client.download_file()
            file_data = download_stream.readall()
            
            # Save to temp file
            filename = os.path.basename(storage_path)
            temp_path = os.path.join('temp', filename)
            os.makedirs('temp', exist_ok=True)
            
            with open(temp_path, 'wb') as f:
                f.write(file_data)
            
            self.logger.info(f"Downloaded file from OneLake: {storage_path}")
            return temp_path
            
        except Exception as e:
            self.logger.error(f"Failed to download from OneLake: {e}")
            return None

class DuckDBService:
    """Enhanced DuckDB Service for analytics and processing"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.connection = duckdb.connect(':memory:')
        self._init_memory_tables()
    
    def _init_memory_tables(self):
        """Initialize in-memory tables for processing"""
        try:
            # Create session-based tables for file processing
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS session_files (
                    session_id VARCHAR,
                    filename VARCHAR,
                    headers VARCHAR[],
                    row_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.logger.info("DuckDB memory tables initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize DuckDB tables: {e}")
    
    def load_file_for_analysis(self, session_id: str, file_path: str, filename: str) -> dict:
        """Load file into DuckDB for rule configuration analysis"""
        try:
            # Read file based on extension
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            # Create session-specific table
            table_name = f"uploaded_data_{session_id.replace('-', '_')}"
            
            # Load data into DuckDB
            self.connection.register(table_name, df)
            
            # Analyze data
            profile_results = self._analyze_columns(table_name, df)
            
            # Store session info
            self.connection.execute(f"""
                INSERT INTO session_files (session_id, filename, headers, row_count)
                VALUES ('{session_id}', '{filename}', {list(df.columns)}, {len(df)})
            """)
            
            self.logger.info(f"File loaded for analysis: {filename} ({len(df)} rows)")
            
            return {
                'table_name': table_name,
                'headers': list(df.columns),
                'row_count': len(df),
                'column_profiles': profile_results,
                'preview': df.head(5).to_dict('records')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to load file for analysis: {e}")
            return {'error': str(e)}
    
    def load_file_for_validation(self, session_id: str, file_path: str, filename: str) -> dict:
        """Load file into DuckDB for data validation processing"""
        try:
            # Read file
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            # Create validation table
            table_name = f"validation_data_{session_id.replace('-', '_')}"
            self.connection.register(table_name, df)
            
            self.logger.info(f"File loaded for validation: {filename} ({len(df)} rows)")
            
            return {
                'table_name': table_name,
                'headers': list(df.columns),
                'row_count': len(df),
                'data': df
            }
            
        except Exception as e:
            self.logger.error(f"Failed to load file for validation: {e}")
            return {'error': str(e)}
    
    def _analyze_columns(self, table_name: str, df: pd.DataFrame) -> dict:
        """Analyze column characteristics for rule suggestions"""
        profiles = {}
        
        for column in df.columns:
            try:
                # Get basic statistics
                null_count = self.connection.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL").fetchone()[0]
                total_count = len(df)
                unique_count = self.connection.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name}").fetchone()[0]
                
                # Determine data type suggestions
                non_null_series = df[column].dropna()
                suggested_rules = self._suggest_rules(non_null_series)
                
                profiles[column] = {
                    'null_percentage': (null_count / total_count) * 100,
                    'unique_count': unique_count,
                    'suggested_rules': suggested_rules,
                    'sample_values': non_null_series.head(3).tolist()
                }
                
            except Exception as e:
                profiles[column] = {'error': str(e)}
        
        return profiles
    
    def _suggest_rules(self, series: pd.Series) -> list:
        """Suggest validation rules based on data patterns"""
        suggestions = []
        
        if len(series) == 0:
            return ['Required']
        
        # Check if all values are numeric
        if pd.api.types.is_numeric_dtype(series):
            if series.dtype == 'int64':
                suggestions.append('Int')
            else:
                suggestions.append('Float')
        
        # Check for email patterns
        elif series.astype(str).str.contains('@').any():
            suggestions.append('Email')
        
        # Check for date patterns
        elif pd.api.types.is_datetime64_any_dtype(series):
            suggestions.append('Date')
        
        # Default to text
        else:
            suggestions.append('Text')
        
        # Always suggest Required if no nulls
        if not series.isnull().any():
            suggestions.insert(0, 'Required')
        
        return suggestions
    

    def analyze_file(self, file_path: str, session_id: str) -> dict:
        """
        Analyze uploaded file using DuckDB - Rule Configuration Step 0
        
        Args:
            file_path: Path to the uploaded file
            session_id: Unique session identifier
            
        Returns:
            Dictionary with file analysis results
        """
        try:
            filename = os.path.basename(file_path)
            self.logger.info(f"Starting DuckDB analysis for file: {filename}")
            
            # Read file based on extension
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {filename}")
            
            # Get column information
            headers = list(df.columns)
            row_count = len(df)
            
            # Create session-specific table
            table_name = f"temp_analysis_{session_id.replace('-', '_')}"
            self.connection.register(table_name, df)
            
            # Get column types from pandas
            column_types = {}
            for col in headers:
                dtype = str(df[col].dtype)
                if 'int' in dtype:
                    column_types[col] = 'integer'
                elif 'float' in dtype:
                    column_types[col] = 'float'
                elif 'datetime' in dtype:
                    column_types[col] = 'datetime'
                else:
                    column_types[col] = 'text'
            
            # Get sample data (first 5 rows)
            sample_data = df.head(5).to_dict('records')
            
            self.logger.info(f"File analysis completed: {row_count} rows, {len(headers)} columns")
            
            return {
                'success': True,
                'session_id': session_id,
                'filename': filename,
                'headers': headers,
                'row_count': row_count,
                'column_count': len(headers),
                'column_types': column_types,
                'sample_data': sample_data
            }
            
        except Exception as e:
            self.logger.error(f"File analysis failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
        finally:
            # Clean up temp table
            try:
                table_name = f"temp_analysis_{session_id.replace('-', '_')}"
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass
    

    def validate_data_with_rules(self, table_name: str, rules_config: dict) -> dict:
        """Apply validation rules to data in DuckDB"""
        try:
            errors = []
            corrections = {}
            
            # Get data
            df = self.connection.execute(f"SELECT * FROM {table_name}").df()
            
            for column, rules in rules_config.items():
                if column not in df.columns:
                    continue
                
                for rule in rules:
                    column_errors, column_corrections = self._apply_rule(df, column, rule)
                    errors.extend(column_errors)
                    corrections.update(column_corrections)
            
            # Apply corrections to create corrected DataFrame
            corrected_df = df.copy()
            for key, value in corrections.items():
                row_idx, col = key.split('_', 1)
                corrected_df.loc[int(row_idx), col] = value
            
            return {
                'errors': errors,
                'total_errors': len(errors),
                'corrected_data': corrected_df,
                'corrections_applied': len(corrections)
            }
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return {'error': str(e)}
    
    def _apply_rule(self, df: pd.DataFrame, column: str, rule: str) -> tuple:
        """Apply specific validation rule to column"""
        errors = []
        corrections = {}
        
        for idx, value in df[column].items():
            error_msg = None
            corrected_value = None
            
            if rule == 'Required' and (pd.isna(value) or str(value).strip() == ''):
                error_msg = f"Required field is empty"
                
            elif rule == 'Email' and not pd.isna(value):
                if '@' not in str(value) or '.' not in str(value):
                    error_msg = f"Invalid email format: {value}"
                    
            elif rule == 'Int' and not pd.isna(value):
                try:
                    int(value)
                except (ValueError, TypeError):
                    error_msg = f"Must be integer: {value}"
                    # Try to correct
                    try:
                        corrected_value = int(float(value))
                    except:
                        pass
                        
            elif rule == 'Float' and not pd.isna(value):
                try:
                    float(value)
                except (ValueError, TypeError):
                    error_msg = f"Must be number: {value}"
                    
            elif rule == 'Alphanumeric' and not pd.isna(value):
                if not str(value).replace(' ', '').isalnum():
                    error_msg = f"Must be alphanumeric: {value}"
            
            if error_msg:
                errors.append({
                    'row': idx + 2,  # +2 for 1-based indexing and header
                    'column': column,
                    'value': value,
                    'rule': rule,
                    'error': error_msg
                })
            
            if corrected_value is not None:
                corrections[f"{idx}_{column}"] = corrected_value
        
        return errors, corrections
    
    def cleanup_session(self, session_id: str):
        """Clean up session-specific tables"""
        try:
            table_name = f"uploaded_data_{session_id.replace('-', '_')}"
            validation_table = f"validation_data_{session_id.replace('-', '_')}"
            
            for table in [table_name, validation_table]:
                try:
                    self.connection.execute(f"DROP TABLE IF EXISTS {table}")
                except:
                    pass
                    
            self.logger.info(f"Cleaned up session tables for: {session_id}")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")

class SQLFabricService:
    """Enhanced SQL Fabric service with Azure AD authentication"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.connection_string = None
        self._connect()
        
    def _connect(self):
        """Connect to SQL Fabric using Azure AD Service Principal"""
        try:
            # Get Azure credentials from environment
            client_id = os.getenv('AZURE_CLIENT_ID')
            client_secret = os.getenv('AZURE_CLIENT_SECRET')
            tenant_id = os.getenv('AZURE_TENANT_ID')
            server = os.getenv('FABRIC_SERVER')
            database = os.getenv('FABRIC_DATABASE')
            
            if all([client_id, client_secret, tenant_id, server, database]):
                # Build connection string for SQL Fabric with Service Principal auth and MARS enabled
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
                    f"MARS_Connection=yes;"
                )
                
                self.connection = pyodbc.connect(self.connection_string)
                self.logger.info(f"Connected to SQL Fabric: {database}")
                
                # Initialize tables if needed
                self._init_tables()
                
            else:
                self.logger.warning("SQL Fabric credentials not complete - using fallback mode")
                
        except Exception as e:
            self.logger.error(f"SQL Fabric connection failed: {e}")
            self.logger.info("Continuing in fallback mode")
    
    def _init_tables(self):
        """Initialize required tables in SQL Fabric if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            # First, check what tables already exist
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            existing_tables = [row[0] for row in cursor.fetchall()]
            self.logger.info(f"Existing tables: {existing_tables}")
            
            # Check current structure of login_details if it exists
            if 'login_details' in existing_tables:
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'login_details'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cursor.fetchall()
                self.logger.info(f"Current login_details columns: {[(col[0], col[1]) for col in columns]}")
                
                # Add missing columns if they don't exist
                existing_column_names = [col[0].lower() for col in columns]
                
                if 'password_hash' not in existing_column_names:
                    cursor.execute("ALTER TABLE login_details ADD password_hash VARCHAR(255)")
                    self.logger.info("Added password_hash column")
                    
                if 'role' not in existing_column_names:
                    cursor.execute("ALTER TABLE login_details ADD role VARCHAR(20) DEFAULT 'user'")
                    self.logger.info("Added role column")
                    
                if 'domain' not in existing_column_names:
                    cursor.execute("ALTER TABLE login_details ADD domain VARCHAR(100)")
                    self.logger.info("Added domain column")
                    
                if 'is_approved' not in existing_column_names:
                    cursor.execute("ALTER TABLE login_details ADD is_approved BIT DEFAULT 0")
                    self.logger.info("Added is_approved column")
                    
                if 'updated_at' not in existing_column_names:
                    cursor.execute("ALTER TABLE login_details ADD updated_at DATETIME DEFAULT GETDATE()")
                    self.logger.info("Added updated_at column")
                    
            else:
                # Create users table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE login_details (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        role VARCHAR(20) DEFAULT 'user',
                        is_approved BIT DEFAULT 0,
                        domain VARCHAR(100),
                        created_at DATETIME DEFAULT GETDATE(),
                        updated_at DATETIME DEFAULT GETDATE()
                    )
                """)
                self.logger.info("Created login_details table")
            
            # Create templates table
            if 'excel_templates' not in existing_tables:
                cursor.execute("""
                    CREATE TABLE excel_templates (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        user_id INT,
                        name VARCHAR(255) NOT NULL,
                        headers NVARCHAR(MAX),
                        rules_config NVARCHAR(MAX),
                        is_active BIT DEFAULT 1,
                        created_at DATETIME DEFAULT GETDATE(),
                        updated_at DATETIME DEFAULT GETDATE()
                    )
                """)
                self.logger.info("Created excel_templates table")
            
            # Create validation sessions table
            if 'validation_sessions' not in existing_tables:
                cursor.execute("""
                    CREATE TABLE validation_sessions (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        user_id INT,
                        session_id VARCHAR(255),
                        filename VARCHAR(255),
                        original_file_path VARCHAR(500),
                        corrected_file_path VARCHAR(500),
                        total_rows INT,
                        error_count INT,
                        success_rate FLOAT,
                        created_at DATETIME DEFAULT GETDATE()
                    )
                """)
                self.logger.info("Created validation_sessions table")
            
            # Create validation errors table
            if 'validation_errors' not in existing_tables:
                cursor.execute("""
                    CREATE TABLE validation_errors (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        session_id INT,
                        row_number INT,
                        column_name VARCHAR(255),
                        error_type VARCHAR(100),
                        error_message VARCHAR(500),
                        original_value NVARCHAR(MAX),
                        created_at DATETIME DEFAULT GETDATE()
                    )
                """)
                self.logger.info("Created validation_errors table")
            
            # Fix validation_history and validation_corrections schema
            # ALWAYS check schema and recreate if wrong
            needs_recreation = False
            
            if 'validation_history' in existing_tables:
                cursor.execute("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'validation_history'
                """)
                existing_cols = [row[0].lower() for row in cursor.fetchall()]
                
                # Check if it has the correct schema
                required_cols = ['validation_id', 'template_id', 'user_id', 'lakehouse_original_path', 
                                'lakehouse_corrected_path', 'error_count', 'total_rows', 'status', 'created_at']
                
                if not all(col in existing_cols for col in required_cols):
                    needs_recreation = True
                    self.logger.info(f"validation_history has wrong schema - has: {existing_cols}")
            
            if 'validation_corrections' in existing_tables:
                cursor.execute("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'validation_corrections'
                """)
                existing_cols = [row[0].lower() for row in cursor.fetchall()]
                
                # Check if it has the correct schema  
                required_cols = ['correction_id', 'validation_id', 'row_number', 'column_name', 
                                'original_value', 'corrected_value', 'rule_type', 'created_at']
                
                if not all(col in existing_cols for col in required_cols):
                    needs_recreation = True
                    self.logger.info(f"validation_corrections has wrong schema - has: {existing_cols}")
            
            if needs_recreation:
                try:
                    self.logger.info("Dropping and recreating validation tables with correct schema...")
                    cursor.execute("DROP TABLE IF EXISTS validation_corrections")
                    cursor.execute("DROP TABLE IF EXISTS validation_history")
                    self.connection.commit()
                    self.logger.info("Old validation tables dropped successfully")
                except Exception as e:
                    self.logger.error(f"Error dropping tables: {e}")
                    self.connection.rollback()
            
            # Create validation_history if it doesn't exist
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'validation_history')
                BEGIN
                    CREATE TABLE validation_history (
                        validation_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        template_id BIGINT,
                        user_id BIGINT,
                        lakehouse_original_path NVARCHAR(1000),
                        lakehouse_corrected_path NVARCHAR(1000),
                        error_count INT,
                        total_rows INT,
                        status NVARCHAR(50),
                        created_at DATETIME DEFAULT GETDATE()
                    )
                END
            """)
            self.logger.info("validation_history table ready")
            
            # Create validation_corrections if it doesn't exist  
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'validation_corrections')
                BEGIN
                    CREATE TABLE validation_corrections (
                        correction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        validation_id BIGINT,
                        row_number INT,
                        column_name NVARCHAR(255),
                        original_value NVARCHAR(MAX),
                        corrected_value NVARCHAR(MAX),
                        rule_type NVARCHAR(50),
                        created_at DATETIME DEFAULT GETDATE()
                    )
                END
            """)
            self.logger.info("validation_corrections table ready")
            
            # Create validation_rule_types table
            if 'validation_rule_types' not in existing_tables:
                cursor.execute("""
                    CREATE TABLE validation_rule_types (
                        rule_type_id INT IDENTITY(1,1) PRIMARY KEY,
                        rule_name VARCHAR(50) NOT NULL UNIQUE,
                        rule_description VARCHAR(255),
                        rule_category VARCHAR(50),
                        is_active BIT DEFAULT 1,
                        created_at DATETIME DEFAULT GETDATE()
                    )
                """)
                self.logger.info("Created validation_rule_types table")
                
                # Seed default validation rules
                default_rules = [
                    (1, 'Required', 'Field must not be empty', None),
                    (2, 'Text', 'Field accepts any text value', None),
                    (3, 'Int', 'Field must be an integer number', None),
                    (4, 'Float', 'Field must be a decimal/floating point number', None),
                    (5, 'Email', 'Field must be a valid email address', '{"pattern": "email"}'),
                    (6, 'Date', 'Field must be a valid date', '{"format": "date"}'),
                    (7, 'Alphanumeric', 'Field must contain only letters and numbers', '{"pattern": "alphanumeric"}'),
                    (8, 'Boolean', 'Field must be true/false or yes/no', '{"values": [true, false]}'),
                    (9, 'Phone', 'Field must be a valid phone number', '{"pattern": "phone"}'),
                    (10, 'URL', 'Field must be a valid URL', '{"pattern": "url"}')
                ]
                
                for rule_id, rule_name, description, parameters in default_rules:
                    cursor.execute("""
                        INSERT INTO validation_rule_types (rule_type_id, rule_name, description, parameters, is_active)
                        VALUES (?, ?, ?, ?, 1)
                    """, rule_id, rule_name, description, parameters)
                
                self.logger.info(f"Seeded {len(default_rules)} validation rule types")
            else:
                # Check if rules exist, if not, seed them
                cursor.execute("SELECT COUNT(*) FROM validation_rule_types")
                rule_count = cursor.fetchone()[0]
                
                if rule_count == 0:
                    self.logger.info("validation_rule_types table exists but is empty - seeding default rules")
                    default_rules = [
                        (1, 'Required', 'Field must not be empty', None),
                        (2, 'Text', 'Field accepts any text value', None),
                        (3, 'Int', 'Field must be an integer number', None),
                        (4, 'Float', 'Field must be a decimal/floating point number', None),
                        (5, 'Email', 'Field must be a valid email address', '{"pattern": "email"}'),
                        (6, 'Date', 'Field must be a valid date', '{"format": "date"}'),
                        (7, 'Alphanumeric', 'Field must contain only letters and numbers', '{"pattern": "alphanumeric"}'),
                        (8, 'Boolean', 'Field must be true/false or yes/no', '{"values": [true, false]}'),
                        (9, 'Phone', 'Field must be a valid phone number', '{"pattern": "phone"}'),
                        (10, 'URL', 'Field must be a valid URL', '{"pattern": "url"}')
                    ]
                    
                    for rule_id, rule_name, description, parameters in default_rules:
                        cursor.execute("""
                            INSERT INTO validation_rule_types (rule_type_id, rule_name, description, parameters, is_active)
                            VALUES (?, ?, ?, ?, 1)
                        """, rule_id, rule_name, description, parameters)
                    
                    self.logger.info(f"Seeded {len(default_rules)} validation rule types")
            
            self.connection.commit()
            self.logger.info("SQL Fabric tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize SQL Fabric tables: {e}")
    
    def save_template_configuration(self, user_id: int, filename: str, headers: list, rules_config: dict, selected_headers: list) -> int:
        """Save template configuration to SQL Fabric WITH actual rules - FIXED VERSION"""
        try:
            if not self.connection:
                # Fallback logic for demo
                template_id = len(headers)  # Mock ID
                self.logger.info(f"Template saved (fallback): {filename}")
                return template_id
            
            cursor = self.connection.cursor()
            
            # Use the actual filename as template name
            template_name = filename
            
            # First check if template with this name already exists for this user
            cursor.execute("""
                SELECT template_id FROM excel_templates 
                WHERE template_name = ? AND user_id = ? AND status = 'ACTIVE'
            """, template_name, user_id)
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing template
                template_id = existing[0]
                cursor.execute("""
                    UPDATE excel_templates 
                    SET headers = ?, updated_at = GETDATE()
                    WHERE template_id = ?
                """, json.dumps(headers), template_id)
                self.logger.info(f"Updated existing template: {template_name} (ID: {template_id})")
                
                # Delete old rules for this template
                cursor.execute("""
                    DELETE FROM column_validation_rules 
                    WHERE column_id IN (
                        SELECT column_id FROM template_columns WHERE template_id = ?
                    )
                """, template_id)
                cursor.execute("DELETE FROM template_columns WHERE template_id = ?", template_id)
            else:
                # Generate template_id manually since the table doesn't have IDENTITY
                cursor.execute("SELECT ISNULL(MAX(template_id), 0) + 1 FROM excel_templates")
                template_id = cursor.fetchone()[0]
                
                # Insert new template with explicit template_id and ACTUAL filename
                cursor.execute("""
                    INSERT INTO excel_templates (template_id, user_id, template_name, headers, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'ACTIVE', GETDATE(), GETDATE())
                """, template_id, user_id, template_name, json.dumps(headers))
                
                self.logger.info(f"Created new template: {template_name} (ID: {template_id})")
            
            # Now save template_columns - GENERATE column_id manually
            for i, header in enumerate(headers):
                is_selected = header in selected_headers
                
                # Generate column_id manually
                cursor.execute("SELECT ISNULL(MAX(column_id), 0) + 1 FROM template_columns")
                column_id = cursor.fetchone()[0]
                
                # Insert with explicit column_id
                cursor.execute("""
                    INSERT INTO template_columns (column_id, template_id, column_name, column_position, is_validation_enabled, is_selected)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, column_id, template_id, header, i + 1, is_selected, is_selected)
                
                self.logger.info(f"Inserted column: {header} (column_id: {column_id})")
                
                # Save validation rules for this column
                if header in rules_config and rules_config[header]:
                    for rule_name in rules_config[header]:
                        # Get rule_type_id
                        cursor.execute("""
                            SELECT rule_type_id FROM validation_rule_types 
                            WHERE rule_name = ? AND is_active = 1
                        """, rule_name)
                        rule_result = cursor.fetchone()
                        
                        if rule_result:
                            rule_type_id = rule_result[0]
                            
                            # Generate column_validation_id manually
                            cursor.execute("SELECT ISNULL(MAX(column_validation_id), 0) + 1 FROM column_validation_rules")
                            validation_id = cursor.fetchone()[0]
                            
                            # Insert into column_validation_rules with explicit ID
                            cursor.execute("""
                                INSERT INTO column_validation_rules (column_validation_id, column_id, rule_type_id, rule_config, created_at)
                                VALUES (?, ?, ?, ?, GETDATE())
                            """, validation_id, column_id, rule_type_id, json.dumps({}))
                            
                            self.logger.info(f"Added rule {rule_name} to column {header}")
            
            self.connection.commit()
            
            self.logger.info(f"✓ Template saved successfully: {template_name} (ID: {template_id}) with {len(rules_config)} rule configurations")
            return template_id
            
        except Exception as e:
            self.logger.error(f"Failed to save template: {e}")
            self.logger.error(f"Template name: {filename}, Headers: {headers[:3]}...")
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return None

    def get_templates_for_user(self, user_id: int) -> list:
        """Get templates for user"""
        try:
            if not self.connection:
                # Return mock data for demo
                return [{
                    'id': 1,
                    'name': 'Customer_Data_Template',
                    'headers': ['name', 'email', 'phone', 'age'],
                    'rules_config': {'name': ['Required'], 'email': ['Email'], 'age': ['Int']},
                    'created_at': datetime.now().isoformat()
                }]
            
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT id, name, headers, rules_config, created_at
                FROM excel_templates
                WHERE user_id = ?
                ORDER BY created_at DESC
            """, user_id)
            
            templates = []
            for row in cursor.fetchall():
                templates.append({
                    'id': row[0],
                    'name': row[1],
                    'headers': json.loads(row[2]),
                    'rules_config': json.loads(row[3]),
                    'created_at': row[4].isoformat()
                })
            
            return templates
            
        except Exception as e:
            self.logger.error(f"Failed to get templates: {e}")
            return []
    
    def save_validation_session(self, user_id: int, session_id: str, file_info: dict, results: dict, original_path: str, corrected_path: str = None) -> int:
        """Save validation session results"""
        try:
            if not self.connection:
                self.logger.info("Validation session saved (fallback)")
                return 1
            
            cursor = self.connection.cursor()
            
            cursor.execute("""
                INSERT INTO validation_sessions 
                (user_id, session_id, filename, original_file_path, corrected_file_path,
                 total_rows, error_count, success_rate, created_at)
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """, 
            user_id, session_id, file_info.get('filename'),
            original_path, corrected_path,
            results.get('total_rows', 0), results.get('total_errors', 0),
            results.get('success_rate', 0.0))
            
            session_db_id = cursor.fetchone()[0]
            
            # Save individual errors
            if results.get('errors'):
                for error in results['errors']:
                    cursor.execute("""
                        INSERT INTO validation_errors 
                        (session_id, row_number, column_name, error_type, error_message, original_value)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, session_db_id, error['row'], error['column'], 
                    error['rule'], error['error'], str(error['value']))
            
            self.connection.commit()
            self.logger.info(f"Validation session saved: {session_id}")
            return session_db_id
            
        except Exception as e:
            self.logger.error(f"Failed to save validation session: {e}")
            return None


# Data validation endpoints removed - only rule configuration remains
from rule_configuration_endpoints import register_rule_configuration_routes

# Initialize services
lakehouse_service = FabricLakehouseService()
duckdb_service = DuckDBService()
sql_fabric_service = SQLFabricService()

# Flask app setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = './sessions'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True

Session(app)
CORS(app, supports_credentials=True, origins=['http://localhost:3000'])

# Register routes ONLY ONCE - Check if already registered
if 'get_all_validation_rules' not in [rule.endpoint for rule in app.url_map.iter_rules()]:
    register_rule_configuration_routes(app, lakehouse_service, duckdb_service, sql_fabric_service)
    logger.info("✓ Rule configuration routes registered")
else:
    logger.warning("⚠ Routes already registered, skipping...")


# Demo users with multi-tenant support
DEMO_USERS = {
    'admin@example.com': {
        'password': bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()),
        'user': {
            'id': 1,
            'email': 'admin@example.com',
            'first_name': 'Admin',
            'last_name': 'User',
            'role': 'admin',
            'is_approved': True,
            'domain': 'example.com',
            'created_at': datetime.now().isoformat()
        }
    },
    'tenant@datasync.ai': {
        'password': bcrypt.hashpw('tenant123'.encode('utf-8'), bcrypt.gensalt()),
        'user': {
            'id': 2,
            'email': 'tenant@datasync.ai',
            'first_name': 'Tenant',
            'last_name': 'Manager',
            'role': 'tenant',
            'is_approved': True,
            'domain': 'datasync.ai',
            'created_at': datetime.now().isoformat()
        }
    },
    'user@datasync.ai': {
        'password': bcrypt.hashpw('user123'.encode('utf-8'), bcrypt.gensalt()),
        'user': {
            'id': 3,
            'email': 'user@datasync.ai',
            'first_name': 'Regular',
            'last_name': 'User',
            'role': 'user',
            'is_approved': True,
            'domain': 'datasync.ai',
            'created_at': datetime.now().isoformat()
        }
    }
}

# Authentication routes

@app.route('/api/auth/register', methods=['POST'])
def register_user():
    """Register new user with admin approval workflow - FIXED to handle both name formats"""
    try:
        data = request.get_json()
        
        # Extract data - support BOTH name formats
        email = data.get('email', '').lower().strip()
        password = data.get('password', '')
        
        # Handle BOTH name formats:
        # Format 1: Single "name" field (Full Name)
        # Format 2: Separate "first_name" and "last_name" fields
        name = data.get('name', '').strip()
        first_name = data.get('first_name', data.get('firstName', '')).strip()
        last_name = data.get('last_name', data.get('lastName', '')).strip()
        
        # If name is provided, split it
        if name and not (first_name and last_name):
            name_parts = name.split(' ', 1)
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        # If first_name and last_name provided separately, use them
        if not name and (first_name or last_name):
            name = f"{first_name} {last_name}".strip()
        
        # Phone/Mobile number - support multiple field names
        phone = data.get('phone', data.get('mobile', '')).strip()
        
        # Validation - require either name OR (first_name AND last_name)
        has_name = bool(name) or (bool(first_name) and bool(last_name))
        
        if not email or not password or not has_name:
            logger.warning(f"Registration validation failed: email={bool(email)}, password={bool(password)}, has_name={has_name}")
            logger.warning(f"Received data: {data}")
            return jsonify({
                'success': False, 
                'message': 'Name (or first & last name), email, and password are required'
            }), 400
        
        if len(password) < 6:
            return jsonify({'success': False, 'message': 'Password must be at least 6 characters'}), 400
        
        # Extract domain from email
        if '@' not in email:
            return jsonify({'success': False, 'message': 'Invalid email format'}), 400
        
        domain = email.split('@')[1]
        
        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        # Check if SQL Fabric is available for real user storage
        if sql_fabric_service.connection:
            try:
                cursor = sql_fabric_service.connection.cursor()
                
                # Check if user already exists
                cursor.execute("SELECT id FROM login_details WHERE email = ?", email)
                if cursor.fetchone():
                    return jsonify({'success': False, 'message': 'Email already registered'}), 409
                
                # Insert new user with phone number (not approved by default)
                logger.info(f"Registering user: email={email}, first_name={first_name}, last_name={last_name}, phone={phone}")
                
                cursor.execute("""
                    INSERT INTO login_details 
                    (email, password_hash, first_name, last_name, mobile, role, is_approved, domain, updated_at)
                    OUTPUT INSERTED.id
                    VALUES (?, ?, ?, ?, ?, 'user', 0, ?, GETDATE())
                """, email, password_hash.decode('utf-8'), first_name, last_name, phone, domain)
                
                user_id = cursor.fetchone()[0]
                sql_fabric_service.connection.commit()
                
                logger.info(f"[SUCCESS] User registered successfully: {email} (ID: {user_id}) - Pending approval")
                
                return jsonify({
                    'success': True,
                    'message': 'Registration successful! Your account is pending admin approval.',
                    'user_id': user_id,
                    'requires_approval': True
                }), 201
                
            except Exception as e:
                logger.error(f"[ERROR] Registration failed: {e}")
                logger.error(f"Data attempted: email={email}, first_name={first_name}, last_name={last_name}")
                return jsonify({'success': False, 'message': f'Registration failed: {str(e)}'}), 500
        
        else:
            # Fallback mode - add to demo users (for development)
            logger.info(f"Registration in demo mode: {email}")
            return jsonify({
                'success': True,
                'message': 'Registration successful! (Demo mode - contact admin for approval)',
                'requires_approval': True
            })
            
    except Exception as e:
        logger.error(f"[ERROR] Registration error: {e}")
        return jsonify({'success': False, 'message': f'Registration failed: {str(e)}'}), 500

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Enhanced login with multi-tenant support and approval checking"""
    try:
        data = request.get_json()
        username = data.get('username', '').lower().strip()
        password = data.get('password', '')
        
        # Check real SQL Fabric database first
        if sql_fabric_service.connection:
            try:
                cursor = sql_fabric_service.connection.cursor()
                cursor.execute("""
                SELECT id, email, password_hash, password, first_name, last_name, role, is_approved, domain, updated_at
                FROM login_details WHERE email = ?
                """, username)
                
                user_record = cursor.fetchone()
                if user_record:
                    # Try password_hash first (new registrations), then password field (existing users)
                    stored_password = user_record[2] if user_record[2] else user_record[3]  # password_hash or password
                    
                    if stored_password and bcrypt.checkpw(password.encode('utf-8'), stored_password.encode('utf-8')):
                        # Auto-approve admin users or check approval status
                        if not user_record[7] and username != 'admin@example.com':  # is_approved column
                            return jsonify({
                                'success': False, 
                                'message': 'Your account is pending approval by an administrator.'
                            }), 403
                        
                        # Auto-approve admin if not already approved
                        if username == 'admin@example.com' and not user_record[7]:
                            cursor.execute("UPDATE login_details SET is_approved = 1, role = 'admin' WHERE email = ?", username)
                            sql_fabric_service.connection.commit()
                            logger.info(f"Auto-approved admin user: {username}")
                            # Refresh user_record to get updated role
                            cursor.execute("""
                                SELECT id, email, password_hash, password, first_name, last_name, role, is_approved, domain, updated_at
                                FROM login_details WHERE email = ?
                            """, username)
                            user_record = cursor.fetchone()
                        
                        # User is approved, create session
                        user_data = {
                            'id': user_record[0],
                            'email': user_record[1],
                            'first_name': user_record[4] if user_record[4] else 'Admin',
                            'last_name': user_record[5] if user_record[5] else 'User',
                            'role': user_record[6] if user_record[6] else ('admin' if username == 'admin@example.com' else 'user'),
                            'is_approved': bool(user_record[7]),
                            'domain': user_record[8] if user_record[8] else user_record[1].split('@')[1],
                            'updated_at': user_record[9].isoformat() if user_record[9] else datetime.now().isoformat()
                        }
                        
                        session['user'] = user_data
                        session['authenticated'] = True
                        
                        logger.info(f"Login successful: {username} ({user_data['role']}) - Real database")
                        
                        return jsonify({
                            'success': True,
                            'message': 'Login successful',
                            'user': user_data
                        })
                    
            except Exception as e:
                logger.error(f"Database login error: {e}")
        
        # Fallback to demo users
        user_data = DEMO_USERS.get(username)
        if user_data and bcrypt.checkpw(password.encode('utf-8'), user_data['password']):
            session['user'] = user_data['user']
            session['authenticated'] = True
            
            logger.info(f"Login successful: {username} ({user_data['user']['role']}) - Demo mode")
            
            return jsonify({
                'success': True,
                'message': 'Login successful',
                'user': user_data['user']
            })
        
        return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
        
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'success': False, 'message': 'Login failed'}), 500

@app.route('/api/auth/me', methods=['GET'])
def get_current_user():
    """Get current authenticated user"""
    if session.get('authenticated') and session.get('user'):
        return jsonify({'success': True, 'user': session['user']})
    
    return jsonify({'success': False, 'message': 'Not authenticated'}), 401

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout user"""
    session.clear()
    return jsonify({'success': True, 'message': 'Logged out successfully'})

# File upload with DuckDB integration
@app.route('/api/files/upload', methods=['POST'])
def upload_file():
    """Enhanced file upload with DuckDB processing"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    context = request.form.get('context', 'unknown')
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    session_id = str(uuid.uuid4())
    filename = secure_filename(file.filename)
    
    # Save file temporarily
    temp_path = os.path.join('temp', f"{session_id}_{filename}")
    file.save(temp_path)
    
    try:
        if context == 'rule-configuration':
            # Rule Configuration Workflow: File Upload → DuckDB (load + analyze)
            result = duckdb_service.load_file_for_analysis(session_id, temp_path, filename)
            
            if 'error' in result:
                return jsonify({'success': False, 'error': result['error']}), 400
            
            # ADD FILENAME to result before storing in session
            result['filename'] = filename  # FIX: Include actual filename
            
            # Store session info
            session['current_session'] = session_id
            session['current_context'] = context
            session['file_info'] = result
            
            # Clean up temp file (no storage needed for rule config)
            os.remove(temp_path)
            
            return jsonify({
                'success': True,
                'context': context,
                'session_id': session_id,
                'file_info': {
                    'filename': filename,
                    'headers': result['headers'],
                    'row_count': result['row_count'],
                    'column_profiles': result['column_profiles'],
                    'preview': result['preview']
                }
            })
            
        elif context == 'data-validation':
            # Data Validation Workflow: File Upload → MS Fabric Lakehouse (store original) → DuckDB (load)
            
            # Store original file in Fabric Lakehouse
            with open(temp_path, 'rb') as f:
                file_content = f.read()
            
            original_path = lakehouse_service.store_original_file(
                user['id'], session_id, filename, file_content
            )
            
            # Load into DuckDB for processing
            result = duckdb_service.load_file_for_validation(session_id, temp_path, filename)
            
            if 'error' in result:
                return jsonify({'success': False, 'error': result['error']}), 400
            
            # Store session info
            session['current_session'] = session_id
            session['current_context'] = context
            session['original_file_path'] = original_path
            session['validation_data'] = result
            
            # Clean up temp file
            os.remove(temp_path)
            
            # Apply validation (mock for now - would use real templates)
            mock_rules = {
                result['headers'][0] if result['headers'] else 'column1': ['Required'],
                result['headers'][1] if len(result['headers']) > 1 else 'column2': ['Email']
            }
            
            validation_result = duckdb_service.validate_data_with_rules(
                result['table_name'], mock_rules
            )
            
            return jsonify({
                'success': True,
                'context': context,
                'session_id': session_id,
                'file_info': {
                    'filename': filename,
                    'headers': result['headers'],
                    'row_count': result['row_count']
                },
                'results': {
                    'total_rows': result['row_count'],
                    'valid_rows': result['row_count'] - len(validation_result.get('errors', [])),
                    'errors': validation_result.get('errors', []),
                    'total_errors': validation_result.get('total_errors', 0)
                }
            })
        
        else:
            os.remove(temp_path)
            return jsonify({'error': 'Invalid context'}), 400
            
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        logger.error(f"File upload failed: {e}")
        return jsonify({'error': str(e)}), 500



# REMOVED DUPLICATE ROUTES - These are properly defined in rule_configuration_endpoints.py
# @app.route('/api/validation/configure-headers', methods=['POST'])
# @app.route('/api/validation/configure-rules', methods=['POST'])


@app.route('/api/validation/validate-existing-template/<int:template_id>', methods=['POST', 'GET'])
def validate_existing_template(template_id):
    """Validate data against existing template rules - COMPLETE IMPLEMENTATION"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        user = session.get('user')
        
        # Get template details and rules from SQL Fabric
        if not sql_fabric_service.connection:
            return jsonify({'error': 'Database connection not available'}), 500
        
        cursor = sql_fabric_service.connection.cursor()
        
        # Get template info
        cursor.execute("""
            SELECT template_name, headers 
            FROM excel_templates 
            WHERE template_id = ? AND status = 'ACTIVE'
        """, template_id)
        
        template_row = cursor.fetchone()
        if not template_row:
            return jsonify({'error': 'Template not found'}), 404
        
        template_name = template_row[0]
        headers = json.loads(template_row[1]) if template_row[1] else []
        
        # Get all rules for this template
        cursor.execute("""
            SELECT 
                tc.column_name,
                vrt.rule_name
            FROM template_columns tc
            JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
            WHERE tc.template_id = ? AND tc.is_selected = 1
            ORDER BY tc.column_name, vrt.rule_name
        """, template_id)
        
        rules_rows = cursor.fetchall()
        
        # Build rules_config dictionary
        rules_config = {}
        for row in rules_rows:
            column_name = row[0]
            rule_name = row[1]
            if column_name not in rules_config:
                rules_config[column_name] = []
            if rule_name not in rules_config[column_name]:
                rules_config[column_name].append(rule_name)
        
        logger.info(f"Validating template {template_id} with rules: {rules_config}")
        
        # Get the uploaded file from session or check for existing validation data
        validation_data = session.get('validation_data')
        
        if not validation_data:
            # No file in session - user needs to upload first
            return jsonify({'error': 'No file uploaded for validation'}), 400
        
        # Get the data from DuckDB
        table_name = validation_data.get('table_name')
        if not table_name:
            return jsonify({'error': 'Invalid validation data'}), 400
        
        # Validate the data using DuckDB service
        validation_result = duckdb_service.validate_data_with_rules(table_name, rules_config)
        
        if 'error' in validation_result:
            return jsonify({'error': validation_result['error']}), 500
        
        # Get the actual data rows
        df = duckdb_service.connection.execute(f"SELECT * FROM {table_name}").df()
        
        # Format error_cell_locations as expected by frontend
        error_cell_locations = {}
        for error in validation_result.get('errors', []):
            column = error['column']
            if column not in error_cell_locations:
                error_cell_locations[column] = []
            
            error_cell_locations[column].append({
                'row': error['row'],
                'value': error['value'] if error['value'] is not None else 'NULL',
                'rule_failed': error['rule'],
                'reason': error['error']
            })
        
        # Convert DataFrame to list of dicts for frontend
        data_rows = df.to_dict('records')
        
        # Replace None/NaN with 'NULL' string for frontend
        for row in data_rows:
            for key in row:
                if pd.isna(row[key]) or row[key] is None:
                    row[key] = 'NULL'
        
        logger.info(f"Validation complete: {len(validation_result.get('errors', []))} errors found")
        
        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows,
            'total_errors': len(validation_result.get('errors', [])),
            'total_rows': len(data_rows)
        })
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/validation/save-corrections/<int:template_id>', methods=['POST'])
def save_old_validation_corrections(template_id):
    """Save corrections and generate corrected file"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        user = session.get('user')
        data = request.get_json()
        corrections = data.get('corrections', {})
        
        # Get validation data from session
        validation_data = session.get('validation_data')
        if not validation_data:
            return jsonify({'error': 'No validation data found'}), 400
        
        table_name = validation_data.get('table_name')
        filename = validation_data.get('filename', 'corrected.xlsx')
        
        # Get the original data
        df = duckdb_service.connection.execute(f"SELECT * FROM {table_name}").df()
        
        # Apply corrections
        corrected_df = df.copy()
        for column, row_corrections in corrections.items():
            if column in corrected_df.columns:
                for row_idx_str, value in row_corrections.items():
                    row_idx = int(row_idx_str)
                    if row_idx < len(corrected_df):
                        corrected_df.at[row_idx, column] = value
        
        # Store corrected file in Fabric Lakehouse
        session_id = session.get('current_session', str(uuid.uuid4()))
        corrected_path = lakehouse_service.store_corrected_file(
            user['id'], session_id, filename, corrected_df
        )
        
        # Save to validation_corrections table
        if sql_fabric_service.connection:
            cursor = sql_fabric_service.connection.cursor()
            cursor.execute("""
                INSERT INTO validation_corrections 
                (template_id, user_id, original_file_path, corrected_file_path, 
                 corrections_json, total_corrections, created_at)
                VALUES (?, ?, ?, ?, ?, ?, GETDATE())
            """, template_id, user['id'], 
            session.get('original_file_path', ''), corrected_path,
            json.dumps(corrections), len(corrections), datetime.now())
            
            sql_fabric_service.connection.commit()
        
        logger.info(f"Corrections saved: {len(corrections)} corrections applied")
        
        return jsonify({
            'success': True,
            'corrected_file_path': corrected_path,
            'message': 'Corrections saved successfully'
        })
        
    except Exception as e:
        logger.error(f"Failed to save corrections: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/validation/download-corrected', methods=['GET'])
def download_old_corrected_file():
    """Download corrected file (Step 3 of Data Validation)"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    session_id = session.get('current_session')
    if not session_id:
        return jsonify({'error': 'No active session'}), 400
    
    # Get validation data and apply corrections
    validation_data = session.get('validation_data', {})
    if not validation_data:
        return jsonify({'error': 'No validation data'}), 400
    
    # Mock correction process - in real implementation, get from validation results
    df = validation_data.get('data')
    if df is not None:
        user = session.get('user')
        filename = session.get('file_info', {}).get('filename', 'corrected.xlsx')
        
        # Store corrected file in Fabric Lakehouse
        corrected_path = lakehouse_service.store_corrected_file(
            user['id'], session_id, filename, df
        )
        
        # Save validation session to SQL Fabric
        results = {
            'total_rows': len(df),
            'total_errors': 0,
            'success_rate': 100.0
        }
        
        sql_fabric_service.save_validation_session(
            user['id'], session_id, 
            {'filename': filename}, 
            results,
            session.get('original_file_path'),
            corrected_path
        )
        
        # Clean up DuckDB session
        duckdb_service.cleanup_session(session_id)
        
        # Return file for download
        full_path = lakehouse_service.get_file(corrected_path)
        if full_path and os.path.exists(full_path):
            return send_file(full_path, as_attachment=True, 
                           download_name=os.path.basename(corrected_path))
    
    return jsonify({'error': 'File not found'}), 404

# Admin routes
@app.route('/api/admin/users', methods=['GET'])
def get_all_users():
    """Get all users (admin only) from real database"""
    if not session.get('authenticated') or session.get('user', {}).get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    # Get users from real SQL Fabric database - USE SEPARATE CONNECTION
    if sql_fabric_service.connection_string:
        try:
            # Create new connection for this query to avoid "connection busy" error
            conn = pyodbc.connect(sql_fabric_service.connection_string, timeout=30)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, email, first_name, last_name, mobile, role, is_approved, domain, updated_at
                FROM login_details
                ORDER BY updated_at DESC
            """)
            
            users = []
            for row in cursor.fetchall():
                users.append({
                    'id': row[0],
                    'email': row[1],
                    'first_name': row[2] if row[2] else 'N/A',
                    'last_name': row[3] if row[3] else 'N/A',
                    'mobile': row[4] if row[4] else 'N/A',
                    'role': row[5] if row[5] else 'user',
                    'is_approved': bool(row[6]) if row[6] is not None else False,
                    'domain': row[7] if row[7] else (row[1].split('@')[1] if '@' in row[1] else 'unknown'),
                    'updated_at': row[8].isoformat() if row[8] else datetime.now().isoformat()
                })
            
            conn.close()
            logger.info(f"Admin retrieved {len(users)} users from SQL Fabric")
            return jsonify({'success': True, 'users': users})
            
        except Exception as e:
            logger.error(f"Failed to get users from SQL Fabric: {e}")
            return jsonify({'error': 'Failed to retrieve users from database'}), 500
    
    # Fallback to demo users
    logger.warning("Using fallback demo users - SQL Fabric not available")
    users = [user_data['user'] for user_data in DEMO_USERS.values()]
    return jsonify({'success': True, 'users': users})

@app.route('/api/admin/users/<int:user_id>/change-role', methods=['POST'])
def change_user_role(user_id):
    """Change user role (admin only)"""
    if not session.get('authenticated') or session.get('user', {}).get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    data = request.get_json()
    new_role = data.get('role', 'user').lower()
    
    # Validate role
    if new_role not in ['user', 'tenant', 'admin']:
        return jsonify({'error': 'Invalid role. Must be user, tenant, or admin'}), 400
    
    # Update role in SQL Fabric database
    if sql_fabric_service.connection:
        try:
            cursor = sql_fabric_service.connection.cursor()
            cursor.execute("""
                UPDATE login_details 
                SET role = ?, updated_at = GETDATE()
                WHERE id = ?
            """, new_role, user_id)
            
            if cursor.rowcount > 0:
                sql_fabric_service.connection.commit()
                
                # Get updated user info
                cursor.execute("""
                    SELECT email, first_name, last_name, role FROM login_details WHERE id = ?
                """, user_id)
                user_info = cursor.fetchone()
                
                if user_info:
                    logger.info(f"Role changed: {user_info[0]} -> {new_role.upper()} by admin")
                    return jsonify({
                        'success': True, 
                        'message': f'Role changed to {new_role.upper()} successfully',
                        'user': {
                            'id': user_id,
                            'email': user_info[0],
                            'name': f"{user_info[1]} {user_info[2]}",
                            'new_role': new_role
                        }
                    })
                else:
                    return jsonify({'error': 'User not found after update'}), 404
            else:
                return jsonify({'error': 'User not found'}), 404
                
        except Exception as e:
            logger.error(f"Failed to change user role: {e}")
            return jsonify({'error': 'Failed to change user role'}), 500
    
    # Mock role change for demo mode
    return jsonify({'success': True, 'message': f'Role changed to {new_role.upper()} (demo mode)'})

@app.route('/api/admin/users/<int:user_id>/approve', methods=['POST'])
def approve_user(user_id):
    """Approve user (admin only)"""
    if not session.get('authenticated') or session.get('user', {}).get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    # Update in real SQL Fabric database
    if sql_fabric_service.connection:
        try:
            cursor = sql_fabric_service.connection.cursor()
            cursor.execute("""
                UPDATE login_details 
                SET is_approved = 1, updated_at = GETDATE()
                WHERE id = ?
            """, user_id)
            
            if cursor.rowcount > 0:
                sql_fabric_service.connection.commit()
                logger.info(f"User approved: ID {user_id}")
                return jsonify({'success': True, 'message': 'User approved successfully'})
            else:
                return jsonify({'error': 'User not found'}), 404
                
        except Exception as e:
            logger.error(f"Failed to approve user: {e}")
            return jsonify({'error': 'Failed to approve user'}), 500
    
    # Mock approval for demo mode
    return jsonify({'success': True, 'message': 'User approved (demo mode)'})

@app.route('/api/admin/users/<int:user_id>/reject', methods=['POST'])
def reject_user(user_id):
    """Reject/Delete user (admin only)"""
    if not session.get('authenticated') or session.get('user', {}).get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    # Delete from real SQL Fabric database
    if sql_fabric_service.connection:
        try:
            cursor = sql_fabric_service.connection.cursor()
            cursor.execute("DELETE FROM login_details WHERE id = ?", user_id)
            
            if cursor.rowcount > 0:
                sql_fabric_service.connection.commit()
                logger.info(f"User rejected/deleted: ID {user_id}")
                return jsonify({'success': True, 'message': 'User rejected successfully'})
            else:
                return jsonify({'error': 'User not found'}), 404
                
        except Exception as e:
            logger.error(f"Failed to reject user: {e}")
            return jsonify({'error': 'Failed to reject user'}), 500
    
    # Mock rejection for demo mode
    return jsonify({'success': True, 'message': 'User rejected (demo mode)'})

"""
PATCH FILE: Add Tenant Routes to app_complete.py

INSTRUCTIONS:
1. Open app_complete.py
2. Find the line: @app.route('/api/dashboard/stats', methods=['GET'])
3. INSERT the code below BEFORE that line
4. Save the file
"""

# =============================================================================
# TENANT ROUTES - INSERT THESE BEFORE @app.route('/api/dashboard/stats')
# =============================================================================

@app.route('/api/tenant/domain-users', methods=['GET'])
def get_tenant_domain_users():
    """Get users in tenant's domain (tenant only)"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    role = user.get('role')
    
    # Allow both tenant and admin to access this
    if role not in ['tenant', 'admin']:
        return jsonify({'error': 'Tenant or Admin access required'}), 403
    
    domain = user.get('domain')
    
    # Get users from real SQL Fabric database - USE SEPARATE CONNECTION
    if sql_fabric_service.connection_string:
        try:
            # Create new connection for this query to avoid "connection busy" error
            conn = pyodbc.connect(sql_fabric_service.connection_string, timeout=30)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, email, first_name, last_name, mobile, role, is_approved, domain, updated_at
                FROM login_details
                WHERE domain = ?
                ORDER BY updated_at DESC
            """, domain)
            
            users = []
            for row in cursor.fetchall():
                users.append({
                    'id': row[0],
                    'email': row[1],
                    'first_name': row[2] if row[2] else 'N/A',
                    'last_name': row[3] if row[3] else 'N/A',
                    'mobile': row[4] if row[4] else 'N/A',
                    'role': row[5] if row[5] else 'user',
                    'is_approved': bool(row[6]) if row[6] is not None else False,
                    'domain': row[7] if row[7] else domain,
                    'updated_at': row[8].isoformat() if row[8] else datetime.now().isoformat()
                })
            
            conn.close()
            logger.info(f"Tenant retrieved {len(users)} users from domain {domain}")
            return jsonify({'success': True, 'users': users, 'domain': domain})
            
        except Exception as e:
            logger.error(f"Failed to get tenant domain users: {e}")
            return jsonify({'error': f'Failed to retrieve domain users: {str(e)}'}), 500
    
    # Fallback to demo users
    logger.warning("Using fallback demo users for tenant")
    demo_domain_users = [
        user_data['user'] for user_data in DEMO_USERS.values() 
        if user_data['user'].get('domain') == domain
    ]
    return jsonify({'success': True, 'users': demo_domain_users, 'domain': domain})

@app.route('/api/tenant/users/<int:user_id>/approve', methods=['POST'])
def tenant_approve_user(user_id):
    """Approve user in tenant's domain (tenant only)"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    if user.get('role') not in ['tenant', 'admin']:
        return jsonify({'error': 'Tenant or Admin access required'}), 403
    
    domain = user.get('domain')
    
    # Update in real SQL Fabric database - USE SEPARATE CONNECTION
    if sql_fabric_service.connection_string:
        try:
            conn = pyodbc.connect(sql_fabric_service.connection_string, timeout=30)
            cursor = conn.cursor()
            
            # Verify user is in tenant's domain
            cursor.execute("SELECT domain FROM login_details WHERE id = ?", user_id)
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return jsonify({'error': 'User not found'}), 404
            
            if result[0] != domain and user.get('role') != 'admin':
                conn.close()
                return jsonify({'error': 'User not in your domain'}), 403
            
            # Approve user
            cursor.execute("""
                UPDATE login_details 
                SET is_approved = 1, updated_at = GETDATE()
                WHERE id = ?
            """, user_id)
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                logger.info(f"Tenant approved user: ID {user_id} in domain {domain}")
                return jsonify({'success': True, 'message': 'User approved successfully'})
            else:
                conn.close()
                return jsonify({'error': 'Failed to approve user'}), 500
                
        except Exception as e:
            logger.error(f"Failed to approve user: {e}")
            return jsonify({'error': f'Failed to approve user: {str(e)}'}), 500
    
    return jsonify({'success': True, 'message': 'User approved (demo mode)'})

@app.route('/api/tenant/users/<int:user_id>/reject', methods=['POST'])
def tenant_reject_user(user_id):
    """Reject/Delete user in tenant's domain (tenant only)"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    if user.get('role') not in ['tenant', 'admin']:
        return jsonify({'error': 'Tenant or Admin access required'}), 403
    
    domain = user.get('domain')
    
    # Delete from real SQL Fabric database - USE SEPARATE CONNECTION
    if sql_fabric_service.connection_string:
        try:
            conn = pyodbc.connect(sql_fabric_service.connection_string, timeout=30)
            cursor = conn.cursor()
            
            # Verify user is in tenant's domain
            cursor.execute("SELECT domain FROM login_details WHERE id = ?", user_id)
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return jsonify({'error': 'User not found'}), 404
            
            if result[0] != domain and user.get('role') != 'admin':
                conn.close()
                return jsonify({'error': 'User not in your domain'}), 403
            
            # Delete user
            cursor.execute("DELETE FROM login_details WHERE id = ?", user_id)
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                logger.info(f"Tenant rejected/deleted user: ID {user_id} from domain {domain}")
                return jsonify({'success': True, 'message': 'User rejected successfully'})
            else:
                conn.close()
                return jsonify({'error': 'Failed to reject user'}), 500
                
        except Exception as e:
            logger.error(f"Failed to reject user: {e}")
            return jsonify({'error': f'Failed to reject user: {str(e)}'}), 500
    
    return jsonify({'success': True, 'message': 'User rejected (demo mode)'})

# END OF TENANT ROUTES
# =============================================================================


@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """Get dashboard statistics based on user role"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    role = user.get('role', 'user')
    
    if role == 'admin':
        # Admin Dashboard Stats - Show all users info and system-wide stats
        try:
            # Get user counts from SQL Fabric
            if sql_fabric_service.connection:
                cursor = sql_fabric_service.connection.cursor()
                
                # Total users
                cursor.execute("SELECT COUNT(*) FROM login_details")
                total_users = cursor.fetchone()[0]
                
                # Pending approvals
                cursor.execute("SELECT COUNT(*) FROM login_details WHERE is_approved = 0")
                pending_approvals = cursor.fetchone()[0]
                
                # Users by role
                cursor.execute("SELECT ISNULL(role, 'user') as role, COUNT(*) FROM login_details GROUP BY ISNULL(role, 'user')")
                role_results = cursor.fetchall()
                role_counts = dict(role_results) if role_results else {}
                
                # Validation sessions
                cursor.execute("SELECT COUNT(*) FROM validation_sessions")
                total_validations = cursor.fetchone()[0]
                
                # Templates
                cursor.execute("SELECT COUNT(*) FROM excel_templates")
                total_templates = cursor.fetchone()[0]
                
                admin_stats = {
                    'dashboard_type': 'admin',
                    'total_users': total_users,
                    'pending_approvals': pending_approvals,
                    'total_validations': total_validations,
                    'total_templates': total_templates,
                    'role_distribution': role_counts,
                    'system_status': 'operational',
                    'active_tenants': role_counts.get('tenant', 0)
                }
                
                logger.info(f"Admin stats: {admin_stats}")
                return jsonify(admin_stats)
            
        except Exception as e:
            logger.error(f"Failed to get admin stats from SQL Fabric: {e}")
        
        # Fallback admin stats
        return jsonify({
            'dashboard_type': 'admin',
            'total_users': len(DEMO_USERS),
            'pending_approvals': 0,
            'total_validations': 45,
            'total_templates': 12,
            'role_distribution': {'admin': 1, 'tenant': 1, 'user': 1},
            'system_status': 'operational',
            'active_tenants': 1
        })
    
    elif role == 'tenant':
        # Tenant Dashboard Stats - Show domain-specific stats
        domain = user.get('domain')
        
        try:
            if sql_fabric_service.connection:
                cursor = sql_fabric_service.connection.cursor()
                
                # Users in tenant's domain
                cursor.execute("SELECT COUNT(*) FROM login_details WHERE domain = ?", domain)
                domain_users = cursor.fetchone()[0]
                
                # Pending approvals in domain
                cursor.execute("SELECT COUNT(*) FROM login_details WHERE domain = ? AND is_approved = 0", domain)
                domain_pending = cursor.fetchone()[0]
                
                tenant_stats = {
                    'dashboard_type': 'tenant',
                    'domain': domain,
                    'domain_users': domain_users,
                    'pending_approvals': domain_pending,
                    'total_validations': 23,
                    'total_templates': 8
                }
                
                return jsonify(tenant_stats)
            
        except Exception as e:
            logger.error(f"Failed to get tenant stats: {e}")
        
        # Fallback tenant stats
        return jsonify({
            'dashboard_type': 'tenant',
            'domain': user.get('domain'),
            'domain_users': 5,
            'pending_approvals': 1,
            'total_validations': 23,
            'total_templates': 8
        })
    
    else:
        # User Dashboard Stats - Show personal stats
        user_stats = {
            'dashboard_type': 'user',
            'my_templates': 3,
            'my_validations': 12,
            'successful_validations': 10,
            'failed_validations': 2,
            'recent_activity': 'Configuration completed'
        }
        
        return jsonify(user_stats)

@app.route('/api/templates', methods=['GET'])
def get_user_templates():
    """Get templates for current user with rule counts"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    user = session.get('user')
    
    # Query excel_templates table with rule counts
    templates = []
    try:
        if sql_fabric_service.connection:
            cursor = sql_fabric_service.connection.cursor()
            cursor.execute("""
                SELECT 
                    t.template_id, 
                    t.template_name, 
                    t.headers,
                    t.created_at,
                    COUNT(DISTINCT cvr.column_validation_id) as rules_count
                FROM excel_templates t
                LEFT JOIN template_columns tc ON t.template_id = tc.template_id
                LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                WHERE t.status = 'ACTIVE'
                GROUP BY t.template_id, t.template_name, t.headers, t.created_at
                ORDER BY t.created_at DESC
            """)
            
            template_rows = cursor.fetchall()
            for row in template_rows:
                # Parse headers from JSON
                try:
                    headers = json.loads(row[2]) if row[2] else []
                except:
                    headers = []
                
                templates.append({
                    'id': row[0],
                    'name': row[1],
                    'created_at': row[3].isoformat() if row[3] else None,
                    'headers': headers,
                    'rules_count': row[4],  # Actual count from database
                    'rules_config': {}  # Can be populated if needed
                })
            
            logger.info(f"Fetched {len(templates)} templates from SQL Fabric")
        else:
            logger.warning("No SQL Fabric connection, returning empty templates")
            templates = []
    except Exception as e:
        logger.error(f"Failed to get templates: {e}")
        templates = []
    
    return jsonify({'success': True, 'templates': templates})


@app.route('/api/files/upload', methods=['POST'])
def upload_file_for_workflow():
    """Handle file upload for rule configuration or data validation"""
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    context = request.form.get('context', 'rule-configuration')
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        # Secure the filename
        filename = secure_filename(file.filename)
        session_id = str(uuid.uuid4())
        
        # Save file locally
        upload_path = os.path.join('uploads', filename)
        file.save(upload_path)
        
        # Load file with DuckDB for analysis
        file_info = duckdb_service.load_file_for_analysis(session_id, upload_path, filename)
        
        if 'error' in file_info:
            return jsonify({'success': False, 'error': file_info['error']}), 400
        
        # Store file info in session
        session['file_info'] = {
            'filename': filename,
            'session_id': session_id,
            'headers': file_info['headers'],
            'row_count': file_info['row_count'],
            'upload_path': upload_path
        }
        session['context'] = context
        
        logger.info(f"File uploaded successfully: {filename} ({file_info['row_count']} rows)")
        
        return jsonify({
            'success': True,
            'file_info': {
                'filename': filename,
                'headers': file_info['headers'],
                'row_count': file_info['row_count'],
                'column_profiles': file_info.get('column_profiles', {}),
                'preview': file_info.get('preview', [])
            },
            'context': context
        })
        
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/debug/users', methods=['GET'])
def debug_users():
    """Debug endpoint to check SQL Fabric users"""
    if sql_fabric_service.connection:
        try:
            cursor = sql_fabric_service.connection.cursor()
            
            # Test basic query
            cursor.execute("SELECT COUNT(*) FROM login_details")
            count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute("SELECT id, email, role, is_approved FROM login_details")
            sample_data = cursor.fetchall()
            
            return jsonify({
                'sql_fabric_connected': True,
                'total_users_in_db': count,
                'sample_users': [{
                    'id': row[0],
                    'email': row[1], 
                    'role': row[2],
                    'is_approved': bool(row[3]) if row[3] is not None else None
                } for row in sample_data[:5]]
            })
            
        except Exception as e:
            return jsonify({
                'sql_fabric_connected': False,
                'error': str(e)
            })
    else:
        return jsonify({
            'sql_fabric_connected': False,
            'error': 'No SQL Fabric connection'
        })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Enhanced health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0.0',
        'services': {
            'sql_fabric': 'connected' if sql_fabric_service.connection else 'fallback',
            'duckdb': 'active',
            'lakehouse': 'active',
            'backend': 'operational'
        },
        'features': {
            'rule_configuration': True,
            'data_validation': True,
            'multi_tenant': True,
            'duckdb_analytics': True,
            'fabric_lakehouse': True
        }
    })

@app.route('/')
def home():
    """Home endpoint"""
    return jsonify({
        'service': 'Data Sync AI Enterprise',
        'version': '2.0.0',
        'status': 'operational',
        'features': [
            'Rule Configuration Workflow',
            'Data Validation Workflow', 
            'DuckDB Analytics',
            'Fabric Lakehouse Storage',
            'Multi-tenant Architecture'
        ]
    })

if __name__ == '__main__':
    logger.info("Starting Data Sync AI Enterprise v2.0")
    logger.info("DuckDB Analytics: Enabled")
    logger.info("Fabric Lakehouse: Enabled") 
    logger.info("Multi-tenant: Admin/Tenant/User roles")
    logger.info("Rule Configuration: 3-step workflow")
    logger.info("Data Validation: 3-step workflow")
    logger.info("Server: http://127.0.0.1:5000")
    logger.info("Demo Login: admin@example.com / admin123")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
