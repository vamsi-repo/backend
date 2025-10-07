"""
Enterprise DuckDB In-Memory Service for Data Sync AI
Handles file analysis, processing, and validation in memory
"""
import duckdb
import logging
import pandas as pd
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import os

logger = logging.getLogger('data_sync_ai.duckdb')

class DuckDBService:
    """
    In-memory database service using DuckDB for high-performance file processing
    Following the exact workflows from your requirements
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Use persistent database file to survive Flask reloads
        db_path = os.path.join('temp', 'duckdb_validation.db')
        os.makedirs('temp', exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._init_memory_tables()
        self.logger.info(f"DuckDB initialized with persistent database at {db_path}")
    
    def _init_memory_tables(self):
        """Initialize in-memory tables for file processing"""
        try:
            # Table for file analysis results
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS file_analysis (
                    session_id VARCHAR,
                    filename VARCHAR,
                    headers VARCHAR[],
                    row_count INTEGER,
                    column_count INTEGER,
                    column_types JSON,
                    sample_data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Table for validation sessions
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS validation_sessions (
                    session_id VARCHAR PRIMARY KEY,
                    table_name VARCHAR,
                    filename VARCHAR,
                    row_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.logger.info("DuckDB memory tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"DuckDB initialization failed: {str(e)}")
            raise
    
    def analyze_file(self, file_path: str, session_id: str) -> Dict[str, Any]:
        """
        Analyze uploaded file using DuckDB - Rule Configuration Step 0
        
        Workflow: File Upload → DuckDB (load + analyze) → Template Creation
        
        Args:
            file_path: Path to the uploaded file
            session_id: Unique session identifier
            
        Returns:
            Dictionary with file analysis results
        """
        try:
            filename = os.path.basename(file_path)
            self.logger.info(f"Starting DuckDB analysis for file: {filename}")
            
            # Load file into DuckDB
            if file_path.lower().endswith('.csv'):
                # Read CSV with DuckDB (handles large files efficiently)
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE temp_analysis_{session_id} AS 
                    SELECT * FROM read_csv_auto('{file_path}')
                """)
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                # For Excel, read with pandas first then register with DuckDB
                df = pd.read_excel(file_path)
                self.conn.register(f'temp_analysis_{session_id}', df)
            else:
                raise ValueError(f"Unsupported file format: {filename}")
            
            # Analyze using DuckDB
            # Get column information
            columns_info = self.conn.execute(f"""
                SELECT column_name, data_type 
                FROM (DESCRIBE temp_analysis_{session_id})
            """).fetchall()
            
            headers = [col[0] for col in columns_info]
            column_types = {col[0]: col[1] for col in columns_info}
            
            # Get row count
            row_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM temp_analysis_{session_id}
            """).fetchone()[0]
            
            # Get sample data (first 5 rows)
            sample_data = self.conn.execute(f"""
                SELECT * FROM temp_analysis_{session_id} LIMIT 5
            """).fetchdf().to_dict('records')
            
            # Store analysis result
            self.conn.execute("""
                INSERT INTO file_analysis 
                (session_id, filename, headers, row_count, column_count, column_types, sample_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                session_id,
                filename,
                headers,
                row_count,
                len(headers),
                json.dumps(column_types),
                json.dumps(sample_data)
            ])
            
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
                self.conn.execute(f"DROP TABLE IF EXISTS temp_analysis_{session_id}")
            except:
                pass
    
    def load_file_for_validation(self, session_id: str, file_path: str, filename: str) -> Dict[str, Any]:
        """
        Load file into DuckDB for validation processing
        
        Workflow: File Upload → Lakehouse → DuckDB (load + process)
        
        Args:
            session_id: Unique session identifier
            file_path: Path to the uploaded file
            filename: Original filename
            
        Returns:
            Dictionary with load results
        """
        try:
            table_name = f"validation_data_{session_id.replace('-', '_')}"
            self.logger.info(f"Loading file into DuckDB table: {table_name}")
            
            # Load file into DuckDB with proper table name
            if file_path.lower().endswith('.csv'):
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{file_path}')
                """)
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
                self.conn.register(table_name, df)
            else:
                raise ValueError(f"Unsupported file format: {filename}")
            
            # Get row count
            row_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
            """).fetchone()[0]
            
            # Store validation session
            self.conn.execute("""
                INSERT INTO validation_sessions 
                (session_id, table_name, filename, row_count)
                VALUES (?, ?, ?, ?)
            """, [session_id, table_name, filename, row_count])
            
            self.logger.info(f"File loaded successfully: {row_count} rows in table {table_name}")
            
            return {
                'success': True,
                'session_id': session_id,
                'table_name': table_name,
                'row_count': row_count
            }
            
        except Exception as e:
            self.logger.error(f"File loading failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def validate_data_with_rules(self, table_name: str, rules_config: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Validate data against configured rules using DuckDB
        
        Workflow: Template Lookup → Rule Application → Error Detection
        
        Args:
            table_name: Name of the DuckDB table containing data
            rules_config: Dictionary mapping column names to list of rule names
            
        Returns:
            Dictionary with validation results
        """
        try:
            self.logger.info(f"Starting validation for table: {table_name}")
            self.logger.debug(f"Rules configuration: {rules_config}")
            
            # Get all data from table
            data_df = self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            errors = []
            error_rows = set()
            
            # Validate each column according to its rules
            for column_name, rules in rules_config.items():
                if column_name not in data_df.columns:
                    self.logger.warning(f"Column {column_name} not found in data")
                    continue
                
                for idx, value in data_df[column_name].items():
                    row_number = idx + 1  # 1-based row numbering
                    
                    for rule_name in rules:
                        error_message = self._apply_validation_rule(
                            value, rule_name, column_name, row_number
                        )
                        
                        if error_message:
                            errors.append({
                                'row_number': row_number,
                                'column_name': column_name,
                                'original_value': str(value) if pd.notna(value) else '',
                                'rule_type': rule_name,
                                'error_message': error_message
                            })
                            error_rows.add(row_number)
            
            # Get rows with errors
            if error_rows:
                error_rows_list = sorted(list(error_rows))
                rows_with_errors = data_df.iloc[[r-1 for r in error_rows_list]].to_dict('records')
            else:
                rows_with_errors = []
            
            self.logger.info(f"Validation completed: {len(errors)} errors found in {len(error_rows)} rows")
            
            return {
                'success': True,
                'total_rows': len(data_df),
                'error_count': len(errors),
                'error_rows_count': len(error_rows),
                'errors': errors,
                'rows_with_errors': rows_with_errors
            }
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def _apply_validation_rule(self, value: Any, rule_name: str, 
                               column_name: str, row_number: int) -> Optional[str]:
        """
        Apply a single validation rule to a value
        
        Args:
            value: The value to validate
            rule_name: Name of the rule to apply
            column_name: Name of the column
            row_number: Row number (1-based)
            
        Returns:
            Error message if validation fails, None if passes
        """
        try:
            # Required rule
            if rule_name.lower() == 'required':
                if pd.isna(value) or str(value).strip() == '':
                    return f"Required field is empty"
                return None
            
            # If value is empty/null and rule is not Required, skip validation
            if pd.isna(value) or str(value).strip() == '':
                return None
            
            value_str = str(value).strip()
            
            # Int rule
            if rule_name.lower() == 'int':
                try:
                    int(float(value_str))
                    return None
                except (ValueError, TypeError):
                    return f"Must be an integer"
            
            # Float rule
            if rule_name.lower() == 'float':
                try:
                    float(value_str)
                    return None
                except (ValueError, TypeError):
                    return f"Must be a decimal number"
            
            # Text rule
            if rule_name.lower() == 'text':
                if not isinstance(value, str) and not value_str.isalpha():
                    return f"Must be text only"
                return None
            
            # Email rule
            if rule_name.lower() == 'email':
                import re
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(email_pattern, value_str):
                    return f"Must be a valid email address"
                return None
            
            # Date rule
            if rule_name.lower() == 'date':
                try:
                    pd.to_datetime(value_str)
                    return None
                except:
                    return f"Must be a valid date"
            
            # Alphanumeric rule
            if rule_name.lower() == 'alphanumeric':
                if not value_str.isalnum():
                    return f"Must contain only letters and numbers"
                return None
            
            # Unknown rule - log warning but don't fail
            self.logger.warning(f"Unknown validation rule: {rule_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error applying rule {rule_name}: {str(e)}")
            return f"Validation error: {str(e)}"
    
    def apply_corrections(self, table_name: str, corrections: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply user corrections to the data in DuckDB
        
        Args:
            table_name: Name of the DuckDB table
            corrections: List of corrections to apply
            
        Returns:
            Dictionary with correction results
        """
        try:
            self.logger.info(f"Applying {len(corrections)} corrections to table: {table_name}")
            
            # Get current data
            data_df = self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Apply corrections
            corrections_applied = 0
            for correction in corrections:
                row_number = correction.get('row_number')
                column_name = correction.get('column_name')
                corrected_value = correction.get('corrected_value')
                
                if row_number and column_name and column_name in data_df.columns:
                    # Convert to 0-based index
                    idx = row_number - 1
                    if 0 <= idx < len(data_df):
                        data_df.at[idx, column_name] = corrected_value
                        corrections_applied += 1
            
            # Update table with corrected data
            self.conn.register(f"{table_name}_corrected", data_df)
            
            self.logger.info(f"Applied {corrections_applied} corrections successfully")
            
            return {
                'success': True,
                'corrections_applied': corrections_applied,
                'corrected_table': f"{table_name}_corrected"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to apply corrections: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def get_corrected_data(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Get corrected data as DataFrame for file export
        
        Args:
            table_name: Original table name
            
        Returns:
            DataFrame with corrected data or None
        """
        try:
            corrected_table = f"{table_name}_corrected"
            df = self.conn.execute(f"SELECT * FROM {corrected_table}").fetchdf()
            self.logger.info(f"Retrieved {len(df)} rows of corrected data")
            return df
        except Exception as e:
            self.logger.error(f"Failed to get corrected data: {str(e)}")
            return None
    
    def cleanup_session(self, session_id: str):
        """
        Clean up DuckDB tables for a completed session
        
        Args:
            session_id: Session identifier to clean up
        """
        try:
            # Get table name for this session
            result = self.conn.execute("""
                SELECT table_name FROM validation_sessions 
                WHERE session_id = ?
            """, [session_id]).fetchone()
            
            if result:
                table_name = result[0]
                
                # Drop tables
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}_corrected")
                
                # Remove from validation_sessions
                self.conn.execute("""
                    DELETE FROM validation_sessions WHERE session_id = ?
                """, [session_id])
                
                self.logger.info(f"Cleaned up DuckDB session: {session_id}")
        except Exception as e:
            self.logger.error(f"Session cleanup failed: {str(e)}")
    
    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a validation session"""
        try:
            result = self.conn.execute("""
                SELECT table_name, filename, row_count, created_at
                FROM validation_sessions 
                WHERE session_id = ?
            """, [session_id]).fetchone()
            
            if result:
                return {
                    'table_name': result[0],
                    'filename': result[1],
                    'row_count': result[2],
                    'created_at': result[3]
                }
            return None
        except Exception as e:
            self.logger.error(f"Failed to get session info: {str(e)}")
            return None
    
    def close(self):
        """Close DuckDB connection"""
        try:
            self.conn.close()
            self.logger.info("DuckDB connection closed")
        except Exception as e:
            self.logger.error(f"Error closing DuckDB connection: {str(e)}")
