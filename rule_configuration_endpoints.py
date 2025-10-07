"""
Rule Configuration Workflow Endpoints
This file contains the complete rule configuration workflow (separate from data validation):

WORKFLOW:
1. User uploads file for rule configuration
2. Step 1 - Select Column Headers: User selects which columns to configure rules for
3. Step 2 - Configure Rules: User drags and drops validation rules to columns
4. Step 3 - Review: User reviews configured rules before saving
"""

import os
import logging
import uuid
import json
import pandas as pd
from datetime import datetime
from flask import request, jsonify, session
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

def register_rule_configuration_routes(app, lakehouse_service, duckdb_service, sql_fabric_service):
    """Register all rule configuration endpoints"""
    
    @app.route('/api/rule-config/upload', methods=['POST'])
    def upload_file_for_rule_config():
        """
        STEP 0: Upload file for rule configuration with Smart Template Detection
        - User uploads Excel/CSV file to create a new template
        - System uses DuckDB to analyze file structure
        - Checks for existing templates with matching headers
        - If match found with rules configured, redirects to Step 3
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        try:
            user = session.get('user')
            session_id = str(uuid.uuid4())
            filename = secure_filename(file.filename)
            
            # Save file temporarily
            temp_path = os.path.join('temp', f"{session_id}_{filename}")
            os.makedirs('temp', exist_ok=True)
            file.save(temp_path)
            
            logger.info(f"[ENTERPRISE] File upload initiated: {filename} by user {user.get('email', 'unknown')}")
            
            # WORKFLOW STEP 1: Use DuckDB to analyze file
            logger.info(f"[DUCKDB] Analyzing file structure for {filename}")
            analysis_result = duckdb_service.analyze_file(temp_path, session_id)
            
            if 'error' in analysis_result:
                os.remove(temp_path)
                logger.error(f"[DUCKDB] File analysis failed: {analysis_result['error']}")
                return jsonify({'error': f"File analysis failed: {analysis_result['error']}"}), 400
            
            headers = analysis_result['headers']
            row_count = analysis_result['row_count']
            column_types = analysis_result['column_types']
            
            logger.info(f"[DUCKDB] Analysis complete - Rows: {row_count}, Columns: {len(headers)}")
            
            # Build sheets structure for response
            if filename.lower().endswith('.csv'):
                sheets = {'Sheet1': {'headers': headers}}
                sheet_name = 'Sheet1'
            else:
                # For Excel, use the first sheet or detected sheet
                sheet_name = 'Sheet1'
                sheets = {sheet_name: {'headers': headers}}
            
            # SMART TEMPLATE DETECTION: Check for existing template with matching headers
            cursor = sql_fabric_service.connection.cursor()
            
            # Sort headers for consistent comparison
            sorted_headers = sorted(headers)
            headers_json = json.dumps(sorted_headers)
            
            logger.info(f"[TEMPLATE DETECTION] Searching for existing templates with matching structure")
            
            # Find existing template with same headers and active rules
            cursor.execute("""
                SELECT TOP 1
                    t.template_id,
                    t.template_name,
                    t.file_name,
                    t.headers,
                    t.status,
                    t.created_at,
                    COUNT(DISTINCT cvr.rule_type_id) as rule_count
                FROM excel_templates t
                LEFT JOIN template_columns tc ON t.template_id = tc.template_id
                LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                WHERE t.user_id = ?
                AND t.status = 'ACTIVE'
                GROUP BY t.template_id, t.template_name, t.file_name, t.headers, t.status, t.created_at
                HAVING COUNT(DISTINCT cvr.rule_type_id) > 0
                ORDER BY t.created_at DESC
            """, user['id'])
            
            existing_templates = cursor.fetchall()
            
            # Check if any existing template has matching headers
            matching_template = None
            for template in existing_templates:
                template_headers = json.loads(template[3]) if template[3] else []
                sorted_template_headers = sorted(template_headers)
                
                if sorted_template_headers == sorted_headers:
                    matching_template = template
                    logger.info(f"[TEMPLATE DETECTION] ✓ Match found! Template ID: {template[0]}, Name: {template[1]}")
                    break
            
            # If matching template found with rules, redirect to Step 3
            if matching_template:
                template_id = matching_template[0]
                template_name = matching_template[1]
                rule_count = matching_template[6]
                
                # Clean up temp file
                os.remove(temp_path)
                
                # Get configured rules for this template
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
                
                # Build rules config
                configured_rules = {}
                selected_headers = set()
                for row in rules_rows:
                    column_name = row[0]
                    rule_name = row[1]
                    selected_headers.add(column_name)
                    if column_name not in configured_rules:
                        configured_rules[column_name] = []
                    if rule_name not in configured_rules[column_name]:
                        configured_rules[column_name].append(rule_name)
                
                # Store in session for Step 3
                session['rule_config_session_id'] = session_id
                session['rule_config_template_id'] = template_id
                session['rule_config_template_name'] = template_name
                session['rule_config_filename'] = filename
                session['rule_config_headers'] = headers
                session['rule_config_selected_headers'] = list(selected_headers)
                session['rule_config_rules'] = configured_rules
                
                logger.info(f"[TEMPLATE DETECTION] Redirecting to Step 3 - {rule_count} rules already configured")
                
                return jsonify({
                    'success': True,
                    'message': f'Template already configured with {rule_count} rules. Showing existing configuration.',
                    'has_existing_rules': True,
                    'redirect_to_step': 3,
                    'template_id': template_id,
                    'template_name': template_name,
                    'filename': filename,
                    'sheets': sheets,
                    'sheet_name': sheet_name,
                    'headers': headers,
                    'selected_headers': list(selected_headers),
                    'configured_rules': configured_rules,
                    'rule_count': rule_count,
                    'analysis': {
                        'row_count': row_count,
                        'column_count': len(headers),
                        'column_types': column_types
                    }
                })
            
            # No matching template found - create new one
            logger.info(f"[TEMPLATE DETECTION] No matching template found - creating new template")
            
            # Extract template name from filename (remove extension)
            template_name = os.path.splitext(filename)[0]
            
            # Generate next template_id
            cursor.execute("SELECT ISNULL(MAX(template_id), 0) + 1 FROM excel_templates")
            template_id = cursor.fetchone()[0]
            
            # Insert new template with explicit template_id
            cursor.execute("""
                INSERT INTO excel_templates (template_id, user_id, template_name, file_name, headers, status, created_at)
                VALUES (?, ?, ?, ?, ?, 'PENDING', GETDATE())
            """, template_id, user['id'], template_name, filename, json.dumps(headers))
            
            # template_id already set above
            
            if not template_id:
                raise Exception("Failed to create template")
            
            sql_fabric_service.connection.commit()
            
            # Store in session
            session['rule_config_session_id'] = session_id
            session['rule_config_template_id'] = template_id
            session['rule_config_template_name'] = template_name
            session['rule_config_filename'] = filename
            session['rule_config_temp_path'] = temp_path
            session['rule_config_sheet_name'] = sheet_name
            session['rule_config_headers'] = headers
            
            logger.info(f"[TEMPLATE CREATION] New template created: ID={template_id}, Name={template_name}")
            
            return jsonify({
                'success': True,
                'message': 'New template created. Please configure validation rules.',
                'template_id': template_id,
                'template_name': template_name,
                'filename': filename,
                'sheets': sheets,
                'sheet_name': sheet_name,
                'headers': headers,
                'file_path': temp_path,
                'has_existing_rules': False,
                'redirect_to_step': 1,
                'analysis': {
                    'row_count': row_count,
                    'column_count': len(headers),
                    'column_types': column_types
                }
            })
            
        except Exception as e:
            logger.error(f"[ERROR] File upload for rule configuration failed: {e}")
            import traceback
            traceback.print_exc()
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/rule-config/select-headers', methods=['POST'])
    def select_headers_for_rules():
        """
        STEP 1: Select Column Headers
        - User selects which columns to configure validation rules for
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        try:
            data = request.get_json()
            selected_headers = data.get('selected_headers', [])
            
            if not selected_headers:
                return jsonify({'error': 'No headers selected'}), 400
            
            template_id = session.get('rule_config_template_id')
            
            if not template_id:
                return jsonify({'error': 'No active rule configuration session'}), 400
            
            cursor = sql_fabric_service.connection.cursor()
            
            # Delete existing columns for this template
            cursor.execute("DELETE FROM template_columns WHERE template_id = ?", template_id)
            
            # Insert selected columns with explicit column_id generation
            for header in selected_headers:
                # Generate next column_id
                cursor.execute("SELECT ISNULL(MAX(column_id), 0) + 1 FROM template_columns")
                column_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO template_columns (column_id, template_id, column_name, is_selected)
                    VALUES (?, ?, ?, 1)
                """, column_id, template_id, header)
            
            sql_fabric_service.connection.commit()
            
            # Store in session
            session['rule_config_selected_headers'] = selected_headers
            
            logger.info(f"Headers selected for template {template_id}: {selected_headers}")
            
            return jsonify({
                'success': True,
                'message': 'Headers selected successfully',
                'selected_headers': selected_headers
            })
            
        except Exception as e:
            logger.error(f"Header selection failed: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/rule-config/configure-rules', methods=['POST'])
    def configure_validation_rules():
        """
        STEP 2: Configure Validation Rules
        - User drags and drops validation rules to selected columns
        - Action can be 'save' or 'review'
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        try:
            data = request.get_json()
            rules_config = data.get('validations', {})
            action = data.get('action', 'save')
            
            if not rules_config:
                return jsonify({'error': 'No rules configured'}), 400
            
            template_id = session.get('rule_config_template_id')
            
            if not template_id:
                return jsonify({'error': 'No active rule configuration session'}), 400
            
            cursor = sql_fabric_service.connection.cursor()
            
            # Get column IDs for selected headers
            cursor.execute("""
                SELECT column_id, column_name 
                FROM template_columns 
                WHERE template_id = ? AND is_selected = 1
            """, template_id)
            
            columns = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Delete existing rules for these columns
            cursor.execute("""
                DELETE FROM column_validation_rules 
                WHERE column_id IN (
                    SELECT column_id FROM template_columns WHERE template_id = ?
                )
            """, template_id)
            
            # Insert new rules
            for column_name, rules in rules_config.items():
                if column_name in columns:
                    column_id = columns[column_name]
                    
                    for rule_name in rules:
                        # Get rule_type_id
                        cursor.execute("""
                            SELECT rule_type_id 
                            FROM validation_rule_types 
                            WHERE rule_name = ?
                        """, rule_name)
                        
                        rule_row = cursor.fetchone()
                        if rule_row:
                            rule_type_id = rule_row[0]
                            
                            # Generate column_validation_id manually
                            cursor.execute("SELECT ISNULL(MAX(column_validation_id), 0) + 1 FROM column_validation_rules")
                            column_validation_id = cursor.fetchone()[0]
                            
                            cursor.execute("""
                                INSERT INTO column_validation_rules (column_validation_id, column_id, rule_type_id)
                                VALUES (?, ?, ?)
                            """, column_validation_id, column_id, rule_type_id)
            
            # Update template status based on action
            if action == 'save':
                cursor.execute("""
                    UPDATE excel_templates 
                    SET status = 'ACTIVE', updated_at = GETDATE()
                    WHERE template_id = ?
                """, template_id)
            
            sql_fabric_service.connection.commit()
            
            # Store in session
            session['rule_config_rules'] = rules_config
            
            logger.info(f"Rules configured for template {template_id}: {rules_config}")
            
            return jsonify({
                'success': True,
                'message': 'Rules configured successfully' if action == 'save' else 'Rules saved for review',
                'action': action
            })
            
        except Exception as e:
            logger.error(f"Rule configuration failed: {e}")
            import traceback
            traceback.print_exc()
            sql_fabric_service.connection.rollback()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/rule-config/get-template/<int:template_id>', methods=['GET'])
    def get_template_for_config(template_id):
        """
        Get template information including headers and existing rules
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        cursor = None
        try:
            user = session.get('user')
            cursor = sql_fabric_service.connection.cursor()
            
            # Get template info
            cursor.execute("""
                SELECT template_id, template_name, file_name, headers, status
                FROM excel_templates
                WHERE template_id = ? AND user_id = ?
            """, template_id, user['id'])
            
            template_row = cursor.fetchone()
            
            if not template_row:
                return jsonify({'error': 'Template not found'}), 404
            
            headers = json.loads(template_row[3]) if template_row[3] else []
            sheet_name = 'Sheet1'  # Default sheet name
            
            # Check if template has existing rules
            cursor.execute("""
                SELECT COUNT(*) 
                FROM template_columns tc
                JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                WHERE tc.template_id = ?
            """, template_id)
            
            has_existing_rules = cursor.fetchone()[0] > 0
            
            # Build sheets structure
            sheets = {sheet_name: {'headers': headers}}
            
            return jsonify({
                'success': True,
                'template_id': template_row[0],
                'template_name': template_row[1],
                'file_name': template_row[2],
                'sheets': sheets,
                'sheet_name': sheet_name,
                'has_existing_rules': has_existing_rules
            })
            
        except Exception as e:
            logger.error(f"Failed to get template: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    
    @app.route('/api/rule-config/get-template-rules/<int:template_id>', methods=['GET'])
    def get_template_rules(template_id):
        """
        Get configured rules for a template
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        cursor = None
        try:
            user = session.get('user')
            cursor = sql_fabric_service.connection.cursor()
            
            # Verify template belongs to user
            cursor.execute("""
                SELECT template_id FROM excel_templates
                WHERE template_id = ? AND user_id = ?
            """, template_id, user['id'])
            
            if not cursor.fetchone():
                return jsonify({'error': 'Template not found'}), 404
            
            # Get rules for each column
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
            
            # Build rules config
            rules = {}
            for row in rules_rows:
                column_name = row[0]
                rule_name = row[1]
                if column_name not in rules:
                    rules[column_name] = []
                if rule_name not in rules[column_name]:
                    rules[column_name].append(rule_name)
            
            return jsonify({
                'success': True,
                'rules': rules
            })
            
        except Exception as e:
            logger.error(f"Failed to get template rules: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    
    @app.route('/api/rule-config/get-all-rules', methods=['GET'])
    def get_all_validation_rules():
        """
        Get all available validation rules (basic + custom)
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        cursor = None
        try:
            # Create a fresh cursor for this request
            cursor = sql_fabric_service.connection.cursor()
            
            # Get all rules
            cursor.execute("""
                SELECT 
                    rule_type_id,
                    rule_name,
                    description,
                    parameters,
                    is_custom
                FROM validation_rule_types
                ORDER BY is_custom ASC, rule_name ASC
            """)
            
            rules_rows = cursor.fetchall()
            
            rules = []
            for row in rules_rows:
                rule = {
                    'rule_type_id': row[0],
                    'rule_name': row[1],
                    'description': row[2],
                    'parameters': json.loads(row[3]) if row[3] else {},
                    'is_custom': bool(row[4])
                }
                rules.append(rule)
            
            return jsonify({
                'success': True,
                'rules': rules
            })
            
        except Exception as e:
            logger.error(f"Failed to get validation rules: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
        finally:
            # Always close the cursor to free the connection
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    
    @app.route('/api/rule-config/finish', methods=['POST'])
    def finish_rule_configuration():
        """
        Finish rule configuration process
        - Clean up session data
        - Clean up temp files
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        try:
            temp_path = session.get('rule_config_temp_path')
            
            # Clean up temp file
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            
            # Clean up session variables (keep authentication)
            keys_to_remove = [
                'rule_config_session_id', 'rule_config_template_id', 'rule_config_template_name',
                'rule_config_filename', 'rule_config_temp_path', 'rule_config_sheet_name',
                'rule_config_headers', 'rule_config_selected_headers', 'rule_config_rules'
            ]
            
            for key in keys_to_remove:
                session.pop(key, None)
            
            logger.info("Rule configuration process finished and session cleaned up")
            
            return jsonify({
                'success': True,
                'message': 'Configuration completed successfully'
            })
            
        except Exception as e:
            logger.error(f"Failed to finish rule configuration: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/rule-config/history', methods=['GET'])
    def get_rule_configuration_history():
        """
        Get configuration history for current user
        - Returns all configured templates with metadata
        - Shows proper file names, dates, column counts, and rule counts
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        try:
            user = session.get('user')
            cursor = sql_fabric_service.connection.cursor()
            
            logger.info(f"[HISTORY] Fetching configuration history for user {user.get('email', 'unknown')}")
            
            # Get all templates with correct counts
            # FIXED: Count only SELECTED columns and rules on selected columns
            cursor.execute("""
                SELECT 
                    t.template_id,
                    t.template_name,
                    ISNULL(t.file_name, 'Unknown File') as file_name,
                    t.status,
                    t.created_at,
                    t.updated_at,
                    COUNT(DISTINCT CASE WHEN tc.is_selected = 1 THEN tc.column_id END) as column_count,
                    COUNT(DISTINCT CASE WHEN tc.is_selected = 1 THEN cvr.rule_type_id END) as rule_count
                FROM excel_templates t
                LEFT JOIN template_columns tc ON t.template_id = tc.template_id
                LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                WHERE t.user_id = ?
                GROUP BY t.template_id, t.template_name, t.file_name, t.status, t.created_at, t.updated_at
                ORDER BY t.updated_at DESC
            """, user['id'])
            
            history_rows = cursor.fetchall()
            
            history = []
            for row in history_rows:
                # Log each template for debugging
                logger.info(f"[HISTORY] Template: ID={row[0]}, Name={row[1]}, File={row[2]}, Columns={row[6]}, Rules={row[7]}")
                
                history.append({
                    'template_id': row[0],
                    'template_name': row[1],
                    'file_name': row[2],  # Now properly handled with ISNULL
                    'status': row[3],
                    'configured_date': row[4].isoformat() if row[4] else None,
                    'updated_date': row[5].isoformat() if row[5] else None,
                    'column_count': row[6],  # Now counts only selected columns
                    'rule_count': row[7]     # Now counts only rules on selected columns
                })
            
            logger.info(f"[HISTORY] Found {len(history)} configured templates")
            
            return jsonify({
                'success': True,
                'history': history
            })
            
        except Exception as e:
            logger.error(f"[HISTORY] ✗ Failed to get configuration history: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/rule-config/delete/<template_id>', methods=['DELETE'])
    def delete_template_configuration(template_id):
        """
        Delete a template configuration
        """
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        try:
            user = session.get('user')
            cursor = sql_fabric_service.connection.cursor()
            
            logger.info(f"[DELETE] Deleting template {template_id} for user {user.get('email', 'unknown')}")
            
            # Verify template belongs to user
            cursor.execute("""
                SELECT template_id FROM excel_templates
                WHERE template_id = ? AND user_id = ?
            """, template_id, user['id'])
            
            if not cursor.fetchone():
                logger.warning(f"[DELETE] Template {template_id} not found or unauthorized")
                return jsonify({'error': 'Template not found'}), 404
            
            # Delete template (cascade will handle related records)
            cursor.execute("""
                DELETE FROM excel_templates WHERE template_id = ?
            """, template_id)
            
            sql_fabric_service.connection.commit()
            
            logger.info(f"[DELETE] ✓ Template {template_id} deleted successfully")
            
            return jsonify({
                'success': True,
                'message': 'Template deleted successfully'
            })
            
        except Exception as e:
            logger.error(f"[DELETE] Failed to delete template: {e}")
            import traceback
            traceback.print_exc()
            sql_fabric_service.connection.rollback()
            return jsonify({'error': str(e)}), 500
    
    

    
    logger.info("Rule configuration endpoints registered successfully")
