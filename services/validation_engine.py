"""
Enterprise Validation Engine for Data Sync AI
Handles rule processing and validation logic following your existing patterns
"""
import logging
import re
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd
from .app_redis import AppRedis

logger = logging.getLogger('data_sync_ai.validation')

class ValidationEngine:
    """Enterprise validation engine for data processing"""
    
    def __init__(self, cache: AppRedis = None):
        """Initialize validation engine"""
        self.cache = cache
        self.accepted_date_formats = ['%d-%m-%Y', '%m-%Y', '%Y', '%Y-%m', '%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y']
        
        # Default validation rules following your app.py pattern
        self.default_rules = {
            'Required': {'allow_null': False},
            'Int': {'format': 'integer'},
            'Float': {'format': 'float'},
            'Text': {'allow_special': False},
            'Email': {'regex': r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'},
            'Date': {'format': '%d-%m-%Y'},
            'Boolean': {'format': 'boolean'},
            'Alphanumeric': {'format': 'alphanumeric'}
        }
        
        logger.info("Validation engine initialized")
    
    def validate_column_data(self, df: pd.DataFrame, column_name: str, validation_rules: List[str],
                           check_null_cells: bool = True) -> Tuple[int, List[Dict]]:
        """
        Validate column data against rules (following your existing check_special_characters_in_column pattern)
        Returns: (error_count, error_locations)
        """
        logger.debug(f"Validating column: {column_name}, rules: {validation_rules}")
        
        special_char_count = 0
        error_cell_locations = []
        
        try:
            for i, cell_value in enumerate(df[column_name], start=1):
                for rule in validation_rules:
                    errors = self._apply_single_rule(cell_value, rule, i, check_null_cells)
                    if errors:
                        special_char_count += len(errors)
                        error_cell_locations.extend(errors)
            
            return special_char_count, error_cell_locations
            
        except Exception as e:
            logger.error(f"Column validation failed for {column_name}: {str(e)}")
            raise
    
    def _apply_single_rule(self, cell_value: Any, rule: str, row_index: int, 
                          check_null_cells: bool) -> List[Dict]:
        """Apply single validation rule to a cell value"""
        errors = []
        
        try:
            # Handle null values
            if check_null_cells and pd.isna(cell_value):
                return [{
                    'row': row_index,
                    'value': 'NULL',
                    'rule_failed': rule,
                    'reason': 'Value is null'
                }]
            
            # Convert to string for validation
            cell_value_str = str(cell_value).strip() if pd.notna(cell_value) else ""
            
            # Apply specific rule validation
            if rule == 'Required':
                if not cell_value_str:
                    errors.append({
                        'row': row_index,
                        'value': 'EMPTY' if cell_value_str == '' else 'NULL',
                        'rule_failed': rule,
                        'reason': 'Value is empty' if cell_value_str == '' else 'Value is null'
                    })
            
            elif rule == 'Int':
                if cell_value_str and not self._is_integer(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Must be an integer'
                    })
            
            elif rule == 'Float':
                if cell_value_str and not self._is_float(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Must be a number (integer or decimal)'
                    })
            
            elif rule == 'Text':
                if cell_value_str and self._has_special_characters_except_quotes_and_parenthesis(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Contains invalid characters'
                    })
            
            elif rule == 'Email':
                if cell_value_str and not self._is_valid_email(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Invalid email format'
                    })
            
            elif rule == 'Date':
                if cell_value_str and not self._is_valid_date_format(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Invalid date format'
                    })
            
            elif rule == 'Boolean':
                if cell_value_str and not self._is_boolean(cell_value_str):
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': 'Must be a boolean (true/false or 0/1)'
                    })
            
            elif rule == 'Alphanumeric':
                if cell_value_str and not self._is_alphanumeric(cell_value_str):
                    invalid_chars = ''.join(c for c in cell_value_str if not c.isalnum())
                    errors.append({
                        'row': row_index,
                        'value': cell_value_str,
                        'rule_failed': rule,
                        'reason': f"Contains non-alphanumeric characters: {invalid_chars}" if invalid_chars else "Must contain only alphanumeric characters"
                    })
            
            return errors
            
        except Exception as e:
            logger.error(f"Rule application failed for {rule}: {str(e)}")
            return []
    
    # Validation helper methods (following your existing app.py patterns)
    
    def _is_integer(self, value: str) -> bool:
        """Check if value is a valid integer"""
        return value.replace('-', '', 1).isdigit()
    
    def _is_float(self, value: str) -> bool:
        """Check if value is a valid float"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _has_special_characters_except_quotes_and_parenthesis(self, value: str) -> bool:
        """Check for invalid special characters (following your existing logic)"""
        if not isinstance(value, str):
            return True
        
        for char in value:
            if char not in ['"', '(', ')'] and not char.isalpha() and char != ' ':
                return True
        return False
    
    def _is_valid_email(self, value: str) -> bool:
        """Validate email format"""
        email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return re.match(email_pattern, value) is not None
    
    def _is_valid_date_format(self, value: str) -> bool:
        """Check if value matches accepted date formats"""
        for date_format in self.accepted_date_formats:
            try:
                datetime.strptime(value, date_format)
                return True
            except ValueError:
                continue
        return False
    
    def _is_boolean(self, value: str) -> bool:
        """Check if value is a valid boolean"""
        return re.match(r'^(true|false|0|1)$', value, re.IGNORECASE) is not None
    
    def _is_alphanumeric(self, value: str) -> bool:
        """Check if value contains only alphanumeric characters"""
        return re.match(r'^[a-zA-Z0-9]+$', value) is not None
    
    # Custom rule processing
    
    def validate_custom_rule(self, df: pd.DataFrame, column_name: str, rule_definition: Dict,
                           headers: List[str]) -> List[Dict]:
        """Validate custom rule defined by user"""
        logger.debug(f"Applying custom rule to column: {column_name}")
        
        errors = []
        
        try:
            # Parse custom rule parameters
            parameters = json.loads(rule_definition.get('parameters', '{}'))
            logic = parameters.get('logic', 'AND')  # AND/OR logic
            base_rules = parameters.get('base_rules', [])
            
            for i, cell_value in enumerate(df[column_name], start=1):
                if logic == 'AND':
                    # All base rules must pass
                    valid = True
                    for base_rule in base_rules:
                        rule_errors = self._apply_single_rule(cell_value, base_rule, i, True)
                        if rule_errors:
                            valid = False
                            break
                    
                    if not valid:
                        errors.append({
                            'row': i,
                            'value': str(cell_value) if pd.notna(cell_value) else 'NULL',
                            'rule_failed': rule_definition.get('rule_name', 'Custom'),
                            'reason': f"Failed custom rule: {rule_definition.get('rule_name', 'Unknown')}"
                        })
                
                elif logic == 'OR':
                    # At least one base rule must pass
                    valid = False
                    for base_rule in base_rules:
                        rule_errors = self._apply_single_rule(cell_value, base_rule, i, True)
                        if not rule_errors:
                            valid = True
                            break
                    
                    if not valid:
                        errors.append({
                            'row': i,
                            'value': str(cell_value) if pd.notna(cell_value) else 'NULL',
                            'rule_failed': rule_definition.get('rule_name', 'Custom'),
                            'reason': f"Failed custom rule: {rule_definition.get('rule_name', 'Unknown')}"
                        })
            
            return errors
            
        except Exception as e:
            logger.error(f"Custom rule validation failed: {str(e)}")
            return []
    
    def validate_formula_rule(self, df: pd.DataFrame, column_name: str, formula: str,
                            headers: List[str], data_type: str) -> bool:
        """Evaluate column rule formula (following your evaluate_column_rule pattern)"""
        try:
            # Replace column names with df[column_name] references
            formula = formula.strip()
            for header in headers:
                formula = formula.replace(f"'{header}'", f"df['{header}']")
            
            # Evaluate formula safely
            result = eval(formula, {"df": df, "__builtins__": {}}, {})
            
            # Validate data type consistency
            valid = True
            for i, value in enumerate(df[column_name]):
                if pd.isna(value):
                    continue
                
                value_str = str(value).strip()
                
                if data_type == "Int" and not self._is_integer(value_str):
                    valid = False
                    break
                elif data_type == "Float" and not self._is_float(value_str):
                    valid = False
                    break
                elif data_type == "Text" and self._has_special_characters_except_quotes_and_parenthesis(value_str):
                    valid = False
                    break
                elif data_type == "Email" and not self._is_valid_email(value_str):
                    valid = False
                    break
                elif data_type == "Date" and not self._is_valid_date_format(value_str):
                    valid = False
                    break
                elif data_type == "Boolean" and not self._is_boolean(value_str):
                    valid = False
                    break
                elif data_type == "Alphanumeric" and not self._is_alphanumeric(value_str):
                    valid = False
                    break
            
            return result and valid
            
        except Exception as e:
            logger.error(f"Error evaluating formula rule {formula}: {str(e)}")
            return False
    
    # Validation result processing
    
    def process_validation_results(self, error_locations: Dict[str, List[Dict]],
                                 df: pd.DataFrame) -> Dict[str, Any]:
        """Process validation results for frontend display"""
        try:
            # Calculate error statistics
            total_errors = sum(len(errors) for errors in error_locations.values())
            affected_columns = len(error_locations)
            affected_rows = len(set(
                error['row'] for errors in error_locations.values() 
                for error in errors
            ))
            
            # Group errors by row for easier frontend processing
            row_errors = {}
            error_reasons = {}
            
            for column, errors in error_locations.items():
                for error in errors:
                    row_key = str(error['row'] - 1)  # Convert to 0-based index
                    
                    if row_key not in row_errors:
                        row_errors[row_key] = []
                        error_reasons[row_key] = ''
                    
                    row_errors[row_key].append({
                        'column': column,
                        'value': error['value'],
                        'rule_failed': error['rule_failed'],
                        'reason': error['reason']
                    })
                    
                    # Build error reason string
                    reason = 'Contains No Data' if error['value'] == 'NULL' and error['rule_failed'] == 'Required' else error['reason']
                    error_reasons[row_key] += f"{column}: Rule \"{error['rule_failed']}\" failed - {reason}; "
            
            return {
                'error_cell_locations': error_locations,
                'row_errors': row_errors,
                'error_reasons': error_reasons,
                'statistics': {
                    'total_errors': total_errors,
                    'affected_columns': affected_columns,
                    'affected_rows': affected_rows,
                    'total_rows': len(df),
                    'error_rate': round((affected_rows / len(df)) * 100, 2) if len(df) > 0 else 0
                },
                'data_rows': df.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error processing validation results: {str(e)}")
            raise
    
    def apply_corrections(self, df: pd.DataFrame, corrections: Dict[str, Dict[str, str]]) -> pd.DataFrame:
        """Apply corrections to DataFrame"""
        logger.info("Applying corrections to data")
        
        try:
            df_corrected = df.copy()
            correction_count = 0
            
            for column, row_corrections in corrections.items():
                if column in df_corrected.columns:
                    for row_str, corrected_value in row_corrections.items():
                        row_index = int(row_str)
                        if 0 <= row_index < len(df_corrected):
                            df_corrected.at[row_index, column] = corrected_value
                            correction_count += 1
            
            logger.info(f"Applied {correction_count} corrections")
            return df_corrected
            
        except Exception as e:
            logger.error(f"Error applying corrections: {str(e)}")
            raise
    
    def validate_correction_input(self, column: str, value: str, validation_rules: List[str]) -> Tuple[bool, str]:
        """Validate user input for corrections"""
        try:
            for rule in validation_rules:
                errors = self._apply_single_rule(value, rule, 1, True)
                if errors:
                    return False, errors[0]['reason']
            
            return True, "Valid input"
            
        except Exception as e:
            logger.error(f"Correction validation failed: {str(e)}")
            return False, f"Validation error: {str(e)}"
    
    def get_validation_suggestions(self, column: str, invalid_value: str, rule: str) -> List[str]:
        """Get suggestions for correcting invalid values"""
        suggestions = []
        
        try:
            if rule == 'Int':
                # Extract digits from value
                digits = re.findall(r'-?\d+', invalid_value)
                if digits:
                    suggestions.extend(digits[:3])  # Top 3 suggestions
            
            elif rule == 'Email':
                # Basic email correction suggestions
                if '@' not in invalid_value:
                    suggestions.append(f"{invalid_value}@example.com")
                elif '.' not in invalid_value.split('@')[-1]:
                    suggestions.append(f"{invalid_value}.com")
            
            elif rule == 'Date':
                # Try to parse partial dates
                numbers = re.findall(r'\d+', invalid_value)
                if len(numbers) >= 2:
                    # Try different date formats
                    if len(numbers[0]) == 4:  # Year first
                        suggestions.append(f"{numbers[1]}-{numbers[0] if len(numbers) > 2 else '01'}-{numbers[2] if len(numbers) > 2 else numbers[0]}")
                    else:  # Day/Month first
                        suggestions.append(f"{numbers[0]}-{numbers[1]}-{numbers[2] if len(numbers) > 2 else datetime.now().year}")
            
            elif rule == 'Alphanumeric':
                # Remove non-alphanumeric characters
                cleaned = re.sub(r'[^a-zA-Z0-9]', '', invalid_value)
                if cleaned:
                    suggestions.append(cleaned)
            
            elif rule == 'Boolean':
                # Boolean suggestions based on value
                lower_val = invalid_value.lower()
                if 'y' in lower_val or 'true' in lower_val or '1' in lower_val:
                    suggestions.extend(['true', '1', 'yes'])
                else:
                    suggestions.extend(['false', '0', 'no'])
            
            return suggestions[:5]  # Return top 5 suggestions
            
        except Exception as e:
            logger.error(f"Error generating suggestions: {str(e)}")
            return []
    
    def get_rule_description(self, rule: str) -> str:
        """Get user-friendly description of validation rule"""
        descriptions = {
            'Required': 'Field must not be empty or null',
            'Int': 'Must be a whole number (integer)',
            'Float': 'Must be a number (can include decimals)',
            'Text': 'Text field (letters, spaces, quotes, parentheses only)',
            'Email': 'Must be a valid email address format',
            'Date': 'Must be a valid date (DD-MM-YYYY format preferred)',
            'Boolean': 'Must be true/false, yes/no, or 1/0',
            'Alphanumeric': 'Must contain only letters and numbers (no spaces or symbols)'
        }
        
        return descriptions.get(rule, f'Custom validation rule: {rule}')
