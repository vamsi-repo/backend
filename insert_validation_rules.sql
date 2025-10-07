-- Insert Default Validation Rules
-- Run this in your SQL Fabric database

-- First, clear existing rules (if any)
DELETE FROM column_validation_rules;
DELETE FROM validation_rule_types;

-- Insert basic validation rules
INSERT INTO validation_rule_types (rule_type_id, rule_name, rule_description, rule_category, is_active, created_at) VALUES
(1, 'Required', 'Field must not be empty', 'Basic', 1, GETDATE()),
(2, 'Text', 'Field accepts any text value', 'Basic', 1, GETDATE()),
(3, 'Int', 'Field must be an integer number', 'Basic', 1, GETDATE()),
(4, 'Float', 'Field must be a decimal/floating point number', 'Basic', 1, GETDATE()),
(5, 'Email', 'Field must be a valid email address', 'Basic', 1, GETDATE()),
(6, 'Date', 'Field must be a valid date', 'Basic', 1, GETDATE()),
(7, 'Alphanumeric', 'Field must contain only letters and numbers', 'Basic', 1, GETDATE()),
(8, 'Boolean', 'Field must be true/false or yes/no', 'Basic', 1, GETDATE()),
(9, 'Phone', 'Field must be a valid phone number', 'Extended', 1, GETDATE()),
(10, 'URL', 'Field must be a valid URL', 'Extended', 1, GETDATE());

-- Verify insertion
SELECT COUNT(*) as 'Total Rules Inserted' FROM validation_rule_types;
SELECT * FROM validation_rule_types ORDER BY rule_type_id;
