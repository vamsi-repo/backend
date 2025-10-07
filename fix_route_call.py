"""
Quick fix to remove register_data_validation_routes call
"""

file_path = r"C:\Users\MOM\Downloads\DATA_SYNC_AI\BACKEND\app_complete.py"

# Read file
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and remove the line
new_lines = []
removed = False
for i, line in enumerate(lines):
    if 'register_data_validation_routes' in line:
        print(f"Line {i+1}: {line.strip()}")
        print("REMOVING THIS LINE")
        removed = True
        continue  # Skip this line
    new_lines.append(line)

# Write back
if removed:
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("\n✓ File updated successfully!")
else:
    print("\n✗ Line not found")
