#!/usr/bin/env python3
"""
diagnose_table_names.py - Find actual table names and columns in your Fabric warehouse

This script identifies:
1. All tables in your warehouse
2. Actual column names in cache table
3. Schema names (case-sensitive!)
4. What needs to be fixed
"""

import sys

print("\n" + "="*80)
print("  TABLE NAME & COLUMN DIAGNOSTIC")
print("="*80 + "\n")

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: List all tables
# ──────────────────────────────────────────────────────────────────────────────

print("[STEP 1/3] Finding all tables in warehouse...\n")

try:
    from db_service import run_warehouse_df
    from config import Config
    
    # Get all tables
    sql = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    
    tables_df = run_warehouse_df(sql)
    
    if tables_df.empty:
        print("  ✗ No tables found - connection issue?")
        exit(1)
    
    print(f"  ✓ Found {len(tables_df)} tables:\n")
    
    # Group by schema
    schemas = tables_df['TABLE_SCHEMA'].unique()
    
    for schema in sorted(schemas):
        schema_tables = tables_df[tables_df['TABLE_SCHEMA'] == schema]
        print(f"  Schema: {schema}")
        
        for _, row in schema_tables.iterrows():
            table = row['TABLE_NAME']
            
            # Highlight important tables
            if 'FACT' in table.upper() or 'fact' in table:
                print(f"    → [{table}] ⭐ FACT TABLE")
            elif 'VENDOR' in table.upper() or 'vendor' in table:
                print(f"    → [{table}] ⭐ VENDOR TABLE")
            elif 'CACHE' in table.upper() or 'cache' in table:
                print(f"    → [{table}] ⭐ CACHE TABLE")
            else:
                print(f"    → [{table}]")
        
        print()

except Exception as e:
    print(f"  ✗ Error: {e}\n")
    import traceback
    traceback.print_exc()
    exit(1)

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Check QUERY_RESULT_CACHE columns
# ──────────────────────────────────────────────────────────────────────────────

print("="*80)
print("[STEP 2/3] Checking QUERY_RESULT_CACHE columns...\n")

try:
    # Try different table names
    cache_table_names = [
        'QUERY_RESULT_CACHE',
        'query_result_cache',
        'QueryResultCache',
        'Query_Result_Cache',
    ]
    
    cache_found = False
    
    for cache_name in cache_table_names:
        try:
            col_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{cache_name}'
            ORDER BY ORDINAL_POSITION
            """
            
            cols_df = run_warehouse_df(col_sql)
            
            if not cols_df.empty:
                print(f"  ✓ Found table: [{cache_name}]\n")
                print(f"  Columns ({len(cols_df)}):\n")
                
                for _, row in cols_df.iterrows():
                    col = row['COLUMN_NAME']
                    dtype = row['DATA_TYPE']
                    print(f"    - {col:25} : {dtype}")
                
                cache_found = True
                
                # Check what's wrong
                print(f"\n  Issue Analysis:")
                expected = [
                    'CACHE_KEY', 'QUESTION_HASH', 'QUESTION_TEXT', 'GENERATED_SQL',
                    'RESULT_JSON', 'ROW_COUNT', 'CREATED_AT', 'EXPIRES_AT', 'HIT_COUNT'
                ]
                
                actual = [row['COLUMN_NAME'].upper() for _, row in cols_df.iterrows()]
                
                missing = set(expected) - set(actual)
                extra = set(actual) - set(expected)
                
                if missing:
                    print(f"\n  ✗ Missing columns: {missing}")
                if extra:
                    print(f"  ⚠ Extra columns: {extra}")
                
                if not missing and not extra:
                    print(f"\n  ✓ All columns correct!")
                
                break
        except:
            continue
    
    if not cache_found:
        print(f"  ✗ QUERY_RESULT_CACHE table not found with any variation")
        print(f"     Tried: {cache_table_names}")
        print(f"\n  Note: You may need to recreate this table")
    
    print()

except Exception as e:
    print(f"  ✗ Error: {e}\n")
    import traceback
    traceback.print_exc()

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Summary and recommendations
# ──────────────────────────────────────────────────────────────────────────────

print("="*80)
print("[STEP 3/3] Summary & Recommendations\n")

print("FINDINGS:\n")

print("1. Table Names (note the CASE - it's case-sensitive!):")
print("   - Check which tables exist and their exact spelling")
print("   - Update all references in code to match exactly\n")

print("2. QUERY_RESULT_CACHE:")
if cache_found:
    if missing:
        print(f"   ✗ MISSING COLUMNS: {missing}")
        print("     → Need to recreate table or alter to add missing columns\n")
    elif extra:
        print(f"   ⚠ EXTRA COLUMNS (unexpected): {extra}")
        print("     → May cause issues if code expects different columns\n")
    else:
        print("   ✓ All columns present\n")
else:
    print("   ✗ TABLE NOT FOUND")
    print("     → Need to create table or check warehouse\n")

print("3. INFORMATION_MART schema:")
print("   - Verify if tables are in this schema")
print("   - Check if schema name case matches code\n")

print("="*80)
print("\nNEXT STEPS:\n")

print("1. Run this diagnostic and note actual table names")
print("2. Update config.py with correct schema/table names")
print("3. Update data_validation.py with correct table references")
print("4. If QUERY_RESULT_CACHE is wrong, recreate it:")
print("   DROP TABLE [dbo].[QUERY_RESULT_CACHE]")
print("   CREATE TABLE [dbo].[QUERY_RESULT_CACHE] (")
print("       CACHE_KEY VARCHAR(64),")
print("       QUESTION_HASH VARCHAR(64),")
print("       QUESTION_TEXT VARCHAR(2000),")
print("       GENERATED_SQL VARCHAR(8000),")
print("       RESULT_JSON VARCHAR(8000),")
print("       ROW_COUNT INT,")
print("       CREATED_AT VARCHAR(30),")
print("       EXPIRES_AT VARCHAR(30),")
print("       HIT_COUNT INT")
print("   )")
print("5. Restart Streamlit and test\n")

print("="*80 + "\n")
