import os
from google.cloud import spanner

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sL_dtwin/.sak/sa-dev-gudv-app-dtwndo-0_key.json"

# Set up client
project = 'vz-it-np-jpuv-dev-anpdo-0'
instance_id = "anp-spanner-instance"
client = spanner.Client(project=project)
instance = client.instance(instance_id)

def execute_ddl(database, ddl_statements):
    """Execute DDL statements (CREATE TABLE, etc.)"""
    try:
        operation = database.update_ddl(ddl_statements)
        print(f"Waiting for operation to complete...")
        operation.result()
        print(f"DDL statements executed successfully.")
        return True
    except Exception as e:
        print(f"Error executing DDL statements: {str(e)}")
        return False

def execute_dml(database, dml_statement):
    """Execute DML statements (INSERT, UPDATE, DELETE)"""
    try:
        def update_data(transaction):
            row_count = transaction.execute_update(dml_statement)
            return row_count
        
        row_count = database.run_in_transaction(update_data)
        print(f"Modified {row_count} rows.")
        print(f"DML statement executed successfully.")
        return True
    except Exception as e:
        print(f"Error executing DML statement: {str(e)}")
        return False

def execute_query(database, query):
    """Execute a query and print results"""
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            
            # Check if results is None
            if results is None:
                print("Query executed but returned no results.")
                return
                
            # Get column names and print them
            try:
                column_names = [column[0] for column in results.metadata.row_type.fields]
                print(" | ".join(column_names))
                print("-" * 80)
            
                # Print results
                row_count = 0
                for row in results:
                    row_count += 1
                    print(" | ".join(str(value) for value in row))
                
                print(f"\nTotal rows: {row_count}")
            except AttributeError:
                print("Query executed but no metadata or rows were returned.")
    except Exception as e:
        print(f"Error executing query: {str(e)}")

# Connect to the two databases
db1 = instance.database("wireline_churn")
db2 = instance.database("network_wls_model")

# 1. Create tables (if they don't exist)
create_table1 = [
    """CREATE TABLE IF NOT EXISTS test_table_2 (
        id INT64 NOT NULL,
        name STRING(MAX) NOT NULL
    ) PRIMARY KEY (id)"""
]

create_table2 = [
    """CREATE TABLE IF NOT EXISTS test_table_1 (
        id INT64 NOT NULL,
        name STRING(MAX) NOT NULL
    ) PRIMARY KEY (id)"""
]

print("Creating tables...")
tables_created1 = execute_ddl(db1, create_table1)
tables_created2 = execute_ddl(db2, create_table2)

# 2. Insert data - first check if tables already have data
check_data1 = "SELECT COUNT(*) as count FROM test_table_2"
check_data2 = "SELECT COUNT(*) as count FROM test_table_1"

print("\nChecking if tables already have data...")
try:
    with db1.snapshot() as snapshot:
        results = snapshot.execute_sql(check_data1)
        row = next(iter(results))
        count1 = row[0]
        print(f"Table test_table_2 has {count1} rows")
except Exception as e:
    print(f"Error checking table data: {str(e)}")
    count1 = 0

try:
    with db2.snapshot() as snapshot:
        results = snapshot.execute_sql(check_data2)
        row = next(iter(results))
        count2 = row[0]
        print(f"Table test_table_1 has {count2} rows")
except Exception as e:
    print(f"Error checking table data: {str(e)}")
    count2 = 0

# Insert data only if tables are empty
if count1 == 0:
    insert_table1 = """INSERT INTO test_table_2 (id, name)
    VALUES 
    (1, 'John Smith'),
    (2, 'Mary Johnson'),
    (3, 'Robert Brown'),
    (4, 'Patricia Davis'),
    (5, 'James Wilson'),
    (11, 'Susan Harris'),
    (12, 'David Clark'),
    (13, 'Nancy Lewis'),
    (14, 'Joseph Walker'),
    (15, 'Karen Hall')"""

    print("\nInserting data into table test_table_2...")
    execute_dml(db1, insert_table1)

if count2 == 0:
    insert_table2 = """INSERT INTO test_table_1 (id, name)
    VALUES 
    (1, 'John Smith'),
    (2, 'Mary Johnson'),
    (3, 'Robert Brown'),
    (4, 'Patricia Davis'),
    (5, 'James Wilson'),
    (6, 'Linda Martinez'),
    (7, 'Michael Anderson'),
    (8, 'Elizabeth Thomas'),
    (9, 'William Jackson'),
    (10, 'Jennifer White')"""

    print("\nInserting data into table test_table_1...")
    execute_dml(db2, insert_table2)

# 3. Display table contents
print("\nData in wireline_churn.test_table_2:")
query1 = "SELECT id, name FROM test_table_2 ORDER BY id"
execute_query(db1, query1)

print("\nData in network_wls_model.test_table_1:")
query2 = "SELECT id, name FROM test_table_1 ORDER BY id"
execute_query(db2, query2)

# 4. Execute join queries
print("\nExecuting INNER JOIN query:")
inner_join = """
SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name
FROM test_table_2 as t1
INNER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id
ORDER BY t1.id"""
execute_query(db1, inner_join)

print("\nExecuting LEFT JOIN query:")
left_join = """
SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name
FROM test_table_2 as t1
LEFT OUTER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id
ORDER BY t1.id"""
execute_query(db1, left_join)

print("\nExecuting RIGHT JOIN query:")
right_join = """
SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name
FROM test_table_2 as t1
RIGHT OUTER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id
ORDER BY COALESCE(t1.id, t2.id)"""
execute_query(db1, right_join)

print("\nExecuting FULL OUTER JOIN query:")
full_join = """
SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name
FROM test_table_2 as t1
FULL OUTER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id
ORDER BY COALESCE(t1.id, t2.id)"""
execute_query(db1, full_join)
