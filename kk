import os
from google.cloud import spanner
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sL_dtwin/.sak/sa-dev-gudv-app-dtwndo-0_key.json"
project = 'vz-it-np-jpuv-dev-anpdo-0'
instance_id = "anp-spanner-instance"
database_id = "wireline_churn"
client = spanner.Client(project=project)
instance = client.instance(instance_id)
database = instance.database(database_id)

# Execute insert statements
try:
    # For INSERT statements, we need to use read-write transactions
    with database.batch() as batch:
        batch.execute_update(tb1_ins)
    print("Inserted data into test_table_1")
    
    with database.batch() as batch:
        batch.execute_update(tb2_ins)
    print("Inserted data into test_table_2")
except Exception as e:
    print(f"Error executing insert statements: {str(e)}")

# Execute join queries
join_queries = {
    "INNER JOIN": inner_join,
    "LEFT JOIN": left_join,
    "RIGHT JOIN": right_join,
    "FULL OUTER JOIN": full_outer_join
}

try:
    for join_type, query in join_queries.items():
        print(f"\n--- {join_type} Results ---")
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            column_names = [column[0] for column in results.metadata.row_type.fields]
            print(" | ".join(column_names))
            print("-" * 80)
            for row in results:
                print(" | ".join(str(value) for value in row))
except Exception as e:
    print(f"Error executing join queries: {str(e)}")

# Original count queries
try:
    for i in count_queries:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(i)
            for row in results:
                print(row)
except Exception as e:
    print(f"Error executing select statement: {str(e)}")
