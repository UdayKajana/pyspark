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


inner_join = """
SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name
FROM test_table_2 as t1
INNER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id
ORDER BY t1.id"""
