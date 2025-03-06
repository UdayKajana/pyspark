error executing select statement: 400 Table not found: wireline_churn.test_table_2 [at 1:19]\nSELECT t1.id FROM wireline_churn.test_table_2 AS t1 INNER JOIN network_wls_mo...\n                  ^ [locale: "en-US"
message: "Table not found: wireline_churn.test_table_2 [at 1:19]\nSELECT t1.id FROM wireline_churn.test_table_2 AS t1 INNER JOIN network_wls_mo...\n 


import os
from google.cloud import spanner
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sL_dtwin/.sak/sa-dev-gudv-app-dtwndo-0_key.json"
project = 'vz-it-np-jpuv-dev-anpdo-0'
instance_id = "anp-spanner-instance"
database_id = "wireline_churn"
client = spanner.Client(project=project)
instance = client.instance(instance_id)
database = instance.database(database_id)
# gcloud spanner databases execute-sql wireline_churn --instance=anp-spanner-instance --sql=

q= 'SELECT t1.id FROM wireline_churn.test_table_2 AS t1 INNER JOIN network_wls_model.test_table_1 AS t2 ON t1.id = t2.id'

try:
    for i in [q]:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(i)
            for row in results:
                print(row)
except Exception as e:
    print(f"error executing select statement: {str(e)}")

