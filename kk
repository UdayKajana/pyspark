import os
from google.cloud import spanner
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sL_dtwin/.sak/sa-dev-gudv-app-dtwndo-0_key.json"
project = 'vz-it-np-jpuv-dev-anpdo-0'
instance_id = "anp-spanner-instance"
database_id = "wireline_churn"
client = spanner.Client(project=project)
instance = client.instance(instance_id)
database = instance.database(database_id)
count_queries = ["SELECT 'nqes_site_scores_32' AS tablename, COUNT(*) AS cnt FROM nqes_site_scores_32",
                 "SELECT 'ntwk_nqes_score_model_scores_32' AS tablename, COUNT(*) AS cnt FROM ntwk_nqes_score_model_scores_32",
                 "SELECT 'ntwk_nqes_score_model_scores' AS tablename, COUNT(*) AS cnt FROM ntwk_nqes_score_model_scores",
                 "SELECT 'nqes_site_long' AS tablename, COUNT(*) AS cnt FROM nqes_site_long",
                 "SELECT 'nqes_site_long_32' AS tablename, COUNT(*) AS cnt FROM nqes_site_long_32",
                 "SELECT 'ntwk_nqes_score_model_long_32' AS tablename, COUNT(*) AS cnt FROM ntwk_nqes_score_model_long_32"]

delete_data = ['''DELETE FROM nqes_site_long_32 WHERE ""=""''',
'''DELETE FROM nqes_site_long WHERE ""=""''',
'''DELETE FROM nqes_site_scores_32 WHERE ""=""''',
'''DELETE FROM nqes_site_scores WHERE ""=""''',
'''DELETE FROM ntwk_nqes_score_model_long_32 WHERE ""=""''',
'''DELETE FROM ntwk_nqes_score_model_long WHERE ""=""''',
'''DELETE FROM ntwk_nqes_score_model_scores_32 WHERE ""=""''',
'''DELETE FROM ntwk_nqes_score_model_scores WHERE ""=""''',
'''DELETE FROM ug_cust_ntwk_usg_anomalous_sites WHERE ""=""''',
'''DELETE FROM ug_cust_ntwk_usg_daily WHERE ""=""''',
'''DELETE FROM ug_cust_ntwk_usg_hourly WHERE ""=""''',
'''DELETE FROM ug_imsi_level_samsung_nokia_raw_v1 WHERE ""=""''',
'''DELETE FROM ug_imsi_level_ericsson_raw_v1 WHERE ""=""''']
tb1_ins = """INSERT INTO network_wls_model.test_table_1 (id, name)
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
  (10, 'Jennifer White');"""

tb2_ins = """INSERT INTO wireline_churn.test_table_2 (id, name)
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
  (15, 'Karen Hall');"""


inner_join = """
SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM network_wls_model.test_table_1 t1
INNER JOIN wireline_churn.test_table_2 t2 ON t1.id = t2.id;"""

left_join = """ SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM network_wls_model.test_table_1 t1
LEFT OUTER JOIN wireline_churn.test_table_2 t2 ON t1.id = t2.id;"""

right_join ="""SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM network_wls_model.test_table_1 t1
RIGHT OUTER JOIN wireline_churn.test_table_2 t2 ON t1.id = t2.id;"""

full_outer_join = """SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM network_wls_model.test_table_1 t1
FULL OUTER JOIN wireline_churn.test_table_2 t2 ON t1.id = t2.id;"""


try:
    for i in count_queries:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(i)
            for row in results:
                print(row)
except Exception as e:
    print(f"error executing select statement: {str(e)}")

