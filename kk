# Don't parse the template string directly, use it as is
date_template = '{{ (data_interval_end - macros.timedelta(days=2, hours=1)).strftime("%Y-%m-%d") }}'
hour_template = '{{ (data_interval_end - macros.timedelta(days=2, hours=1)).strftime("%Y%m%d%H") }}'

# Let Airflow handle the template rendering at runtime
str_query = str(config_values['query']).replace('CURRENT_DATE_VALUE', f"'{date_template}'") 
str_query = str_query.replace('CURRENT_DT_HR_VALUE', f"'{hour_template}'")
