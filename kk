pyvenv39) tpaldgudva001:/apps/opt/application/udayka/spd/bq_spaner/src/main/python$  python3 -m pipeline \
        --project vz-it-np-gudv-dev-dtwndo-0 \
        --runner DataflowRunner \
        --region us-east4 \
        --sdk_container_image us-east4-docker.pkg.dev/vz-it-np-gudv-dev-dtwndo-0/fwa-sssot/py39beam26:1.0.0 \
        --template_location gs://fwa_ssot_spanner_poc_v1/cicd/dataflow/wc/templates/ntwk_nqes_score_model_long \
        --temp_location gs://fwa_ssot_spanner_poc_v1/cicd/dataflow/wc/temp/ntwk_nqes_score_model_long \
        --staging_location gs://fwa_ssot_spanner_poc_v1/cicd/dataflow/wc/stg/ntwk_nqes_score_model_long \
        --spanner_project 'vz-it-np-jpuv-dev-anpdo-0' \
        --spanner_dataset 'network_wls_model' \
        --spanner_instance 'anp-spanner-instance' \
        --spanner_table 'ntwk_nqes_score_model_long' \
        --process 'ntwk_nqes_score_model_long' \
        --expansion_service 'localhost:50768'
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
I0000 00:00:1741943177.874807  660819 config.cc:230] gRPC experiments enabled: call_status_override_on_cancellation, event_engine_dns, event_engine_listener, http2_stats_fix, monitoring_experiment, pick_first_new, trace_record_callops, work_serializer_clears_time_cache
/apps/opt/application/vznet/pyvenv39/bin/python3: No module named build
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing required meta-data: url

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) should be supplied

