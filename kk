Error message from worker: Traceback (most recent call last):
  File "apache_beam/runners/common.py", line 1501, in apache_beam.runners.common.DoFnRunner.process
  File "apache_beam/runners/common.py", line 689, in apache_beam.runners.common.SimpleInvoker.invoke_process
  File "apache_beam/runners/common.py", line 1677, in apache_beam.runners.common._OutputHandler.handle_process_outputs
  File "/apps/opt/application/udayka/spanner/bq_spaner/src/main/python/pipeline.py", line 42, in process
  File "/apps/opt/application/udayka/spanner/bq_spaner/src/main/python/pipeline.py", line 48, in transform_to_mutation
  File "/opt/apache/beam-venv/beam-venv-worker-sdk-1-0/lib/python3.9/site-packages/mutations/Mutations.py", line 629, in setMutation
    mhour_ts = Convertions.date_hour_to_timestamp(row.get('mhour')),
  File "/opt/apache/beam-venv/beam-venv-worker-sdk-1-0/lib/python3.9/site-packages/mutations/Mutations.py", line 668, in date_hour_to_timestamp
    formated_date_hr = datetime.strptime(str_date_value, "%Y-%m-%d%H")
  File "/usr/local/lib/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/local/lib/python3.9/_strptime.py", line 349, in _strptime
    raise ValueError("time data %r does not match format %r" %
ValueError: time data '2025030512' does not match format '%Y-%m-%d%H'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/sdk_worker.py", line 311, in _execute
    response = task()
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/sdk_worker.py", line 386, in <lambda>
    lambda: self.create_worker().do_instruction(request), request)
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/sdk_worker.py", line 656, in do_instruction
    return getattr(self, request_type)(
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/sdk_worker.py", line 694, in process_bundle
    bundle_processor.process_bundle(instruction_id))
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/bundle_processor.py", line 1119, in process_bundle
    input_op_by_transform_id[element.transform_id].process_encoded(
  File "/usr/local/lib/python3.9/site-packages/apache_beam/runners/worker/bundle_processor.py", line 237, in process_encoded
    self.output(decoded_value)


from typing import NamedTuple, Iterator, Optional
from datetime import date, datetime, timedelta
import logging

def getMutation(mutation):
    switcher = {
        "nqes_site_scores": NQESSiteScores,
        "ntwk_nqes_score_model_scores": NNQESScoreModelScores,
        "ntwk_nqes_score_model_scores_32": NNQESScoreModelScores32,
        "nqes_site_scores_32": NQESSiteScores32,
        "nqes_site_long": NQESSiteLong,
        "nqes_site_long_32": NQESSiteLong32,
        "ntwk_nqes_score_model_long": NNQESScoreModelLong,
        "ntwk_nqes_score_model_long_32": NNQESScoreModelLong32,
        "ug_customer_network_usage_anomalous_sites": UG_Customer_Network_Usage_Anomalous_Sites,
        "ug_customer_network_usage_hourly": UG_Customer_Network_Usage_Hourly,
        "ug_customer_network_usage_daily": UG_Customer_Network_Usage_Daily,
        "ug_imsi_level_samsung_nokia_raw_v1": UG_imsi_level_samsung_nokia_raw_v1,
        "ug_imsi_level_ericsson_raw_v1": UG_imsi_level_ericsson_raw_v1

    }
    return switcher.get(mutation)


class NQESSiteScores(NamedTuple):
    carrier: Optional[str]
    enodeb_id: Optional[str]
    gnb_du_id: Optional[str]
    lte_market_id: Optional[str]
    re_process_dt: Optional[str]
    rpt_dt: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_centile: Optional[int]
    score_decile: Optional[int]
    score_model_id: Optional[str]
    score_name: Optional[str]
    score_value: Optional[float]
    scoring_date: Optional[str]
    sector: Optional[int]
    site_id: Optional[str]
    trans_date: Optional[str]
    def setMutation(row):
        return NQESSiteScores(
            carrier=row.get('carrier'),
            enodeb_id=row.get('enodeb_id'),
            gnb_du_id=row.get('gnb_du_id'),
            lte_market_id=row.get('lte_market_id'),
            re_process_dt= Convertions.format_date(row.get('re_process_dt')),
            rpt_dt=Convertions.format_date(row.get('rpt_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('rpt_dt')),
            run_key=row.get('run_key'),
            score_centile=Convertions.safe_int(row.get('score_centile')),
            score_decile=Convertions.safe_int(row.get('score_decile')),
            score_model_id=row.get('score_model_id'),
            score_name=row.get('score_name'),
            score_value=Convertions.safe_float(row.get('score_value')),
            scoring_date=Convertions.format_date(row.get('scoring_date')),
            sector=Convertions.safe_int(row.get('sector')),
            site_id=row.get('site_id'),
            trans_date=Convertions.format_date(row.get('trans_date')))

class NNQESScoreModelScores(NamedTuple):
    report_date: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    acct_num: Optional[str]
    cust_id: Optional[str]
    cust_line_seq_id: Optional[str]
    imsi: Optional[str]
    score: Optional[float]
    score_name: Optional[str]
    scoring_date: Optional[str]
    trans_date: Optional[str]
    score_centile: Optional[int]
    score_decile: Optional[int]
    group_name: Optional[str]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NNQESScoreModelScores(
        report_date=Convertions.format_date(row.get('report_date')),
        rpt_dt_ts=Convertions.format_timestamp_dt(row.get('report_date')),
        run_key=row.get('run_key'),
        score_model_id=row.get('score_model_id'),
        acct_num=row.get('acct_num'),
        cust_id=row.get('cust_id'),
        cust_line_seq_id=row.get('cust_line_seq_id'),
        imsi=row.get('imsi'),
        score=round(row.get('cqi_per_session_avg'), 9) if row.get('cqi_per_session_avg') is not None else None,
        score_name=row.get('group_name'),
        scoring_date=Convertions.format_date(row.get('scoring_date')),
        trans_date=Convertions.format_date(row.get('trans_date')),
        score_centile=Convertions.safe_int(row.get('score_centile')),
        score_decile=Convertions.safe_int(row.get('score_decile')),
        group_name=row.get('group_name'),
        re_process_dt=Convertions.format_date(row.get('re_process_dt')))	

class NNQESScoreModelScores32(NamedTuple):
    rpt_dt: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    imsi: Optional[str]
    cust_id: Optional[str]
    cust_line_seq_id: Optional[str]
    acct_num: Optional[str]
    group_name: Optional[str]
    score_value: Optional[float]
    score_name: Optional[str]
    score_decile: Optional[int]
    score_centile: Optional[int]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NNQESScoreModelScores32(
            rpt_dt= Convertions.format_date(row.get('re_process_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('re_process_dt')),
            run_key = row.get('run_key')	,
            score_model_id = row.get('score_model_id')	,
            imsi = row.get('imsi')	,
            cust_id = row.get('cust_id')	,
            cust_line_seq_id = row.get('cust_line_seq_id')	,
            acct_num = row.get('acct_num')	,
            group_name = row.get('group_name')	,
            score_value= round(row.get('score_value'), 9) if row.get('score_value') is not None else None,
            score_name = row.get('score_name'),
            score_decile= Convertions.safe_int(row.get('score_decile')),
            score_centile= Convertions.safe_int(row.get('score_centile')),
            re_process_dt= Convertions.format_date(row.get('re_process_dt')),
            )

class NQESSiteScores32(NamedTuple):
    rpt_dt: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    enodeb_id: Optional[str]
    sector: Optional[int]
    carrier: Optional[str]
    gnb_du_id: Optional[str]
    site_id: Optional[str]
    score_value: Optional[float]
    score_name: Optional[str]
    score_decile: Optional[int]
    score_centile: Optional[int]
    lte_market_id: Optional[str]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NQESSiteScores32(
            rpt_dt=Convertions.format_date(row.get('rpt_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('rpt_dt')),
            run_key=row.get('run_key'),
            score_model_id=row.get('score_model_id'),
            enodeb_id=row.get('enodeb_id'),
            sector=Convertions.safe_int(row.get('sector')),
            carrier=row.get('carrier'),
            gnb_du_id=row.get('gnb_du_id'),
            site_id=row.get('site_id'),
            score_value=Convertions.safe_float(row.get('score_value')),
            score_name=row.get('score_name'),
            score_decile=Convertions.safe_int(row.get('score_decile')),
            score_centile=Convertions.safe_int(row.get('score_centile')),
            lte_market_id=row.get('lte_market_id'),
            re_process_dt=Convertions.format_date(row.get('re_process_dt'))
            )
    

class NQESSiteLong(NamedTuple):
    rpt_dt: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    enodeb_id: Optional[str]
    sector: Optional[int]
    carrier: Optional[str]
    gnb_du_id: Optional[str]
    site_id: Optional[str]
    feature: Optional[str]
    value: Optional[float]
    scoring_date: Optional[str]
    trans_date: Optional[str]
    is_imputed: Optional[int]
    lte_market_id: Optional[str]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NQESSiteLong(
            rpt_dt= Convertions.format_date(row.get('re_process_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('re_process_dt')),
            run_key= row.get('run_key'),
            score_model_id= row.get('score_model_id'),
            enodeb_id= row.get('enodeb_id'),
            sector=Convertions.safe_int(row.get('sector')),
            carrier= row.get('carrier'),
            gnb_du_id= row.get('gnb_du_id'),
            site_id= row.get('site_id'),
            feature= row.get('feature'),
            value= round(row.get('value'), 9) if row.get('value') is not None else None,
            scoring_date= Convertions.format_date(row.get('re_process_dt')),
            trans_date= Convertions.format_date(row.get('re_process_dt')),
            is_imputed=Convertions.safe_int(row.get('is_imputed')),
            lte_market_id= row.get('lte_market_id'),
            re_process_dt= Convertions.format_date(row.get('re_process_dt')),
        )

class NQESSiteLong32(NamedTuple):
    rpt_dt: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    enodeb_id: Optional[str]
    sector: Optional[int]
    carrier: Optional[str]
    gnb_du_id: Optional[str]
    site_id: Optional[str]
    feature: Optional[str]
    value: Optional[float]
    is_imputed: Optional[int]
    lte_market_id: Optional[str]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NQESSiteLong32(
            rpt_dt= Convertions.format_date(row.get('rpt_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('rpt_dt')),
            run_key= row.get('run_key'),
            score_model_id= row.get('score_model_id'),
            enodeb_id= row.get('enodeb_id'),
            sector= Convertions.safe_int(row.get('sector')),
            carrier= row.get('carrier'),
            gnb_du_id= row.get('gnb_du_id'),
            site_id= row.get('site_id'),
            feature= row.get('feature'),
            value= round(row.get('value'), 9) if row.get('value') is not None else None,
            is_imputed= Convertions.safe_int(row.get('is_imputed')),
            lte_market_id= row.get('lte_market_id'),
            re_process_dt= Convertions.format_date(row.get('re_process_dt')),
        )

class NNQESScoreModelLong(NamedTuple):
    report_date: Optional[str]
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    acct_num: Optional[str]
    cust_id: Optional[str]
    cust_line_seq_id: Optional[str]
    imsi: Optional[str]
    value: Optional[float]
    feature: Optional[str]
    is_imputed: Optional[int]
    scoring_date: Optional[str]
    trans_date: Optional[str]
    group_name: Optional[str]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NNQESScoreModelLong(
            report_date=  Convertions.format_date(row.get('report_date')),
            rpt_dt_ts = Convertions.format_timestamp_dt(row.get('report_date')),
            run_key= row.get('run_key'),
            score_model_id= row.get('score_model_id'),
            acct_num= row.get('acct_num'),
            cust_id= row.get('cust_id'),
            cust_line_seq_id= row.get('cust_line_seq_id'),
            imsi= row.get('imsi'),
            value= round(row.get('value'), 9) if row.get('value') is not None else None,
            feature= row.get('feature'),
            is_imputed= Convertions.safe_int(row.get('sector')),
            scoring_date=  Convertions.format_date(row.get('scoring_date')),
            trans_date=  Convertions.format_date(row.get('trans_date')),
            group_name= row.get('group_name'),
            re_process_dt=  Convertions.format_date(row.get('re_process_dt')),
        )

class NNQESScoreModelLong32(NamedTuple):
    rpt_dt: Optional[str]	
    rpt_dt_ts: Optional[str]
    run_key: Optional[str]
    score_model_id: Optional[str]
    acct_num: Optional[str]
    cust_id: Optional[str]
    cust_line_seq_id: Optional[str]
    imsi: Optional[str]
    group_name: Optional[str]
    value: Optional[float]
    feature: Optional[str]
    is_imputed: Optional[int]
    re_process_dt: Optional[str]
    def setMutation(row):
        return NNQESScoreModelLong32(
            rpt_dt=  Convertions.format_date(row.get('rpt_dt')),
            rpt_dt_ts=Convertions.format_timestamp_dt(row.get('rpt_dt')),
            run_key= row.get('run_key'),
            score_model_id= row.get('score_model_id'),
            acct_num= row.get('acct_num'),
            cust_id= row.get('cust_id'),
            cust_line_seq_id= row.get('cust_line_seq_id'),
            imsi= row.get('imsi'),
            group_name= row.get('group_name'),
            value= round(row.get('value'), 9) if row.get('value') is not None else None,
            feature= row.get('feature'),
            is_imputed= Convertions.safe_int(row.get('sector')),
            re_process_dt=  Convertions.format_date(row.get('re_process_dt')),
        )

class UG_Customer_Network_Usage_Anomalous_Sites(NamedTuple):
    vendor: Optional[str]
    market_5g: Optional[str]
    gnb_id: Optional[str]
    du: Optional[str]
    sector_5g: Optional[str]
    assessment: Optional[str]
    fwa_users_count: Optional[int]
    mobility_qci8_users_count: Optional[int]
    mobility_qci9_users_count: Optional[int]
    process_dt: Optional[str]
    created_timestamp: Optional[str]

    def setMutation(row):
        return UG_Customer_Network_Usage_Anomalous_Sites(
            vendor=row.get('vendor'),
            market_5g=row.get('market_5g'),
            gnb_id=row.get('gnb_id'),
            du=row.get('du'),
            sector_5g=row.get('sector_5g'),
            assessment=row.get('assessment'),
            fwa_users_count=row.get('fwa_users_count'),
            mobility_qci8_users_count=row.get('mobility_qci8_users_count'),
            mobility_qci9_users_count=row.get('mobility_qci9_users_count'),
            process_dt=Convertions.format_datetime(row.get('process_dt')),
            created_timestamp=Convertions.format_timestamp_dt(row.get('created_timestamp'))
            )

class UG_Customer_Network_Usage_Hourly(NamedTuple):
    mdate: Optional[str]
    mdate_ts: Optional[str]
    mhour: Optional[float]
    market_5g: Optional[str]
    gnb_id: Optional[str]
    sector_5g: Optional[str]
    carrier_5G: Optional[str]
    traffic_type: Optional[str]
    avg_nr_rsrp: Optional[float]
    avg_nr_rsrq: Optional[float]
    avg_lte_rsrp: Optional[float]
    avg_lte_rsrq: Optional[float]
    p10_dl_speed: Optional[float]
    p50_dl_speed: Optional[float]
    p90_dl_speed: Optional[float]
    p10_ul_speed: Optional[float]
    p50_ul_speed: Optional[float]
    p90_ul_speed: Optional[float]
    total_dl_volume: Optional[float]
    total_ul_volume: Optional[float]
    avg_nr_dl_volume_share: Optional[float]
    avg_nr_ul_volume_share: Optional[float]
    users_count: Optional[int]
    dl_connections_count: Optional[int]
    process_dt: Optional[str]
    created_timestamp: Optional[str]

    def setMutation(row):
        return UG_Customer_Network_Usage_Hourly(
            mdate=Convertions.format_date(row.get('mdate')),
            mdate_ts=Convertions.format_timestamp_dt(row.get('mdate')),
            mhour=row.get('mhour'),
            market_5g=row.get('market_5g'),
            gnb_id=row.get('gnb_id'),
            sector_5g=row.get('sector_5g'),
            carrier_5G=row.get('carrier_5G'),
            traffic_type=row.get('traffic_type'),
            avg_nr_rsrp=row.get('avg_nr_rsrp'),
            avg_nr_rsrq=row.get('avg_nr_rsrq'),
            avg_lte_rsrp=row.get('avg_lte_rsrp'),
            avg_lte_rsrq=row.get('avg_lte_rsrq'),
            p10_dl_speed=row.get('p10_dl_speed'),
            p50_dl_speed=row.get('p50_dl_speed'),
            p90_dl_speed=row.get('p90_dl_speed'),
            p10_ul_speed=row.get('p10_ul_speed'),
            p50_ul_speed=row.get('p50_ul_speed'),
            p90_ul_speed=row.get('p90_ul_speed'),
            total_dl_volume=row.get('total_dl_volume'),
            total_ul_volume=row.get('total_ul_volume'),
            avg_nr_dl_volume_share=row.get('avg_nr_dl_volume_share'),
            avg_nr_ul_volume_share=row.get('avg_nr_ul_volume_share'),
            users_count=row.get('users_count'),
            dl_connections_count=row.get('dl_connections_count'),
            process_dt=Convertions.format_datetime(row.get('process_dt')),
            created_timestamp=Convertions.format_timestamp_dt(row.get('created_timestamp'))
            )

class UG_Customer_Network_Usage_Daily(NamedTuple):
    vendor: Optional[str]
    traffic_type: Optional[str]
    mdate: Optional[str]
    mdate_ts: Optional[str]
    gnb_id: Optional[str]
    sector_5g: Optional[str]
    carrier_5G: Optional[str]
    du: Optional[str]
    total_dl_volume: Optional[float]
    p10_dl_speed: Optional[float]
    p50_dl_speed: Optional[float]
    p90_dl_speed: Optional[float]
    total_ul_volume: Optional[float]
    p10_ul_speed: Optional[float]
    p50_ul_speed: Optional[float]
    p90_ul_speed: Optional[float]
    avg_nr_dl_volume_share: Optional[float]
    avg_nr_ul_volume_share: Optional[float]
    avg_dl_latency: Optional[float]
    p90_dl_latency: Optional[float]
    dl_connections_count: Optional[int]
    ul_connections_count: Optional[int]
    user_count: Optional[int]
    process_dt: Optional[str]
    created_timestamp: Optional[str]

    def setMutation(row):
        return UG_Customer_Network_Usage_Daily(
            vendor=row.get('vendor'),
            traffic_type=row.get('traffic_type'),
            mdate=Convertions.format_date(row.get('mdate')),
            mdate_ts=Convertions.format_timestamp_dt(row.get('mdate')),
            gnb_id=row.get('gnb_id'),
            sector_5g=row.get('sector_5g'),
            carrier_5G=row.get('carrier_5G'),
            du=row.get('du'),
            total_dl_volume=row.get('total_dl_volume'),
            p10_dl_speed=row.get('p10_dl_speed'),
            p50_dl_speed=row.get('p50_dl_speed'),
            p90_dl_speed=row.get('p90_dl_speed'),
            total_ul_volume=row.get('total_ul_volume'),
            p10_ul_speed=row.get('p10_ul_speed'),
            p50_ul_speed=row.get('p50_ul_speed'),
            p90_ul_speed=row.get('p90_ul_speed'),
            avg_nr_dl_volume_share=row.get('avg_nr_dl_volume_share'),
            avg_nr_ul_volume_share=row.get('avg_nr_ul_volume_share'),
            avg_dl_latency=row.get('avg_dl_latency'),
            p90_dl_latency=row.get('p90_dl_latency'),
            dl_connections_count=row.get('dl_connections_count'),
            ul_connections_count=row.get('ul_connections_count'),
            user_count=row.get('user_count'),
            process_dt=Convertions.format_datetime(row.get('process_dt')),
            created_timestamp=Convertions.format_timestamp_dt(row.get('created_timestamp'))
            )

class UG_imsi_level_samsung_nokia_raw_v1(NamedTuple):
    mdate: Optional[str]
    mhour: Optional[str]
    imsi: Optional[str]
    device_model: Optional[str]
    isfwa: Optional[str]
    enodeb_id: Optional[str]
    sector_4g: Optional[str]
    carrier_4g: Optional[str]
    gnb_id: Optional[str]
    sector_5g: Optional[str]
    carrier_5g: Optional[str]
    du: Optional[str]
    qci: Optional[str]
    geobin: Optional[str]
    median_lte_distance_miles: Optional[str]
    median_nr_distance_miles: Optional[str]
    median_lte_rsrp: Optional[str]
    median_lte_rsrq: Optional[str]
    median_nr_rsrp: Optional[str]
    median_nr_rsrq: Optional[str]
    median_cqi: Optional[str]
    total_dl_volume: Optional[str]
    total_ul_volume: Optional[str]
    p10_dl_speed: Optional[str]
    p50_dl_speed: Optional[str]
    p90_dl_speed: Optional[str]
    median_nr_ul_harq_fail_rate_during_dl: Optional[str]
    median_lte_ul_harq_fail_rate_during_dl: Optional[str]
    nr_dl_volume_share: Optional[str]
    dl_connections_count: Optional[str]
    p10_ul_speed: Optional[str]
    p50_ul_speed: Optional[str]
    p90_ul_speed: Optional[str]
    median_nr_ul_harq_fail_rate_during_ul: Optional[str]
    median_lte_ul_harq_fail_rate_during_ul: Optional[str]
    nr_ul_volume_share: Optional[str]
    ul_connections_count: Optional[str]
    total_connections_count: Optional[str]
    session_type: Optional[str]
    s_nssai: Optional[str]
    trans_dt: Optional[str]
    mdate_mhour_ts:Optional[str]
    def setMutation(row):
            return UG_imsi_level_samsung_nokia_raw_v1(
                mdate=row.get('mdate'),
                mhour=row.get('mhour'),
                imsi=row.get('imsi'),
                device_model=row.get('device_model'),
                isfwa=row.get('isfwa'),
                enodeb_id=row.get('enodeb_id'),
                sector_4g=row.get('sector_4g'),
                carrier_4g=row.get('carrier_4g'),
                gnb_id=row.get('gnb_id'),
                sector_5g=row.get('sector_5g'),
                carrier_5g=row.get('carrier_5g'),
                du=row.get('du'),
                qci=row.get('qci'),
                geobin=row.get('geobin'),
                median_lte_distance_miles=row.get('median_lte_distance_miles'),
                median_nr_distance_miles=row.get('median_nr_distance_miles'),
                median_lte_rsrp=row.get('median_lte_rsrp'),
                median_lte_rsrq=row.get('mmedian_lte_rsrqdate'),
                median_nr_rsrp=row.get('median_nr_rsrp'),
                median_nr_rsrq=row.get('median_nr_rsrq'),
                median_cqi=row.get('median_cqi'),
                total_dl_volume=row.get('total_dl_volume'),
                total_ul_volume=row.get('total_ul_volume'),
                p10_dl_speed=row.get('p10_dl_speed'),
                p50_dl_speed=row.get('p50_dl_speed'),
                p90_dl_speed=row.get('p90_dl_speed'),
                median_nr_ul_harq_fail_rate_during_dl=row.get('mmedian_nr_ul_harq_fail_rate_during_dldate'),
                median_lte_ul_harq_fail_rate_during_dl=row.get('median_lte_ul_harq_fail_rate_during_dl'),
                nr_dl_volume_share=row.get('nr_dl_volume_share'),
                dl_connections_count=row.get('dl_connections_count'),
                p10_ul_speed=row.get('p10_ul_speed'),
                p50_ul_speed=row.get('p50_ul_speed'),
                p90_ul_speed=row.get('p90_ul_speed'),
                median_nr_ul_harq_fail_rate_during_ul=row.get('median_nr_ul_harq_fail_rate_during_ul'),
                median_lte_ul_harq_fail_rate_during_ul=row.get('median_lte_ul_harq_fail_rate_during_ul'),
                nr_ul_volume_share=row.get('nr_ul_volume_share'),
                ul_connections_count=row.get('ul_connections_count'),
                total_connections_count=row.get('total_connections_count'),
                session_type=row.get('session_type'),
                s_nssai=row.get('s_nssai'),
                trans_dt=Convertions.format_date(row.get('trans_dt')),
                mdate_mhour_ts= Convertions.add_hours_to_date(row.get('mdate'),row.get('mhour'))
            )

class UG_imsi_level_ericsson_raw_v1(NamedTuple):
    mhour: Optional[str]
    imsi: Optional[str]
    device_model: Optional[str]
    is_fwa: Optional[str]
    enodeb_id: Optional[str]
    sector_4g: Optional[str]
    carrier_4g: Optional[str]
    gnb_id: Optional[str]
    sector_5g: Optional[str]
    carrier_5g: Optional[str]
    du: Optional[str]
    session_type: Optional[str]
    qci: Optional[str]
    s_nssai: Optional[str]
    radio_conditions_4g: Optional[str]
    radio_conditions_5g: Optional[str]
    median_lte_distance_miles: Optional[str]
    median_nr_distance_miles: Optional[str]
    total_dl_volume: Optional[str]
    total_ul_volume: Optional[str]
    p10_dl_speed: Optional[str]
    p50_dl_speed: Optional[str]
    p90_dl_speed: Optional[str]
    median_nr_ul_harq_fail_rate_during_dl: Optional[str]
    median_lte_ul_harq_fail_rate_during_dl: Optional[str]
    nr_dl_volume_share: Optional[str]
    dl_connections_count: Optional[str]
    p10_ul_speed: Optional[str]
    p50_ul_speed: Optional[str]
    p90_ul_speed: Optional[str]
    median_nr_ul_harq_fail_rate_during_ul: Optional[str]
    median_lte_ul_harq_fail_rate_during_ul: Optional[str]
    nr_ul_volume_share: Optional[str]
    ul_connections_count: Optional[str]
    geobin: Optional[str]
    avg_dl_latency: Optional[float]
    P10_dl_latency: Optional[float]
    median_dl_latency: Optional[float]
    P90_dl_latency: Optional[float]
    median_lte_cqi: Optional[float]
    median_lte_rsrp: Optional[float]
    median_lte_rsrq: Optional[float]
    median_nr_rsrp: Optional[float]
    median_nr_rsrq: Optional[float]
    trans_dt: Optional[str]
    mhour_ts: Optional[str]
    def setMutation(row):
            return UG_imsi_level_ericsson_raw_v1(
                mhour=row.get('mhour'),
                imsi=row.get('imsi'),
                device_model=row.get('device_model'),
                is_fwa=row.get('is_fwa'),
                enodeb_id=row.get('enodeb_id'),
                sector_4g=row.get('sector_4g'),
                carrier_4g=row.get('carrier_4g'),
                gnb_id=row.get('gnb_id'),
                sector_5g=row.get('sector_5g'),
                carrier_5g=row.get('carrier_5g'),
                du=row.get('du'),
                session_type=row.get('session_type'),
                qci=row.get('qci'),
                s_nssai=row.get('s_nssai'),
                radio_conditions_4g=row.get('radio_conditions_4g'),
                radio_conditions_5g=row.get('radio_conditions_5g'),
                median_lte_distance_miles=row.get('median_lte_distance_miles'),
                median_nr_distance_miles=row.get('median_nr_distance_miles'),
                total_dl_volume=row.get('total_dl_volume'),
                total_ul_volume=row.get('total_ul_volume'),
                p10_dl_speed=row.get('p10_dl_speed'),
                p50_dl_speed=row.get('p50_dl_speed'),
                p90_dl_speed=row.get('p90_dl_speed'),
                median_nr_ul_harq_fail_rate_during_dl=row.get('median_nr_ul_harq_fail_rate_during_dl'),
                median_lte_ul_harq_fail_rate_during_dl=row.get('median_lte_ul_harq_fail_rate_during_dl'),
                nr_dl_volume_share=row.get('nr_dl_volume_share'),
                dl_connections_count=row.get('dl_connections_count'),
                p10_ul_speed=row.get('p10_ul_speed'),
                p50_ul_speed=row.get('p50_ul_speed'),
                p90_ul_speed=row.get('p90_ul_speed'),
                median_nr_ul_harq_fail_rate_during_ul=row.get('median_nr_ul_harq_fail_rate_during_ul'),
                median_lte_ul_harq_fail_rate_during_ul=row.get('mdamedian_lte_ul_harq_fail_rate_during_ulte'),    
                nr_ul_volume_share=row.get('nr_ul_volume_share'),
                ul_connections_count=row.get('ul_connections_count'),
                geobin=row.get('geobin'),
                avg_dl_latency=row.get('avg_dl_latency'),
                P10_dl_latency=row.get('P10_dl_latency'),
                median_dl_latency=row.get('median_dl_latency'),
                P90_dl_latency=row.get('P90_dl_latency'),
                median_lte_cqi=row.get('median_lte_cqi'),
                median_lte_rsrp=row.get('median_lte_rsrp'),
                median_lte_rsrq=row.get('median_lte_rsrq'),
                median_nr_rsrp=row.get('median_nr_rsrp'),
                median_nr_rsrq=row.get('median_nr_rsrq'),
                trans_dt =Convertions.format_date(row.get('trans_dt')),
                mhour_ts = Convertions.date_hour_to_timestamp(row.get('mhour')),                                        
            )

class Convertions:
    def format_date(value):
        """Convert date/datetime to string format."""
        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return value
    
    def format_timestamp_dt(value):
        if value is None:
            return None
        try:
            if isinstance(value, datetime):
                return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            if isinstance(value, date):
                return f"{value.strftime('%Y-%m-%d')}T00:00:00.000000Z"
        except Exception as e:        
            return None
    
    def add_hours_to_date(date_value, hours):
        str_hr = hours if int(hours)>9 else f'0{int(hours)}'
        if isinstance(date_value, date):
                return f"{date_value.strftime('%Y-%m-%d')}T{str_hr}:00:00.000000Z"
        # if date_value is None:
        #     return None
        # try:
        #     if isinstance(date_value, date):
        #         return f"{date_value.strftime('%Y-%m-%d')}T{str_hr}:00:00.000000Z"
        # except Exception as e:        
        #     return None
        # return None
    
    def date_hour_to_timestamp(date_value):
        # try:
        str_date_value = str(date_value)
        formated_date_hr = datetime.strptime(str_date_value, "%Y-%m-%d%H")
        return f"{formated_date_hr.strftime('%Y-%m-%dT%H')}:00:00.000000Z"
        # except Exception as e:
        #     return None
    
    def format_timestamp(value):
        """Convert date/datetime to string format."""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                # Remove microseconds if present
                dt = datetime.strptime(value.split('.')[0], '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return value
        except Exception as e:
            return None
    def format_datetime(value):
        """Convert datetime to string format."""
        new_val_dt=value
        if "T" in value:
            # Remove Date and Time separator literal T if present to sync with python datatime 
            new_val=value.replace("T"," ")
            new_val_dt=datetime.strptime(new_val,"%Y-%m-%d %H:%M:%S")
        if value is None:
            return None
        try:
            if isinstance(new_val_dt, datetime):
                return new_val_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            return None
    def safe_str(value):
        """Safely convert value to string."""
        if value is None:
            return None
        try:
            return str(value)
        except Exception:
            return None
    def safe_int(value):
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    def safe_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
