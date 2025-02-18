import json
import apache_beam as beam
from apache_beam.transforms import CombineFn
from typing import List, Iterable

# NOTE: We only did changes in the PrepareSpannerData class
class PrepareSpannerData(beam.DoFn):
    def process(self, element) -> Iterable[dict]:
        try:
            yield element
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
            return

# NOTE: Create CombineFn to construct the combined insert statement
class BatchSQL(CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, element):
        accumulator.append(element)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = []
        for accumulator in accumulators:
            merged.extend(accumulator)
        return merged

    def extract_output(self, accumulator):
        # Sanitize your SQL
        def safe_sql_string(s):
            if s is None:
                return "NULL"
            return "'" + str(s).replace("'", "''") + "'"

        sql_statement_list = []
        for record in accumulator:

            telephone_number = record.get('telephone_number')
            trouble_report_num = record.get('trouble_report_num')
            address_id = record.get('address_id')
            chronic_flag = record.get('chronic_flag')
            chronic_total = record.get('chronic_total')
            line_id_trimmed = record.get('line_id_trimmed')
            port_associated_service = record.get('port_associated_service')
            date_opened = record.get('date_opened')
            data_circuit_id = record.get('data_circuit_id')
            video_circuit_id = record.get('video_circuit_id')

            # Sanitize your SQL
            def safe_sql_string(s):
                if s is None:
                    return "NULL"
                return "'" + str(s).replace("'", "''") + "'"

            sql_statement = f"""
            INSERT INTO your_spanner_table (telephone_number, trouble_report_num, address_id, chronic_flag,
                                            chronic_total, line_id_trimmed, port_associated_service, date_opened,
                                            data_circuit_id, video_circuit_id)
            VALUES ({safe_sql_string(telephone_number)}, {safe_sql_string(trouble_report_num)}, {safe_sql_string(address_id)},
                    {safe_sql_string(chronic_flag)}, {safe_sql_string(chronic_total)}, {safe_sql_string(line_id_trimmed)},
                    {safe_sql_string(port_associated_service)}, {safe_sql_string(date_opened)}, {safe_sql_string(data_circuit_id)},
                    {safe_sql_string(video_circuit_id)});
            """
            sql_statement_list.append(sql_statement)
        return "\n".join(sql_statement_list).encode('utf-8') # Join and encode as bytes


def run(known_args, beam_args):
    options = { ... }  # Your pipeline options

    with beam.Pipeline(options=options) as p:
        data = (p
            | "Read From Pubsub" >> beam.io.ReadFromPubSub(subscription=known_args.pubsub_subscription_name)
            | "Decode Avro" >> beam.ParDo(DecodeAvroRecords())
            | "Windowing" >> beam.Window.into(beam.window.FixedWindows(2 * 60))
            | 'Prepare Spanner Data' >> beam.ParDo(PrepareSpannerData())
            | "Combine To SQL" >> beam.CombineGlobally(BatchSQL()).as_singleton_view()
        )
        _ = (data
            | "Write To Spanner" >> beam.io.gcp.spanner.SpannerInsertOrUpdate(
                instance_id=known_args.spanner_instance,
                database_id=known_args.spanner_dataset,
                project_id=known_args.spanner_project,
                table=known_args.spanner_table,
                expansion_service=known_args.expansion_service,  # Keep this
                max_number_mutations=7000,
                high_priority=True
            )
        )
