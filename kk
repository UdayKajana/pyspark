import argparse
import logging
from typing import NamedTuple, Iterator, Optional
import apache_beam as beam
import apache_beam.io.gcp.spanner as sp
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from decimal import Decimal
from datetime import date, datetime

class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--query', type=str)

class CDIKeysMutation(NamedTuple):
    account_sk: Optional[Decimal]
    mon: Optional[str]
    cust_sk: Optional[Decimal]
    svc_party_sk: Optional[Decimal]
    acct_estbd_dt: Optional[str]
    acct_term_dt: Optional[str]
    tenure_term_days: Optional[int]

beam.coders.registry.register_coder(CDIKeysMutation, beam.coders.RowCoder)

class PrepareData(beam.DoFn):
    def process(self, element) -> Iterator[CDIKeysMutation]:
        try:
            yield self.transform_to_mutation(element)
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
            raise

    def format_date(self, value):
        """Convert date/datetime to string format."""
        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return value

    def transform_to_mutation(self, row):
        # Format dates while keeping other fields as is
        return CDIKeysMutation(
            account_sk=row.get('acct_sk'),
            mon=row.get('mon'),
            cust_sk=row.get('cust_sk'),
            svc_party_sk=row.get('svc_party_sk'),
            acct_estbd_dt=self.format_date(row.get('acct_estbd_dt')),
            acct_term_dt=self.format_date(row.get('acct_term_dt')),
            tenure_term_days=row.get('tenure_term_days')
        )

def run(args, beam_args):
    options = {
        'project': args.project,
        'runner': args.runner,
        'region': args.region,
        'staging_location': args.staging_location,
        'temp_location': args.temp_location,
        'template_location': args.template_location,
        'save_main_session': True,
        'streaming': False
    }

    pipeline_options = PipelineOptions.from_dictionary(options)
    template_options = pipeline_options.view_as(TemplateOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from BigQuery
        data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
            query=template_options.query,
            use_standard_sql=True
        ))
        processed_data = (data | 'PrepareData' >> 
            beam.ParDo(PrepareData())
            .with_output_types(CDIKeysMutation))

        # Write to Spanner
        _ = (processed_data | "WriteToSpanner" >> 
            sp.SpannerInsertOrUpdate(
                project_id=args.spanner_project,
                instance_id=args.spanner_instance,
                database_id=args.spanner_dataset,
                table=args.spanner_table,
                expansion_service=args.expansion_service
            )
        )

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Project ID')
    parser.add_argument('--runner', default="DataflowRunner", help='Pipeline runner')
    parser.add_argument('--region', default="us-east4", help='GCP project region')
    parser.add_argument('--staging_location', required=True, help='Staging GCS bucket path')
    parser.add_argument('--temp_location', required=True, help='Temp GCS bucket path')
    parser.add_argument('--template_location', required=True, help='GCS bucket path to save template')
    parser.add_argument('--spanner_dataset', required=True, help='spanner dataset')
    parser.add_argument('--spanner_instance', required=True, help='spanner instance')
    parser.add_argument('--spanner_project', required=True, help='spanner project')
    parser.add_argument('--spanner_table', required=True, help='spanner table')
    parser.add_argument('--expansion_service', default='localhost:50768', help='Expansion service host:port')
    return parser.parse_known_args()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, beam_args = parse_arguments()
    run(known_args, beam_args)
    print(f'Created template at {known_args.template_location}.') make this code to accept null values for columns svc_party_sk it is failing with the error: Error message from worker: org.apache.beam.sdk.util.UserCodeException: java.lang.NullPointerException: Null decimal at column svc_party_sk
	org.apache.beam.sdk.util.UserCodeException.wrap(UserCodeException.java:39)
	org.apache.beam.sdk.transforms.MapElements$2$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForParDo(FnApiDoFnRunner.java:810)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.BeamFnDataReadRunner.forwardElementToConsumer(BeamFnDataReadRunner.java:213)
	org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver.multiplexElements(BeamFnDataInboundObserver.java:172)
	org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver.awaitCompletion(BeamFnDataInboundObserver.java:136)
	org.apache.beam.fn.harness.control.ProcessBundleHandler.processBundle(ProcessBundleHandler.java:550)
	org.apache.beam.fn.harness.control.BeamFnControlClient.delegateOnInstructionRequestType(BeamFnControlClient.java:150)
	org.apache.beam.fn.harness.control.BeamFnControlClient$InboundObserver.lambda$onNext$0(BeamFnControlClient.java:115)
	java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	org.apache.beam.sdk.util.UnboundedScheduledExecutorService$ScheduledFutureTask.run(UnboundedScheduledExecutorService.java:163)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.NullPointerException: Null decimal at column svc_party_sk
	org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull(Preconditions.java:921)
	org.apache.beam.sdk.io.gcp.spanner.MutationUtils.setBeamValueToMutation(MutationUtils.java:222)
	org.apache.beam.sdk.io.gcp.spanner.MutationUtils.lambda$createMutationFromBeamRows$1(MutationUtils.java:100)
	java.base/java.util.ArrayList.forEach(ArrayList.java:1541)
	org.apache.beam.sdk.io.gcp.spanner.MutationUtils.createMutationFromBeamRows(MutationUtils.java:98)
	org.apache.beam.sdk.io.gcp.spanner.MutationUtils.lambda$beamRowToMutationFn$ae3ac727$1(MutationUtils.java:75)
	org.apache.beam.sdk.transforms.MapElements$2.processElement(MapElements.java:151)
