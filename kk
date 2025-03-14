Error message from worker: org.apache.beam.sdk.util.UserCodeException: com.google.cloud.spanner.SpannerException: DEADLINE_EXCEEDED: com.google.api.gax.rpc.DeadlineExceededException: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: Deadline Exceeded
	org.apache.beam.sdk.util.UserCodeException.wrap(UserCodeException.java:39)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForParDo(FnApiDoFnRunner.java:810)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$WindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2240)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.outputBatch(SpannerIO.java:2031)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.sortAndOutputBatches(SpannerIO.java:2007)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.processElement(SpannerIO.java:2051)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForWindowObservingParDo(FnApiDoFnRunner.java:823)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$WindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2240)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$BatchableMutationFilterFn.processElement(SpannerIO.java:2167)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$BatchableMutationFilterFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForWindowObservingParDo(FnApiDoFnRunner.java:823)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.MapFnRunners$ExplodedWindowedValueMapperFactory.lambda$create$0(MapFnRunners.java:133)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$NonWindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2695)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$ToMutationGroupFn.processElement(SpannerIO.java:1908)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$ToMutationGroupFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForParDo(FnApiDoFnRunner.java:810)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$NonWindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2695)
	org.apache.beam.sdk.transforms.MapElements$2.processElement(MapElements.java:151)
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
Caused by: com.google.cloud.spanner.SpannerException: DEADLINE_EXCEEDED: com.google.api.gax.rpc.DeadlineExceededException: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: Deadline Exceeded
	com.google.cloud.spanner.SpannerExceptionFactory.newSpannerExceptionPreformatted(SpannerExceptionFactory.java:302)
	com.google.cloud.spanner.SpannerExceptionFactory.newSpannerExceptionPreformatted(SpannerExceptionFactory.java:308)
	com.google.cloud.spanner.SpannerExceptionFactory.newSpannerException(SpannerExceptionFactory.java:61)
	com.google.cloud.spanner.SpannerExceptionFactory.newSpannerException(SpannerExceptionFactory.java:181)
	com.google.cloud.spanner.spi.v1.GapicSpannerRpc.get(GapicSpannerRpc.java:1942)
	com.google.cloud.spanner.spi.v1.GapicSpannerRpc.commit(GapicSpannerRpc.java:1815)
	com.google.cloud.spanner.SessionImpl.lambda$writeAtLeastOnceWithOptions$2(SessionImpl.java:268)
	com.google.api.gax.retrying.DirectRetryingExecutor.submit(DirectRetryingExecutor.java:102)
	com.google.cloud.RetryHelper.run(RetryHelper.java:76)
	com.google.cloud.RetryHelper.runWithRetries(RetryHelper.java:50)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:94)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:73)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:69)
	com.google.cloud.spanner.SessionImpl.writeAtLeastOnceWithOptions(SessionImpl.java:267)
	com.google.cloud.spanner.SessionPool$PooledSession.writeAtLeastOnceWithOptions(SessionPool.java:1683)
	com.google.cloud.spanner.SessionPool$PooledSessionFuture.writeAtLeastOnceWithOptions(SessionPool.java:1358)
	com.google.cloud.spanner.DatabaseClientImpl.lambda$writeAtLeastOnceWithOptions$1(DatabaseClientImpl.java:118)
	com.google.cloud.spanner.DatabaseClientImpl.runWithSessionRetry(DatabaseClientImpl.java:278)
	com.google.cloud.spanner.DatabaseClientImpl.writeAtLeastOnceWithOptions(DatabaseClientImpl.java:117)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.spannerWriteWithRetryIfSchemaChange(SpannerIO.java:2314)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.writeMutations(SpannerIO.java:2395)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.processElement(SpannerIO.java:2273)
Caused by: java.util.concurrent.ExecutionException: com.google.api.gax.rpc.DeadlineExceededException: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: Deadline Exceeded
	com.google.common.util.concurrent.AbstractFuture.getDoneValue(AbstractFuture.java:594)
	com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:573)
	com.google.cloud.spanner.spi.v1.GapicSpannerRpc.get(GapicSpannerRpc.java:1936)
	com.google.cloud.spanner.spi.v1.GapicSpannerRpc.commit(GapicSpannerRpc.java:1815)
	com.google.cloud.spanner.SessionImpl.lambda$writeAtLeastOnceWithOptions$2(SessionImpl.java:268)
	com.google.api.gax.retrying.DirectRetryingExecutor.submit(DirectRetryingExecutor.java:102)
	com.google.cloud.RetryHelper.run(RetryHelper.java:76)
	com.google.cloud.RetryHelper.runWithRetries(RetryHelper.java:50)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:94)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:73)
	com.google.cloud.spanner.SpannerRetryHelper.runTxWithRetriesOnAborted(SpannerRetryHelper.java:69)
	com.google.cloud.spanner.SessionImpl.writeAtLeastOnceWithOptions(SessionImpl.java:267)
	com.google.cloud.spanner.SessionPool$PooledSession.writeAtLeastOnceWithOptions(SessionPool.java:1683)
	com.google.cloud.spanner.SessionPool$PooledSessionFuture.writeAtLeastOnceWithOptions(SessionPool.java:1358)
	com.google.cloud.spanner.DatabaseClientImpl.lambda$writeAtLeastOnceWithOptions$1(DatabaseClientImpl.java:118)
	com.google.cloud.spanner.DatabaseClientImpl.runWithSessionRetry(DatabaseClientImpl.java:278)
	com.google.cloud.spanner.DatabaseClientImpl.writeAtLeastOnceWithOptions(DatabaseClientImpl.java:117)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.spannerWriteWithRetryIfSchemaChange(SpannerIO.java:2314)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.writeMutations(SpannerIO.java:2395)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn.processElement(SpannerIO.java:2273)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$WriteToSpannerFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForParDo(FnApiDoFnRunner.java:810)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$WindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2240)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.outputBatch(SpannerIO.java:2031)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.sortAndOutputBatches(SpannerIO.java:2007)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn.processElement(SpannerIO.java:2051)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$GatherSortCreateBatchesFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForWindowObservingParDo(FnApiDoFnRunner.java:823)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$WindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2240)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$BatchableMutationFilterFn.processElement(SpannerIO.java:2167)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$BatchableMutationFilterFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForWindowObservingParDo(FnApiDoFnRunner.java:823)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.MapFnRunners$ExplodedWindowedValueMapperFactory.lambda$create$0(MapFnRunners.java:133)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$NonWindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2695)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$ToMutationGroupFn.processElement(SpannerIO.java:1908)
	org.apache.beam.sdk.io.gcp.spanner.SpannerIO$ToMutationGroupFn$DoFnInvoker.invokeProcessElement(Unknown Source)
	org.apache.beam.fn.harness.FnApiDoFnRunner.processElementForParDo(FnApiDoFnRunner.java:810)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:348)
	org.apache.beam.fn.harness.data.PCollectionConsumerRegistry$MetricTrackingFnDataReceiver.accept(PCollectionConsumerRegistry.java:275)
	org.apache.beam.fn.harness.FnApiDoFnRunner.outputTo(FnApiDoFnRunner.java:1837)
	org.apache.beam.fn.harness.FnApiDoFnRunner.access$3100(FnApiDoFnRunner.java:145)
	org.apache.beam.fn.harness.FnApiDoFnRunner$NonWindowObservingProcessBundleContext.output(FnApiDoFnRunner.java:2695)
	org.apache.beam.sdk.transforms.MapElements$2.processElement(MapElements.java:151)
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
Caused by: com.google.api.gax.rpc.DeadlineExceededException: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: Deadline Exceeded
	com.google.api.gax.rpc.ApiExceptionFactory.createException(ApiExceptionFactory.java:94)
	com.google.api.gax.rpc.ApiExceptionFactory.createException(ApiExceptionFactory.java:41)
	com.google.api.gax.grpc.GrpcApiExceptionFactory.create(GrpcApiExceptionFactory.java:86)
	com.google.api.gax.grpc.GrpcApiExceptionFactory.create(GrpcApiExceptionFactory.java:66)
	com.google.api.gax.grpc.GrpcExceptionCallable$ExceptionTransformingFuture.onFailure(GrpcExceptionCallable.java:97)
	com.google.api.core.ApiFutures$1.onFailure(ApiFutures.java:84)
	com.google.common.util.concurrent.Futures$CallbackListener.run(Futures.java:1130)
	com.google.common.util.concurrent.DirectExecutor.execute(DirectExecutor.java:31)
	com.google.common.util.concurrent.AbstractFuture.executeListener(AbstractFuture.java:1298)
	com.google.common.util.concurrent.AbstractFuture.complete(AbstractFuture.java:1059)
	com.google.common.util.concurrent.AbstractFuture.setException(AbstractFuture.java:809)
	io.grpc.stub.ClientCalls$GrpcFuture.setException(ClientCalls.java:568)
	io.grpc.stub.ClientCalls$UnaryStreamToFuture.onClose(ClientCalls.java:538)
	io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	com.google.api.gax.grpc.ChannelPool$ReleasingClientCall$1.onClose(ChannelPool.java:569)
	io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	com.google.cloud.spanner.spi.v1.SpannerErrorInterceptor$1$1.onClose(SpannerErrorInterceptor.java:100)
	io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	io.grpc.census.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:814)
	io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	io.grpc.census.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:494)
	io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:564)
	io.grpc.internal.ClientCallImpl.access$100(ClientCallImpl.java:72)
	io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInternal(ClientCallImpl.java:729)
	io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:710)
	io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)
	... 3 more
Caused by: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: Deadline Exceeded
	io.grpc.Status.asRuntimeException(Status.java:533)
	... 29 more




import argparse
from mutations.Mutations import *
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
    parser.add_argument('--process', required=True, help='process')
    parser.add_argument('--expansion_service', default='localhost:50768', help='Expansion service host:port')
    parser.add_argument('--setup_file', dest='setup_file', default='./setup.py', required=False, help='setup_file')
    return parser.parse_known_args()

logging.getLogger().setLevel(logging.ERROR)
known_args, beam_args = parse_arguments()

mutation = getMutation(known_args.process)
beam.coders.registry.register_coder(mutation, beam.coders.RowCoder)

class PrepareData(beam.DoFn):
    def process(self, element) -> Iterator[NQESSiteScores]:
        try:
            yield self.transform_to_mutation(element)
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
            raise
    def transform_to_mutation(self, row):
        return mutation.setMutation(row)

options = {
    'project': known_args.project,
    'runner': known_args.runner,
    'region': known_args.region,
    'staging_location': known_args.staging_location,
    'temp_location': known_args.temp_location,
    'template_location': known_args.template_location,
    'setup_file': known_args.setup_file,
    'save_main_session': True,
    'streaming': False
}

pipeline_options = PipelineOptions.from_dictionary(options)
template_options = pipeline_options.view_as(TemplateOptions)
with beam.Pipeline(options=pipeline_options) as p:
    # Read from BigQuery
    data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
        query=template_options.query,
        use_standard_sql=True,
        bigquery_job_labels={'parallelism': '50'}
    ))
    data1 = (data | "FusionBreak" >> beam.Reshuffle())
    processed_data = (data1 | 'PrepareData' >> 
        beam.ParDo(PrepareData())
        .with_output_types(mutation))
    # Write to Spanner
    _ = (processed_data | "WriteToSpanner" >> 
        sp.SpannerInsertOrUpdate(
            project_id=known_args.spanner_project,
            instance_id=known_args.spanner_instance,
            database_id=known_args.spanner_dataset,
            table=known_args.spanner_table,
            expansion_service=known_args.expansion_service,
            max_number_mutations=7000
        )
    )



python3 -m pipeline \
	--project vz-it-np-gudv-dev-dtwndo-0 \
	--runner DataflowRunner \
	--region us-east4 \
	--sdk_container_image us-east4-docker.pkg.dev/vz-it-np-gudv-dev-dtwndo-0/fwa-sssot/py39beam26:1.0.0 \
	--template_location gs://gudv-dev-dtwndo-0-usmr-internal/templates/ntwk_nqes_score_model_long \
	--temp_location gs://gudv-dev-dtwndo-0-usmr-internal/temp/ntwk_nqes_score_model_long \
	--staging_location gs://gudv-dev-dtwndo-0-usmr-internal/stg/ntwk_nqes_score_model_long \
	--spanner_project 'vz-it-np-jpuv-dev-anpdo-0' \
	--spanner_dataset 'network_wls_model' \
	--spanner_instance 'anp-spanner-instance' \
	--spanner_table 'ntwk_nqes_score_model_long' \
	--process 'ntwk_nqes_score_model_long' \
	--expansion_service 'localhost:50768'
