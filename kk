Error message from worker: generic::unknown: org.apache.beam.sdk.coders.CoderException: `UnknownCoderWrapper` was used to perform an actual decoding in the Java SDK. Potentially a Java transform is being followed by a cross-language transform that uses a coder that is not available in the Java SDK. Please make sure that Python transforms at the multi-language boundary use Beam portable coders.
	org.apache.beam.sdk.util.construction.UnknownCoderWrapper.decode(UnknownCoderWrapper.java:55)
	org.apache.beam.sdk.coders.Coder.decode(Coder.java:159)
	org.apache.beam.sdk.coders.LengthPrefixCoder.decode(LengthPrefixCoder.java:64)
	org.apache.beam.sdk.coders.Coder.decode(Coder.java:159)
	org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.decode(WindowedValue.java:621)
	org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.decode(WindowedValue.java:612)
	org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.decode(WindowedValue.java:558)
	org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver.multiplexElements(BeamFnDataInboundObserver.java:172)
	org.apache.beam.fn.harness.control.ProcessBundleHandler.processBundle(ProcessBundleHandler.java:537)
	org.apache.beam.fn.harness.control.BeamFnControlClient.delegateOnInstructionRequestType(BeamFnControlClient.java:150)
	org.apache.beam.fn.harness.control.BeamFnControlClient$InboundObserver.lambda$onNext$0(BeamFnControlClient.java:115)
	java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	org.apache.beam.sdk.util.UnboundedScheduledExecutorService$ScheduledFutureTask.run(UnboundedScheduledExecutorService.java:163)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	java.base/java.lang.Thread.run(Thread.java:829)

passed through:
==>
    dist_proc/dax/workflow/worker/fnapi_service_impl.cc:1316
