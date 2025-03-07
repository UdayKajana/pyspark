Stage name
F44
Status
 Running
Progress
5%
Start time
March 7, 2025 at 5:16:56 PM GMT+5
End time
—
Component steps
ParDo(SDFBoundedSourceDoFn), SDFBoundedSourceReader, Read, ReadFromBigQuery, Workflow, ParDo(PassThrough), ParDo(PassThrough), _PassThroughThenCleanup, PrepareData, ParMultiDo(Anonymous), Map, MapElements, WriteToSpanner, ParMultiDo(ToMutationGroup), To mutation group, SpannerIO.Write, Window.Assign, RewindowIntoGlobal, Write mutations to Cloud Spanner, Filter Unbatchable Mutations, Write batches to Spanner, ParMultiDo(GatherSortCreateBatches), and Gather Sort And Create Batches
