package net.opentsdb;

/**
 * Yet Another redesign for 3.x of the Query pipeline.
 * 
 * The requirements are as follows:
 * - Composable functions (group by, downsample, multi-pass topN, etc)
 * - Streaming support as data for a query may not fit entirely in memory. Therefore
 *   we want to stream ala Splunk, e.g. fetch, process and return now to 1h ago,
 *   then fetch, process and return 1h to 2h ago, etc.
 * - Balance quick-as-possible query time with minimizing memory bloat as we need to 
 *   handle many simultaneous queries. (** This is tough! **)
 * - Provide an easy to consume Java API for querying and working with the results.
 *   This will, naturally, be used by the RPC APIs.
 * - Asynchronous pipeline (with optional user facing sync ops) so we can use
 *   threads and host resources efficiently.
 * - Multi-type result pipelines for dealing with regular old numeric data, histograms,
 *   annotations, status', etc.
 * - Cachable multi-pass processors. We don't want to go all the way back to storage
 *   if we can help it. And we want to maintain ONLY the raw data in memory if we 
 *   can at all help it.
 *
 *  NOTE: This is all scratch code and needs a TON of cleanup, error handling, etc.
 *  
 *  Q & A:
 *  
 *  Q) Why not use JDK 8 Streams?
 *  A) Turns out there is a MAJOR performance hit using streams. See the other 
 *     JMH benchs here. It's not worth it until they can optimize the code.
 *  
 *  Q) Why iterators and not a simple list or array of data points?
 *  A) TSDB used iterators in the past because it allows us to process each value
 *     through a pipeline without keeping duplicate lists in mem. E.g. if you have
 *     100 time series with 144 points each, and a pipeline with 5 operations, you'd
 *     potentially need space for 72,000 dps vs 14,4000 + 500 (a buffer per op per timeseris). 
 *  
 */
public class Pipeline {
  public static void main(final String[] args) {
    //net.opentsdb.pipeline.PipelineBenchmark.iteratorsWithComplexTypes(null);
    //net.opentsdb.pipeline.PipelineBenchmark.version1Sizes();
    //net.opentsdb.pipeline2.PipelineBenchmark.listsOfComplexTypes(null);
    //net.opentsdb.pipeline3.PipelineBenchmark.arraysOfPrimitives(null);
    //net.opentsdb.pipeline4.PipelineBenchmark.iteratorsVersion2WithComplexTypes(null);
    //net.opentsdb.pipeline5.PipelineBenchmark.iteratorsVersion3WithCachedIDs(null);
    //net.opentsdb.pipeline6.PipelineBenchmark.iteratorsWithXXHashIds(null);
    //net.opentsdb.pipeline7.PipelineBenchmark.iteratorsWithStringIds(null);
    //net.opentsdb.pipeline8.PipelineBenchmark.iteratorsWithDownsampledData(null);
    net.opentsdb.pipeline9.PipelineBenchmark.iteratorsWithLazyInstantiationAndDownsampling(null);
  }
}
