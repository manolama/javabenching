package net.opentsdb.pipeline;

import java.util.Iterator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.StringType;
import net.opentsdb.pipeline.Functions.*;
import net.opentsdb.pipeline.Implementations.*;
import net.opentsdb.pipeline.Interfaces.*;

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
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class Main {
  
  public static void main(final String[] args) {
    //version1(null);
    //version1Sizes();
    //net.opentsdb.pipeline2.Main.version2(null);
    net.opentsdb.pipeline3.Main.arraysOfPrimitives(null);
  }
  
  /**
   * In this case the API gives a QueryExecutionPipeline that the user would
   * provide an asynchronous listener. To initiate the stream, the caller just
   * calls {@link QExecutionPipeline#fetchNext()}.
   */
  @Benchmark
  public static void iteratorsWithComplexTypes(Blackhole black_hole) {
    QueryMode mode = QueryMode.CLIENT_STREAM;
    
    /** This section would be hidden behind the query engine. Users just 
     * submit the query and the call graph is setup, yada yada. */
    TimeSortedDataStore store = new TimeSortedDataStore(true);
    QExecutionPipeline exec = store.new MyExecution(true, mode);
    exec = (QExecutionPipeline) new FilterNumsByString(exec);
    exec = (QExecutionPipeline) new GroupBy(exec);
    exec = (QExecutionPipeline) new DiffFromStdD(exec);
    exec = (QExecutionPipeline) new ExpressionProc(exec);
    /** END QUERY ENGINE BIT */
    
    /**
     * Here's where the API consumer does their work.
     */
    class MyListener implements StreamListener {
      QExecutionPipeline exec;
      int iterations = 0;
      Deferred<Object> d = new Deferred<Object>();
      
      public MyListener(QExecutionPipeline exec) {
        this.exec = exec;
        exec.setListener(this);
      }
      
      /**
       * This is called by the execution pipeline when the final call to 
       * fetchNext() will not return data.
       */
      @Override
      public void onComplete() {
        d.callback(null);
      }

      /**
       * This is called each time a successful (possibly empty) result is returned
       * from the execution. It will either contain:
       * - The entire time range of data for one or more time series 
       * - or all of the time series for a slice of the query range. In this case
       *   subsequent onNext() results may contain different time series iterators. 
       */
      @Override
      public void onNext(QResult next) {
        try {
          // consumers can iterate over each series and then iterate over the dps 
          // within that series.
          for (TS<?> ts : next.series()) {
            if (black_hole == null) {
              System.out.println(ts.id());
            } else {
              black_hole.consume(ts.id().toString());
            }
            Iterator<?> it = ts.iterator();
            if (ts.type() == NumericType.TYPE) {
              while (it.hasNext()) {
                TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
                if (black_hole == null) {
                  System.out.println("  " + v.timestamp().epoch() + " " + v.value().toDouble());
                } else {
                  black_hole.consume(v.timestamp().epoch() + " " + v.value().toDouble());
                }
              }
            } else {
              while (it.hasNext()) {
                TimeSeriesValue<StringType> v = (TimeSeriesValue<StringType>) it.next();
                if (black_hole == null) {
                  System.out.println("  " + v.timestamp().epoch() + " " + v.value().values());
                } else {
                  black_hole.consume(v.timestamp().epoch() + " " + v.value().values());
                }
              }
            }
          }
          if (black_hole == null) {
            System.out.println("-------------------------");
          }
          
          iterations++;
          if (mode == QueryMode.CLIENT_STREAM) {
            exec.fetchNext();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      @Override
      public void onError(Throwable t) {
        d.callback(t);
      }
      
    }
    
    // instantiate a new listener to asyncronously receive data
    MyListener listener = new MyListener(exec);
    
    // start the iteration.
    exec.fetchNext();
    
    try {
      listener.d.join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    //System.out.println(ClassLayout.parseInstance(exec).toPrintable());
    
    store.pool.shutdownNow();
  }

  public static void version1Sizes() {
    System.out.println(VM.current().details());
    
    System.out.println(ClassLayout.parseClass(QExecutionPipeline.class).toPrintable());
    System.out.println(ClassLayout.parseClass(TS.class).toPrintable());
    
    System.out.println(ClassLayout.parseClass(FilterNumsByString.class).toPrintable());
    System.out.println(ClassLayout.parseClass(GroupBy.class).toPrintable());
    System.out.println(ClassLayout.parseClass(DiffFromStdD.class).toPrintable());
    
    System.out.println(ClassLayout.parseClass(ArrayBackedLongTS.class).toPrintable());
    System.out.println(ClassLayout.parseClass(MutableStringType.class).toPrintable());
  }
  
}
