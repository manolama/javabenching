package net.opentsdb.pipeline9;

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

import net.opentsdb.pipeline9.TimeSortedDataStore;
import net.opentsdb.pipeline9.Functions.*;
import net.opentsdb.pipeline9.Interfaces.*;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class PipelineBenchmark {

  /**
   * In this case the API gives a QueryExecutionPipeline that the user would
   * provide an asynchronous listener. To initiate the stream, the caller just
   * calls {@link QExecutionPipeline#fetchNext()}.
   */
  @Benchmark
  public static void iteratorsWithLazyInstantiationAndDownsampling(Blackhole black_hole) {
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
          for (TSByteId tsid : next.series()) {
            if (black_hole == null) {
              System.out.println(tsid);
            } else {
              black_hole.consume(tsid.toString());
            }
            
            for (TS<?> ts : next.getSeries(tsid)) {
              Iterator<?> it = ts.iterator();
              if (ts.type() == NType.TYPE) {
                while (it.hasNext()) {
                  TSValue<NType> v = (TSValue<NType>) it.next();
                  if (black_hole == null) {
                    System.out.println("  " + v.timestamp().epoch() + " " + v.value().toDouble());
                  } else {
                    black_hole.consume(v.timestamp().epoch() + " " + v.value().toDouble());
                  }
                }
              } else {
                while (it.hasNext()) {
                  TSValue<StringType> v = (TSValue<StringType>) it.next();
                  if (black_hole == null) {
                    System.out.println("  " + v.timestamp().epoch() + " " + v.value().values());
                  } else {
                    black_hole.consume(v.timestamp().epoch() + " " + v.value().values());
                  }
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
    
    store.pool.shutdownNow();
  }
}
