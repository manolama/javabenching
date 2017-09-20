package net.opentsdb.pipeline3;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline3.Functions.*;
import net.opentsdb.pipeline3.Interfaces.*;
import net.opentsdb.pipeline3.Abstracts.*;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class Main {

  @Benchmark
  public static void arraysOfPrimitives() {
    QueryMode mode = QueryMode.CLIENT_STREAM;
    TimeSortedDataStore store = new TimeSortedDataStore(true);
    QExecutionPipeline exec = store.new MyExecution(true, mode);
    exec = (QExecutionPipeline) new FilterNumsByString(exec);
    exec = (QExecutionPipeline) new GroupBy(exec);
    exec = (QExecutionPipeline) new DiffFromStdD(exec);
    exec = (QExecutionPipeline) new ExpressionProc(exec);
    
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
        System.out.println("DONE after " + iterations + " iterations");
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
            System.out.println(ts.id());
            if (ts.type() == NumericType.TYPE) {
              NumericTSDataType data = (NumericTSDataType) ts;
              for (int i = 0; i < data.timestamps().length; i++) {
                System.out.println("  " + (data.timestamps()[i] / 1000) + " " + 
                    (data.isIntegers() ? data.integers()[i] : data.doubles()[i]));
              }
            } else {
              StringTSDataType data = (StringTSDataType) ts;
              for (int i = 0; i < data.timestamps().length; i++) {
                System.out.println("  " + (data.timestamps()[i] / 1000) + " " + data.strings()[i]);
              }
            }
          }
          System.out.println("-------------------------");
          
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
}
