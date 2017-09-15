package net.opentsdb.pipeline;

import java.util.Iterator;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Functions.*;
import net.opentsdb.pipeline.Interfaces.*;

public class Main {
  
  public static void main(final String[] args) {
    version1();
  }
  
  /**
   * In this case the API gives a QueryExecutionPipeline that the user would
   * provide an asynchronous listener. To initiate the stream, the caller just
   * calls {@link QExecutionPipeline#fetchNext()}.
   */
  public static void version1() {
    /** This section would be hidden behind the query engine. Users just 
     * submit the query and the call graph is setup, yada yada. */
    TimeSortedDataStore store = new TimeSortedDataStore();
    QExecutionPipeline exec = store.new MyExecution(true);
    exec = (QExecutionPipeline) new GroupBy(exec);
    exec = (QExecutionPipeline) new DiffFromStdD(exec);
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
            Iterator<?> it = ts.iterator();
            while (it.hasNext()) {
              TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
              System.out.println("  " + v.timestamp().epoch() + " " + v.value().toDouble());
            }
          }
          System.out.println("-------------------------");
          
          iterations++;
          exec.fetchNext();
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
      listener.d.join(10000);
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
