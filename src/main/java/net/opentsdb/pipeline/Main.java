package net.opentsdb.pipeline;

import java.util.Iterator;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Functions.*;
import net.opentsdb.pipeline.Interfaces.*;

public class Main {
  public static void main(final String[] args) {
    TimeSortedDataStore store = new TimeSortedDataStore();
    QExecution exec = store.new MyExecution(true);
    exec = (QExecution) new GroupBy(exec);
    exec = (QExecution) new DiffFromStdD(exec);
    
    class MyListener implements StreamListener {
      QExecution exec;
      int iterations = 0;
      Deferred<Object> d = new Deferred<Object>();
      
      public MyListener(QExecution exec) {
        this.exec = exec;
      }
      
      @Override
      public void onComplete() {
        System.out.println("DONE after " + iterations + " iterations");
        d.callback(null);
      }

      @Override
      public void onNext(QResult next) {
        try {
          System.out.println("Gonna iterate me some data");
          for (TS<?> ts : next.series()) {
  //          if (Bytes.memcmp("web.requests".getBytes(Const.UTF8_CHARSET), ts.id().metrics().get(0)) != 0) {
  //            continue;
  //          }
  //          if (Bytes.memcmp("PHX".getBytes(Const.UTF8_CHARSET), ts.id().tags().get("dc".getBytes(Const.UTF8_CHARSET))) != 0) {
  //            continue;
  //          }
  //          if (Bytes.memcmp("web01".getBytes(Const.UTF8_CHARSET), ts.id().tags().get("host".getBytes(Const.UTF8_CHARSET))) != 0) {
  //            continue;
  //          }
            System.out.println(ts.id());
            Iterator<?> it = ts.iterator();
            while (it.hasNext()) {
              TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
              System.out.println("  " + v.timestamp().epoch() + " " + v.value().toDouble());
            }
          }
          System.out.println("-------------------------");
          
          iterations++;
          if (!exec.endOfStream()) {
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
    
    MyListener listener = new MyListener(exec);
    exec.setListener(listener);
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
