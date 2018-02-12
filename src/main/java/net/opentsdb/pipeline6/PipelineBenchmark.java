// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.pipeline6;

import java.util.Iterator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.stumbleupon.async.Deferred;

import net.opentsdb.pipeline6.TimeSortedDataStore;
import net.opentsdb.pipeline6.Functions.*;
import net.opentsdb.pipeline6.Interfaces.*;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class PipelineBenchmark {

  /**
   * In this case the API gives a QueryExecutionPipeline that the user would
   * provide an asynchronous listener. To initiate the stream, the caller just
   * calls {@link QExecutionPipeline#fetchNext()}.
   */
  @Benchmark
  public static void iteratorsWithXXHashIds(Blackhole black_hole) {
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
}
