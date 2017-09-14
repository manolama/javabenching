package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.*;
import net.opentsdb.pipeline.Functions.*;
import net.opentsdb.pipeline.Interfaces.*;
import net.opentsdb.utils.Bytes;

public class TimeSortedDataStore {
  public static final long HOSTS = 4;
  public static final long INTERVAL = 1000;
  public static final long INTERVALS = 6;
  public static final int INTERVALS_PER_CHUNK = 3;
  public static final List<String> DATACENTERS = Lists.newArrayList(
      "PHX", "LGA", "LAX", "DEN");
  public static final List<String> METRICS = Lists.newArrayList(
      "sys.cpu.user", "sys.if.out", "sys.if.in", "web.requests");

  ExecutorService pool = Executors.newFixedThreadPool(1);
  List<TimeSeriesId> timeseries;
  long start_ts = 0; // in ms
  
  public TimeSortedDataStore() {
    timeseries = Lists.newArrayList();
    
    for (final String metric : METRICS) {
      for (final String dc : DATACENTERS) {
        for (int h = 0; h < HOSTS; h++) {
          TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
              .addMetric(metric)
              .addTags("dc", dc)
              .addTags("host", String.format("web%02d", h + 1))
              .build();
          timeseries.add(id);
        }
      }
    }
  }
  
  class MyExecution implements QExecution, Supplier<Void> {
    boolean reverse_chunks = false;
    StreamListener listener;
    long ts;
    
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Results results = new Results(time_series);
    
    public MyExecution(boolean reverse_chunks) {
      this.reverse_chunks = reverse_chunks;
      ts = reverse_chunks ? start_ts + INTERVALS * INTERVAL : start_ts;
    }
    
    @Override
    public boolean endOfStream() {
      return reverse_chunks ? ts <= start_ts : 
        ts >= start_ts + (INTERVALS * INTERVAL);
    }

    @Override
    public void fetchNext() {
      
      for (int x = 0; x < timeseries.size(); x++) {
        if (Bytes.memcmp("web.requests".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0) {
          continue;
        }
        // for now add em all
        byte[] payload = new byte[INTERVALS_PER_CHUNK * 16];
        int idx = reverse_chunks ? payload.length - 8 : 0;
        long local_ts = ts;
        if (reverse_chunks) {
          for (int i = INTERVALS_PER_CHUNK - 1; i >= 0; i--) {
            //System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
            System.arraycopy(Bytes.fromLong(1), 0, payload, idx, 8);
            idx -= 8;
            System.arraycopy(Bytes.fromLong(local_ts), 0, payload, idx, 8);
            idx -= 8;
            local_ts -= INTERVAL;
          }
        } else {
          for (int i = 0; i < INTERVALS_PER_CHUNK; i++) {
            System.arraycopy(Bytes.fromLong(local_ts), 0, payload, idx, 8);
            idx += 8;
            //System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
            System.arraycopy(Bytes.fromLong(1), 0, payload, idx, 8);
            idx += 8;
            local_ts += INTERVAL;
          }
        }
        
        TS<?> t = time_series.get(timeseries.get(x));
        if (t == null) {
          t = new MyNumTS(timeseries.get(x));
          time_series.put(timeseries.get(x), t);
        }
        ((MyTS<?>) t).nextChunk(payload);
      }
      if (reverse_chunks) {
        ts -= INTERVALS_PER_CHUNK * INTERVAL;
      } else {
        ts += INTERVALS_PER_CHUNK * INTERVAL;
      }
      
      CompletableFuture<Void> f = CompletableFuture.supplyAsync(this, pool);
      if (reverse_chunks && ts <= start_ts) {
        f.thenAccept(obj -> {
          listener.onComplete();
        });
      } else if (ts >= start_ts + (INTERVALS * INTERVAL)) {
        f.thenAccept(obj -> {
          listener.onComplete();
        });
      }
    }

    @Override
    public void setListener(StreamListener listener) {
      this.listener = listener;
    }

    @Override
    public Void get() {
      listener.onNext(results);
      return null;
    }

    @Override
    public StreamListener getListener() {
      return listener;
    }
    
  }
  
  class Results implements QResult {

    Map<TimeSeriesId, TS<?>> time_series;
    
    public Results(Map<TimeSeriesId, TS<?>> time_series) {
      this.time_series = time_series;
    }
    
    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public Throwable exception() {
      return null;
    }

    @Override
    public boolean hasException() {
      return false;
    }
    
  }
  
  class MyNumTS extends MyTS<NumericType> implements Iterator<TimeSeriesValue<NumericType>> {
    
    TimeStamp ts = new MillisecondTimeStamp(0);
    MutableNumericType dp;
    
    public MyNumTS(final TimeSeriesId id) {
      super(id);
      dp = new MutableNumericType(id);
    }
    
    @Override
    public Iterator<TimeSeriesValue<NumericType>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return idx < dps.length;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      ts.updateMsEpoch(Bytes.getLong(dps, idx));
      idx += 8;
      dp.reset(ts, Bytes.getLong(dps, idx), 1);
      idx += 8;
      return dp;
    }
  }
  
  class MyStringTS implements TS<AnnotationType> {

    @Override
    public TimeSeriesId id() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Iterator<TimeSeriesValue<AnnotationType>> iterator() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  public static void main(final String[] args) {
    TimeSortedDataStore store = new TimeSortedDataStore();
    QExecution exec = store.new MyExecution(true);
    exec = (QExecution) new GroupBy(exec);
    
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
