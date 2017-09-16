package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Collections;
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
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.*;
import net.opentsdb.pipeline.Functions.*;
import net.opentsdb.pipeline.Implementations.*;
import net.opentsdb.pipeline.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

/**
 * And example data source. It just
 */
public class TimeSortedDataStore {
  public static final long HOSTS = 4;
  public static final long INTERVAL = 1000;
  public static final long INTERVALS = 8;
  public static final int INTERVALS_PER_CHUNK = 4;
  public static final List<String> DATACENTERS = Lists.newArrayList(
      "PHX", "LGA", "LAX", "DEN");
  public static final List<String> METRICS = Lists.newArrayList(
      "sys.cpu.user", "sys.if.out", "sys.if.in", "web.requests");

  ExecutorService pool = Executors.newFixedThreadPool(1);
  List<TimeSeriesId> timeseries;
  long start_ts = 0; // in ms
  boolean with_strings;
  
  public TimeSortedDataStore(boolean with_strings) {
    this.with_strings = with_strings;
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
  
  class MyExecution implements QExecutionPipeline, Supplier<Void> {
    boolean reverse_chunks = false;
    StreamListener listener;
    long ts;
    QueryMode mode;
    
    Map<TimeSeriesId, TS<?>> num_map = Maps.newHashMap();
    Map<TimeSeriesId, TS<?>> string_map = Maps.newHashMap();
    Results results = new Results(num_map, string_map);
    
    public MyExecution(boolean reverse_chunks, QueryMode mode) {
      this.reverse_chunks = reverse_chunks;
      ts = reverse_chunks ? start_ts + INTERVALS * INTERVAL : start_ts;
      this.mode = mode;
    }
    
    @Override
    public void fetchNext() {
      System.out.println(".... fetching from store");
      if (reverse_chunks ? ts <= start_ts : ts >= start_ts + (INTERVALS * INTERVAL)) {
        listener.onComplete();
        return;
      }
      
      for (int x = 0; x < timeseries.size(); x++) {
        if (Bytes.memcmp("web.requests".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0) {
          continue;
        }
        List<Pair<Long, String>> strings = Lists.newArrayListWithCapacity(INTERVALS_PER_CHUNK);
        // for now add em all
        byte[] payload = new byte[INTERVALS_PER_CHUNK * 16];
        int idx = reverse_chunks ? payload.length - 8 : 0;
        long local_ts = ts;
        if (reverse_chunks) {
          for (int i = INTERVALS_PER_CHUNK - 1; i >= 0; i--) {
            System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
            //System.arraycopy(Bytes.fromLong(1), 0, payload, idx, 8);
            idx -= 8;
            System.arraycopy(Bytes.fromLong(local_ts), 0, payload, idx, 8);
            idx -= 8;
            strings.add(new Pair<Long, String>(local_ts, i % 2 == 0 ? "foo" : "bar"));
            local_ts -= INTERVAL;
          }
          Collections.reverse(strings);
        } else {
          for (int i = 0; i < INTERVALS_PER_CHUNK; i++) {
            System.arraycopy(Bytes.fromLong(local_ts), 0, payload, idx, 8);
            idx += 8;
            System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
            //System.arraycopy(Bytes.fromLong(1), 0, payload, idx, 8);
            idx += 8;
            strings.add(new Pair<Long, String>(local_ts, i % 2 == 0 ? "foo" : "bar"));
            local_ts += INTERVAL;
          }
        }
        
        TS<?> t = num_map.get(timeseries.get(x));
        if (t == null) {
          t = new ArrayBackedLongTS(timeseries.get(x));
          num_map.put(timeseries.get(x), t);
        }
        ((MyTS<?>) t).nextChunk(payload);
          
        if (with_strings) {
          t = string_map.get(timeseries.get(x));
          if (t == null) {
            t = new ListBackedStringTS(timeseries.get(x));
            string_map.put(timeseries.get(x), t);
          }
          ((ListBackedStringTS) t).setStrings(strings);
        }
      }
      if (reverse_chunks) {
        ts -= INTERVALS_PER_CHUNK * INTERVAL;
      } else {
        ts += INTERVALS_PER_CHUNK * INTERVAL;
      }
      
      CompletableFuture<Void> f = CompletableFuture.supplyAsync(this, pool);
      switch (mode) {
      case SINGLE:
      case CLIENT_STREAM:
        // nothing to do here
        break;
      case SERVER_SYNC_STREAM:
        // TODO - walk to the ROOT execution and call that.
        if (reverse_chunks && ts <= start_ts) {
          f.thenAccept(obj -> {
            fetchNext();
          });
        } else if (ts >= start_ts + (INTERVALS * INTERVAL)) {
          f.thenAccept(obj -> {
            fetchNext();
          });
        }
        break;
      case SERVER_ASYNC_STREAM:
        // TODO - walk to the ROOT execution and call that.
        fetchNext();
        break;
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

    @Override
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      QExecutionPipeline ex = new MyExecution(reverse_chunks, mode);
      ex.setListener(listener);
      return ex;
    }

    @Override
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryMode getMode() {
      return mode;
    }
    
  }
  
  public static class Results implements QResult {

    Map<TimeSeriesId, TS<?>> num_map;
    Map<TimeSeriesId, TS<?>> string_map;
    
    public Results(Map<TimeSeriesId, TS<?>> num_map, Map<TimeSeriesId, TS<?>> string_map) {
      this.num_map = num_map;
      this.string_map = string_map;
    }
    
    @Override
    public Collection<TS<?>> series() {
      List<TS<?>> results = Lists.newArrayListWithCapacity(num_map.size() + string_map.size());
      results.addAll(num_map.values());
      results.addAll(string_map.values());
      return results;
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
  
  
}
