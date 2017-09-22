package net.opentsdb.pipeline4;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.collect.Lists;

import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.pipeline4.Abstracts.*;
import net.opentsdb.pipeline.BaseTimeSortedDataStore;
import net.opentsdb.pipeline4.Implementations.*;
import net.opentsdb.pipeline4.Interfaces.*;

/**
 * And example data source. 
 */
public class TimeSortedDataStore extends BaseTimeSortedDataStore {

  public TimeSortedDataStore(boolean with_strings) {
    super(with_strings);
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
      if (reverse_chunks ? ts <= start_ts : ts >= start_ts + (INTERVALS * INTERVAL)) {
        listener.onComplete();
        return;
      }
      
      Map<TimeSeriesId, byte[]> nums = getChunk(DataType.NUMBERS, ts, reverse_chunks);
      for (Entry<TimeSeriesId, byte[]> entry : nums.entrySet()) {
        TS<?> t = num_map.get(entry.getKey());
        if (t == null) {
          t = new ArrayBackedLongTS(entry.getKey());
          num_map.put(entry.getKey(), t);
        }
        ((BaseTS<?>) t).nextChunk(entry.getValue());
      }
      
      if (with_strings) {
        Map<TimeSeriesId, byte[]> strings = getChunk(DataType.STRINGS, ts, reverse_chunks);
        for (Entry<TimeSeriesId, byte[]> entry : strings.entrySet()) {
          TS<?> t = string_map.get(entry.getKey());
          if (t == null) {
            t = new ArrayBackedStringTS(entry.getKey());
            string_map.put(entry.getKey(), t);
          }
          ((BaseTS<?>) t).nextChunk(entry.getValue());
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
    
  }
  
  
}
