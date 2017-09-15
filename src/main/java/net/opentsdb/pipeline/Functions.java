package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import avro.shaded.com.google.common.collect.Sets;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Implementations.*;
import net.opentsdb.pipeline.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Functions {

  /**
   * Simple SUMming group by. No interpolation at this point, just assumes zero
   * if a series doesn't have a value at some point.
   */
  public static class GroupBy implements TSProcessor<NumericType>, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    GroupBy parent;
    boolean cache = false;
    
    List<Map<TimeSeriesId, byte[]>> local_cache = Lists.newArrayList();
    int cache_idx = 0;
    
    protected GroupBy() { }
    
    public GroupBy(QExecutionPipeline downstream_execution) {
      this.upstream = downstream_execution.getListener();
      this.downstream = downstream_execution;
      downstream_execution.setListener(this);
    }
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      if (cache) {
        parent.local_cache.add(Maps.newHashMap());
      }
      
      //System.out.println("Received next...");
      for (TS<?> ts : next.series()) {
        
        if (hashes.contains(ts.hashCode())) {
          continue;
        }
        
        // naive group by on the host tag.
        TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
            .addMetric(new String(ts.id().metrics().get(0), Const.UTF8_CHARSET))
            .addTags("host", new String(ts.id().tags().get("host".getBytes(Const.UTF8_CHARSET)), Const.UTF8_CHARSET))
            .addAggregatedTag("dc")
            .build();
        GBIterator extant = (GBIterator) time_series.get(id);
        if (extant == null) {
          extant = new GBIterator(id);
          time_series.put(id, extant);
        }
        extant.addSource((TS<NumericType>) ts);
        hashes.add(ts.hashCode());
      }
      
      for (TS<?> it : time_series.values()) {
        ((GBIterator) it).reset();
      }
      //System.out.println("Calling up: " + time_series.size());
      
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }

    class GBIterator implements TS<NumericType>, Iterator<TimeSeriesValue<NumericType>> {
      TimeSeriesId id;
      List<TS<NumericType>> sources;
      List<MutableNumericType> values;
      byte[] data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      int cache_idx = 0;
      
      boolean first_run = true;
      boolean has_next = false;
      long next_ts = Long.MAX_VALUE;
      MutableNumericType dp;
      
      public GBIterator(TimeSeriesId id) {
        this.id = id;
        sources = Lists.newArrayList();
        dp = new MutableNumericType(id);
      }
      
      public void reset() {
        has_next = false;
        for (final TS<NumericType> source : sources) {
          if (source.iterator().hasNext()) {
            has_next = true;
            break;
          }
        }
        first_run = true;
        next_ts = Long.MAX_VALUE;
        cache_idx = 0;
        data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      }
      
      public void addSource(TS<NumericType> source) {
        if (source.iterator().hasNext()) {
          has_next = true;
        }
        sources.add(source);
      }
      
      @Override
      public TimeSeriesId id() {
        return id;
      }

      @Override
      public Iterator<TimeSeriesValue<NumericType>> iterator() {
        return this;
      }

      @Override
      public void setCache(boolean cache) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        has_next = false;
        try {
        if (first_run) {
          values = Lists.newArrayListWithCapacity(sources.size());
          for (final TS<NumericType> ts : sources) {
            final Iterator<TimeSeriesValue<NumericType>> it = ts.iterator();
            if (it.hasNext()) {
              TimeSeriesValue<NumericType> v = it.next();
              values.add(new MutableNumericType(v));
              if (v.timestamp().msEpoch() < next_ts) {
                next_ts = v.timestamp().msEpoch();
              }
            } else {
              values.add(null);
            }
          }
          first_run = false;
          //System.out.println("TS after first run: " + next_ts);
        }
        
        long next_next_ts = Long.MAX_VALUE;
        long sum = 0;
        for (int i = 0; i < sources.size(); i++) {
          TimeSeriesValue<NumericType> v = values.get(i);
          if (v == null) {
            // TODO - fill
            continue;
          }
          if (v.timestamp().msEpoch() == next_ts) {
            sum += v.value().longValue();
            if (sources.get(i).iterator().hasNext()) {
              v = sources.get(i).iterator().next();
              if (v.timestamp().msEpoch() < next_next_ts) {
                next_next_ts = v.timestamp().msEpoch();
              }
              values.get(i).reset(v);
              has_next = true;
            } else {
              values.set(i, null);
            }
          } else {
            if (v.timestamp().msEpoch() > next_next_ts) {
              next_next_ts = v.timestamp().msEpoch();
              has_next = true;
            }
          }
        }
        
        dp.reset(new MillisecondTimeStamp(next_ts), sum, 1);
        if (cache) {
          System.arraycopy(Bytes.fromLong(dp.timestamp().msEpoch()), 0, data, cache_idx, 8);
          cache_idx += 8;
          System.arraycopy(Bytes.fromLong(dp.longValue()), 0, data, cache_idx, 8);
          cache_idx += 8;
          if (!has_next) {
            Map<TimeSeriesId, byte[]> c = parent.local_cache.get(parent.local_cache.size() - 1);
            c.put(id, data);
          }
        }
        next_ts = next_next_ts;

        return dp;
        } catch (Exception e){ 
          e.printStackTrace();
          throw new RuntimeException("WTF?", e);
        }
      }
      
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean hasException() {
      return false;
    }

    @Override
    public void setListener(StreamListener listener) {
      upstream = listener;
    }
    
    @Override
    public void fetchNext() {
      if (local_cache.size() > 0) {
        if (cache_idx >= local_cache.size()) {
          upstream.onComplete();
          return;
        }
        
        // work from cache.
        // TODO - fall through in case the cache has been exhausted. That'll get ugly.
        Map<TimeSeriesId, byte[]> chunk = local_cache.get(cache_idx++);
        for (Entry<TimeSeriesId, byte[]> entry : chunk.entrySet()) {
          ArrayBackedLongTS extant = (ArrayBackedLongTS) time_series.get(entry.getKey());
          if (extant == null) {
            extant = new ArrayBackedLongTS(entry.getKey());
            time_series.put(entry.getKey(), extant);
          }
          extant.nextChunk(entry.getValue());
        }
        System.out.println("FED FROM CACHE!");
        upstream.onNext(this);
      } else {
        downstream.fetchNext();
      }
    }
    
    @Override
    public StreamListener getListener() {
      return upstream;
    }

    @Override
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      GroupBy clone = new GroupBy();
      clone.downstream = downstream.getMultiPassClone(clone);
      clone.parent = this;
      clone.cache = true;
      clone.upstream = listener;
      return clone;
    }

    @Override
    public void setCache(boolean cache) {
      this.cache = cache;
    }
    
    @Override
    public QueryMode getMode() {
      return downstream.getMode();
    }
  }

  /**
   * Two pass processor that iterates over the entire stream, accumulating the 
   * sum of squares and counts. This is performed on the first call to {@link #fetchNext()}.
   * After that's done then we will setup iterators for every time series encountered
   * and store the standard deviation. Then when the upstream caller starts iterating,
   * we return the difference for the data point from the standard deviation.
   */
  public static class DiffFromStdD implements TSProcessor<NumericType>, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Map<TimeSeriesId, Pair<Long, Double>> sums = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    boolean initialized = false;
    
    public DiffFromStdD(QExecutionPipeline downstream_execution) {
      this.upstream = downstream_execution.getListener();
      this.downstream = downstream_execution;
      downstream_execution.setListener(this);
    }
    
    @Override
    public void setListener(StreamListener listener) {
      upstream = listener;
    }

    @Override
    public StreamListener getListener() {
      return upstream;
    }
    
    @Override
    public void fetchNext() {
      if (initialized) {
        downstream.fetchNext();
      } else {
        FirstPassListener fpl = new FirstPassListener();
        QExecutionPipeline fp = downstream.getMultiPassClone(fpl);
        fpl.downstream = fp;
        initialized = true;
        fp.fetchNext();
      }
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean hasException() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      for (TS<?> ts : next.series()) {
        SIt it = (SIt) time_series.get(ts.id());
        it.source = (TS<NumericType>) ts;
      }

      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }

    @Override
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      // TODO Auto-generated method stub
      return null;
    }
    
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
    }
    
    class SIt implements TS<NumericType>, Iterator<TimeSeriesValue<NumericType>> {

      TS<NumericType> source;
      double stdev;
      MutableNumericType dp;
      
      public SIt(final TimeSeriesId id) {
        dp = new MutableNumericType(id);
      }
      
      @Override
      public boolean hasNext() {
        return source == null ? false : source.iterator().hasNext();
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        TimeSeriesValue<NumericType> next = source.iterator().next();
        dp.reset(next.timestamp(), stdev - next.value().toDouble(), 1);
        return dp;
      }

      @Override
      public TimeSeriesId id() {
        return dp.id();
      }

      @Override
      public Iterator<TimeSeriesValue<NumericType>> iterator() {
        return this;
      }

      @Override
      public void setCache(boolean cache) {
        // TODO Auto-generated method stub
        
      }
      
    }

    class FirstPassListener implements StreamListener {
      QExecutionPipeline downstream;
      
      @Override
      public void onComplete() {
        System.out.println("COMPLETE with the first pass!");
        
        // setup the new iterators
        for (Entry<TimeSeriesId, Pair<Long, Double>> series : sums.entrySet()) {
          SIt it = new SIt(series.getKey());
          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
          System.out.println("STD: " + it.stdev);
          // PURPOSELY not setting the source here.
          time_series.put(it.id(), it);
        }
        
        DiffFromStdD.this.fetchNext();
      }

      @Override
      public void onNext(QResult next) {
        for (TS<?> ts : next.series()) {
          Pair<Long, Double> pair = sums.get(ts.id());
          double sum_of_squares = pair == null ? 0 : pair.getValue();
          long count = pair == null ? 0 : pair.getKey();
          
          Iterator<?> it = ts.iterator();
          while(it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            sum_of_squares += Math.pow(v.value().toDouble(), 2);
            count++;
          }
          sums.put(ts.id(), new Pair<Long, Double>(count, sum_of_squares));
        }
        downstream.fetchNext();
      }

      @Override
      public void onError(Throwable t) {
        DiffFromStdD.this.onError(t);
      }
      
    }

    @Override
    public QueryMode getMode() {
      return downstream.getMode();
    }
  }
  
}
