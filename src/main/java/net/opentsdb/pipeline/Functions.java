package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stumbleupon.async.Deferred;

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
import net.opentsdb.pipeline.Interfaces.QExecution;
import net.opentsdb.pipeline.Interfaces.QResult;
import net.opentsdb.pipeline.Interfaces.StreamListener;
import net.opentsdb.pipeline.Interfaces.TS;
import net.opentsdb.pipeline.Interfaces.TSProcessor;

public class Functions {

  public static class GroupBy implements TSProcessor<NumericType>, StreamListener, QResult, QExecution {
    StreamListener upstream;
    QExecution downstream_execution;
    
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    
    public GroupBy(QExecution downstream_execution) {
      this.upstream = downstream_execution.getListener();
      this.downstream_execution = downstream_execution;
      downstream_execution.setListener(this);
    }
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
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
        double sum = 0;
        for (int i = 0; i < sources.size(); i++) {
          TimeSeriesValue<NumericType> v = values.get(i);
          if (v == null) {
            // TODO - fill
            continue;
          }
          if (v.timestamp().msEpoch() == next_ts) {
            sum += v.value().toDouble();
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
        next_ts = next_next_ts;
        //System.out.println("Returning dp: " + dp.timestamp() + " " + dp.toDouble());
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
    public boolean endOfStream() {
      return downstream_execution.endOfStream();
    }

    @Override
    public void fetchNext() {
      downstream_execution.fetchNext();
    }
    
    @Override
    public StreamListener getListener() {
      return upstream;
    }
  }
  
}
