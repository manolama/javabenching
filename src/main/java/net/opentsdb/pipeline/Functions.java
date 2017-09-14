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
import net.opentsdb.pipeline.Interfaces.QResult;
import net.opentsdb.pipeline.Interfaces.StreamListener;
import net.opentsdb.pipeline.Interfaces.TS;
import net.opentsdb.pipeline.Interfaces.TSProcessor;

public class Functions {

  public static class GroupBy implements TSProcessor<NumericType>, StreamListener, QResult {
    StreamListener upstream;
    
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      for (TS<?> ts : next.series()) {
        if (hashes.contains(ts.hashCode())) {
          continue;
        }
        
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
        extant.sources.add((TS<NumericType>) ts);
        hashes.add(ts.hashCode());
      }
      
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
        if (first_run) {
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
            if (v.timestamp().msEpoch() < next_next_ts) {
              next_next_ts = v.timestamp().msEpoch();
            }
          }
        }
        
        dp.reset(new MillisecondTimeStamp(next_ts), sum, 1);
        next_ts = next_next_ts;
        return dp;
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
  }
  
}
