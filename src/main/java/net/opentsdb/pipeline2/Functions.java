package net.opentsdb.pipeline2;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import avro.shaded.com.google.common.collect.Sets;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline2.Abstracts.StringType;
import net.opentsdb.pipeline2.Implementations.*;
import net.opentsdb.pipeline2.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Functions {
  
  public static class FilterNumsByString implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    
    protected FilterNumsByString() { }
    
    public FilterNumsByString(QExecutionPipeline downstream_execution) {
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
      downstream.fetchNext();
    }

    @Override
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      FilterNumsByString clone = new FilterNumsByString();
      clone.downstream = downstream.getMultiPassClone(clone);
//      clone.parent = this;
//      clone.cache = true;
      clone.upstream = listener;
      return clone;
    }

    @Override
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryMode getMode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      for (TS<?> ts : next.series()) {
        TS<?> it = time_series.get(ts.id());
        if (it == null) {
          it = new FilterIterator();
          time_series.put(ts.id(), it);
        }
        ((FilterIterator) it).setTS(ts);
      }
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class FilterIterator implements TS<NumericType> {
      TS<NumericType> number;
      TS<StringType> string;
      
      public void setTS(TS<?> ts) {
        if (ts.type() == NumericType.TYPE && number == null) {
          number = (TS<NumericType>) ts;
        } else if (string == null) {
          string = (TS<StringType>) ts;
        }
      }
      
      @Override
      public TimeSeriesId id() {
        return number == null ? string.id() : number.id();
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public List<TimeSeriesValue<NumericType>> data() {
        List<TimeSeriesValue<NumericType>> output = Lists.newArrayList();
        
        for (int i = 0; i < number.data().size(); i++) {
          if (string.data().get(i).value().values().get(0).equals("foo")) {
            output.add(number.data().get(i));
          }
        }
        
        return output;
      }
      
    }
    
  }
  
  public static class GroupBy implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    GroupBy parent;
    boolean cache = false;
    List<Map<TimeSeriesId, List<TimeSeriesValue<?>>>> local_cache = Lists.newArrayList();
    int cache_idx = 0;
    
    protected GroupBy() { }
    
    public GroupBy(QExecutionPipeline downstream_execution) {
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
      if (local_cache.size() > 0) {
        if (cache_idx >= local_cache.size()) {
          upstream.onComplete();
          return;
        }
        
        // work from cache.
        // TODO - fall through in case the cache has been exhausted. That'll get ugly.
        Map<TimeSeriesId, List<TimeSeriesValue<? extends TimeSeriesDataType>>> chunk = local_cache.get(cache_idx++);
        for (Entry<TimeSeriesId, List<TimeSeriesValue<? extends TimeSeriesDataType>>> entry : chunk.entrySet()) {
          LocalNumericTS extant = (LocalNumericTS) time_series.get(entry.getKey());
          if (extant == null) {
            extant = new LocalNumericTS(entry.getKey());
            time_series.put(entry.getKey(), extant);
          }
          extant.addData(entry.getValue());
        }
        System.out.println("FED FROM CACHE!");
        upstream.onNext(this);
      } else {
        downstream.fetchNext();
      }
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
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryMode getMode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
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
      
      time_series.clear();
      //System.out.println("Received next...");
      for (TS<?> ts : next.series()) {
        if (ts.type() != NumericType.TYPE) {
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
        extant.sources.add((TS<NumericType>) ts);
      }
      
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class GBIterator implements TS<NumericType> {
      TimeSeriesId id;
      List<TS<NumericType>> sources;
      
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
      
      @Override
      public TimeSeriesId id() {
        return id;
      }

      @Override
      public List<TimeSeriesValue<NumericType>> data() {
        int[] idxs = new int[sources.size()];
        List<TimeSeriesValue<NumericType>> output = Lists.newArrayList();
        long last_ts = -1;
        long sum = 0;
        boolean initial = true;
        while(true) {
          long next_ts = Long.MAX_VALUE;
          
          boolean has_next = false;
          for (int i = 0; i < sources.size(); i++) {
            if (idxs[i] >= sources.get(i).data().size()) {
              continue;
            }
            
            TimeSeriesValue<NumericType> dp = sources.get(i).data().get(idxs[i]); 
            
            if (last_ts == dp.timestamp().msEpoch()) {
              sum += dp.value().longValue();
              if (idxs[i] < sources.get(i).data().size() - 1) {
                idxs[i]++;
                dp = sources.get(i).data().get(idxs[i]); 
                if (dp.timestamp().msEpoch() < next_ts) {
                  next_ts = dp.timestamp().msEpoch();
                }
                has_next = true;
              }
            } else {
              if (dp.timestamp().msEpoch() < next_ts) {
                next_ts = dp.timestamp().msEpoch();
              }
              if (idxs[i] < sources.get(i).data().size()) {
                has_next = true;
              }
            }
          }
          
          if (initial) {
            initial = false;
          } else {
            output.add(new MutableNumericType(id, new MillisecondTimeStamp(last_ts), sum, 1));
          }
          last_ts = next_ts;
          
          if (!has_next) {
            break;
          }
        }
        
        if (cache) {
          Map<TimeSeriesId, List<TimeSeriesValue<?>>> c = parent.local_cache.get(parent.local_cache.size() - 1);
          // WTF casting!?!
          List<TimeSeriesValue<?>> o = Lists.newArrayList(output);
          c.put(id, o);
        }
        
        return output;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
    }
    
  }

  public static class DiffFromStdD implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Map<TimeSeriesId, Pair<Long, Double>> sums = Maps.newHashMap();
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
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryMode getMode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      for (TS<?> ts : next.series()) {
        if (ts.type() != NumericType.TYPE) {
          continue;
        }
        SIt it = (SIt) time_series.get(ts.id());
        it.source = (TS<NumericType>) ts;
      }

      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class SIt implements TS<NumericType> {

      TS<NumericType> source;
      double stdev;
      
      @Override
      public TimeSeriesId id() {
        return source.id();
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public List<TimeSeriesValue<NumericType>> data() {
        List<TimeSeriesValue<NumericType>> new_values = Lists.newArrayListWithCapacity(source.data().size());
        for (final TimeSeriesValue<NumericType> dp : source.data()) {
          new_values.add(new MutableNumericType(source.id(), dp.timestamp(), stdev - dp.value().toDouble(), 1));
        }
        return new_values;
      }
      
    }
    
    class FirstPassListener implements StreamListener {
      QExecutionPipeline downstream;
      
      @Override
      public void onComplete() {
        System.out.println("COMPLETE with the first pass!");
        
        // setup the new iterators
        for (Entry<TimeSeriesId, Pair<Long, Double>> series : sums.entrySet()) {
          SIt it = new SIt();
          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
          System.out.println("STD: " + it.stdev);
          // PURPOSELY not setting the source here.
          time_series.put(series.getKey(), it);
        }
        
        DiffFromStdD.this.fetchNext();
      }

      @Override
      public void onNext(QResult next) {
        try {
        for (TS<?> ts : next.series()) {
          if (ts.type() != NumericType.TYPE) {
            continue;
          }
          Pair<Long, Double> pair = sums.get(ts.id());
          double sum_of_squares = pair == null ? 0 : pair.getValue();
          long count = pair == null ? 0 : pair.getKey();
          
          for (TimeSeriesValue<?> dp : ts.data()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) dp;
            sum_of_squares += Math.pow(v.value().toDouble(), 2);
            count++;
          }
          sums.put(ts.id(), new Pair<Long, Double>(count, sum_of_squares));
        }
        
        downstream.fetchNext();
        }        catch (Exception e) {
          e.printStackTrace();
        }
      }

      @Override
      public void onError(Throwable t) {
        DiffFromStdD.this.onError(t);
      }
      
    }
  }

  public static class ExpressionProc implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    
    public ExpressionProc(QExecutionPipeline downstream_execution) {
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
      downstream.fetchNext();
    }

    @Override
    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setCache(boolean cache) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryMode getMode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TS<?>> series() {
      return time_series.values();
    }

    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      for (TS<?> ts : next.series()) {
        if (ts.type() != NumericType.TYPE) {
          continue;
        }
        
        SimpleStringTimeSeriesId.Builder builder = SimpleStringTimeSeriesId.newBuilder()
            .addMetric("Sum of if in and out");
        for (Entry<byte[], byte[]> pair : ts.id().tags().entrySet()) {
          builder.addTags(new String(pair.getKey(), Const.UTF8_CHARSET), 
              new String(pair.getValue(), Const.UTF8_CHARSET));
        }
        for (byte[] tag : ts.id().aggregatedTags()) {
          builder.addAggregatedTag(new String(tag, Const.UTF8_CHARSET));
        }
        
        TS<?> it = time_series.get(builder.build());
        if (it == null) {
          it = new ExpressionIterator(builder.build());
          time_series.put(builder.build(), it);
        }
        ((ExpressionIterator) it).addSeries(ts);
      }
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class ExpressionIterator implements TS<NumericType> {
      Map<String, TS<?>> series = Maps.newHashMap();
      TimeSeriesId id;
      
      public ExpressionIterator(TimeSeriesId id) {
        this.id = id;
      }
      
      public void addSeries(TS<?> ts) {
        if (Bytes.memcmp(ts.id().metrics().get(0), "sys.if.out".getBytes(Const.UTF8_CHARSET)) == 0) {
          series.put("sys.if.out", ts);
        } else {
          series.put("sys.if.in", ts);
        }
      }

      @Override
      public TimeSeriesId id() {
        return id;
      }
      
      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public List<TimeSeriesValue<NumericType>> data() {
        List<TimeSeriesValue<NumericType>> output = Lists.newArrayList();
        for (int i = 0; i < series.get("sys.if.out").data().size(); i++) {
          double sum = 0;
          for (TS<?> series : series.values()) {
            TS<NumericType> s = (TS<NumericType>) series;
            sum += s.data().get(i).value().toDouble();
          }
          output.add(new MutableNumericType(id, series.get("sys.if.out").data().get(i).timestamp(), sum, 1));
        }
        return output;
      }
      
    }
  }
}
