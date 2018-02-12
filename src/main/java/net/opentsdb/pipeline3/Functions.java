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
package net.opentsdb.pipeline3;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline3.Abstracts.*;
import net.opentsdb.pipeline3.Implementations.*;
import net.opentsdb.pipeline3.Interfaces.*;
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
    
    class FilterIterator extends NumericTSDataType {
      NumericTSDataType number;
      StringTSDataType string;
      long[] timestamps;
      long[] integers;
      
      public void setTS(TS<?> ts) {
        if (ts.type() == NumericType.TYPE && number == null) {
          number = (NumericTSDataType) ts;
        } else if (string == null) {
          string = (StringTSDataType) ts;
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

      void build() {
        timestamps = new long[number.timestamps().length];
        integers = new long[number.timestamps().length];
        int idx = 0;
        
        for (int i = 0; i < string.strings().length; i++) {
          if (string.strings()[i].equals("foo")) {
            timestamps[idx] = number.timestamps()[i];
            integers[idx++] = number.integers()[i];
          }
        }
        
        if (idx < number.timestamps().length) {
          timestamps = Arrays.copyOf(timestamps, idx);
          integers = Arrays.copyOf(integers, idx);
        }
      }
      
//      @Override
//      public List<TimeSeriesValue<NumericType>> data() {
//        List<TimeSeriesValue<NumericType>> output = Lists.newArrayList();
//        
//        for (int i = 0; i < number.data().size(); i++) {
//          if (string.data().get(i).value().values().get(0).equals("foo")) {
//            output.add(number.data().get(i));
//          }
//        }
//        
//        return output;
//      }

      @Override
      public long[] timestamps() {
        if (timestamps == null) {
          build();
        }
        return timestamps;
      }

      @Override
      public long[] integers() {
        if (integers == null) {
          build();
        }
        return integers;
      }

      @Override
      public double[] doubles() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean isIntegers() {
        return true;
      }

      @Override
      protected void reset() {
        // TODO Auto-generated method stub
        
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
    List<Map<TimeSeriesId, MyNumeric>> local_cache = Lists.newArrayList();
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
        Map<TimeSeriesId, MyNumeric> chunk = local_cache.get(cache_idx++);
        time_series.clear();
        time_series.putAll(chunk);
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
      for (TS<?> ts : next.series()) {
        if (ts.type() != NumericType.TYPE) {
          continue;
        }
        
        // naive group by on the host tag.
        TimeSeriesId id = BaseTimeSeriesId.newBuilder()
            .setMetric(ts.id().metric())
            .addTags("host", ts.id().tags().get("host"))
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
    
    class GBIterator extends NumericTSDataType {
      long[] timestamps;
      long[] integers;
      double[] doubles;
      TimeSeriesId id;
      List<TS<NumericType>> sources;
      byte[] data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      int cache_idx = 0;
      long next_ts = Long.MAX_VALUE;
      
      public GBIterator(TimeSeriesId id) {
        this.id = id;
        sources = Lists.newArrayList();
      }
      
      @Override
      public TimeSeriesId id() {
        return id;
      }

      void build() {
        int[] idxs = new int[sources.size()];
        long[] temp_ts = new long[4]; // TODO - get longest
        long[] temp_ints = new long[4];
        int idx = 0;
        
        long last_ts = -1;
        
        boolean initial = true;
        while(true) {
          long next_ts = Long.MAX_VALUE;
          long sum = 0;
          boolean has_next = false;
          for (int i = 0; i < sources.size(); i++) {
            NumericTSDataType source = (NumericTSDataType) sources.get(i);
            if (idxs[i] >= source.timestamps().length) {
              continue;
            }
            
            if (last_ts == source.timestamps()[idxs[i]]) {
              sum += source.integers()[idxs[i]];
              if (idxs[i] < source.timestamps().length- 1) {
                idxs[i]++;
                if (source.timestamps()[idxs[i]] < next_ts) {
                  next_ts = source.timestamps()[idxs[i]];
                }
                has_next = true;
              }
            } else {
              if (source.timestamps()[idxs[i]] < next_ts) {
                next_ts = source.timestamps()[idxs[i]];
              }
              if (idxs[i] < source.timestamps().length) {
                has_next = true;
              }
            }
          }
          
          if (initial) {
            initial = false;
          } else {
            temp_ts[idx] = last_ts;
            temp_ints[idx++] = sum;
            if (cache) {
              System.arraycopy(Bytes.fromLong(last_ts), 0, data, cache_idx, 8);
              cache_idx += 8;
              System.arraycopy(Bytes.fromLong(sum), 0, data, cache_idx, 8);
              cache_idx += 8;
            }
          }
          last_ts = next_ts;
          
          if (!has_next) {
            timestamps = Arrays.copyOf(temp_ts, idx);
            integers = Arrays.copyOf(temp_ints, idx);
            if (cache) {
              Map<TimeSeriesId, MyNumeric> c = parent.local_cache.get(parent.local_cache.size() - 1);
              MyNumeric ts = new MyNumeric(id);
              ts.setData(Arrays.copyOf(data, idx * 16), idx, true);
              c.put(id, ts);
            }
            break;
          }
        }
        
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public long[] timestamps() {
        if (timestamps == null) {
          build();
        }
        return timestamps;
      }

      @Override
      public long[] integers() {
        if (integers == null) {
          build();
        }
        return integers;
      }

      @Override
      public double[] doubles() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean isIntegers() {
        return true;
      }

      @Override
      protected void reset() {
        // TODO Auto-generated method stub
        
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
        it.source = (NumericTSDataType) ts;
      }

      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class SIt extends NumericTSDataType {
      long[] timestamps;
      double[] doubles;
      
      NumericTSDataType source;
      double stdev;

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
      @Override
      public TimeSeriesId id() {
        return source.id();
      }

      void build() {
        timestamps = new long[source.timestamps().length];
        doubles = new double[source.timestamps().length];
        
        for (int i = 0; i < source.timestamps().length; i++) {
          timestamps[i] = source.timestamps()[i];
          doubles[i] = source.isIntegers() ? stdev - source.integers()[i] : stdev - source.doubles()[i];
        }
      }
      
      @Override
      public long[] timestamps() {
        if (timestamps == null) {
          build();
        }
        return timestamps;
      }

      @Override
      public long[] integers() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public double[] doubles() {
        if (doubles == null) {
          build();
        }
        return doubles;
      }

      @Override
      public boolean isIntegers() {
        return false;
      }

      @Override
      protected void reset() {
        // TODO Auto-generated method stub
        
      }

//      @Override
//      public List<TimeSeriesValue<NumericType>> data() {
//        List<TimeSeriesValue<NumericType>> new_values = Lists.newArrayListWithCapacity(source.data().size());
//        for (final TimeSeriesValue<NumericType> dp : source.data()) {
//          new_values.add(new MutableNumericType(source.id(), dp.timestamp(), stdev - dp.value().toDouble(), 1));
//        }
//        return new_values;
//      }
      
    }
    
    class FirstPassListener implements StreamListener {
      QExecutionPipeline downstream;
      
      @Override
      public void onComplete() {
        // setup the new iterators
        for (Entry<TimeSeriesId, Pair<Long, Double>> series : sums.entrySet()) {
          SIt it = new SIt();
          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
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
          
          NumericTSDataType t = (NumericTSDataType) ts;
          
          for (int i = 0; i < t.timestamps().length; i++) {
            sum_of_squares += Math.pow(t.isIntegers() ? t.integers()[i] : t.doubles()[i], 2);
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
        
        BaseTimeSeriesId.Builder builder = BaseTimeSeriesId.newBuilder()
            .setMetric("Sum of if in and out");
        for (Entry<String, String> pair : ts.id().tags().entrySet()) {
          builder.addTags(pair.getKey(), pair.getValue());
        }
        for (String tag : ts.id().aggregatedTags()) {
          builder.addAggregatedTag(tag);
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
    
    class ExpressionIterator extends NumericTSDataType {
      Map<String, NumericTSDataType> series = Maps.newHashMap();
      TimeSeriesId id;
      long[] timestamps;
      double[] doubles;
      
      public ExpressionIterator(TimeSeriesId id) {
        this.id = id;
      }
      
      public void addSeries(TS<?> ts) {
        if ("sys.if.out".equals(ts.id().metric())) {
          series.put("sys.if.out", (NumericTSDataType) ts);
        } else {
          series.put("sys.if.in", (NumericTSDataType) ts);
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
      public long[] timestamps() {
        if (timestamps == null) {
          build();
        }
        return timestamps;
      }

      @Override
      public long[] integers() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public double[] doubles() {
        if (doubles == null) {
          build();
        }
        return doubles;
      }

      @Override
      public boolean isIntegers() {
        return false;
      }

      @Override
      protected void reset() {
        // TODO Auto-generated method stub
        
      }

      void build() {
        NumericTSDataType sentinel = series.get("sys.if.out");
        timestamps = new long[sentinel.timestamps().length];
        doubles = new double[sentinel.timestamps().length];
        
        for (int i = 0; i < sentinel.timestamps().length; i++) {
          double sum = 0;
          for (NumericTSDataType s : series.values()) {
            sum += s.doubles()[i];
          }
          timestamps[i] = sentinel.timestamps()[i];
          doubles[i] = sum;
        }
      }
    }
  }
}
