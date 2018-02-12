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
package net.opentsdb.pipeline;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import avro.shaded.com.google.common.collect.Sets;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.StringType;
import net.opentsdb.pipeline.Implementations.*;
import net.opentsdb.pipeline.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Functions {

  /**
   * Groups time series by ID and only emits numeric data points when there is
   * an associated string type with the value "foo". For our dummy set, this should
   * cut out half the DPs.
   */
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
      for (TS<?> ts : time_series.values()) {
        ((FilterIterator) ts).reset();
      }
      
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
    
    class FilterIterator implements TS<NumericType>, Iterator<TimeSeriesValue<NumericType>> {
      TS<NumericType> number;
      TS<StringType> string;
      MutableNumericType dp;
      MutableNumericType last;
      boolean has_next = false;
      
      public void setTS(TS<?> ts) {
        if (dp == null) {
          dp = new MutableNumericType();
          last = new MutableNumericType();
        }
        
        if (ts.type() == NumericType.TYPE && number == null) {
          number = (TS<NumericType>) ts;
        } else if (string == null) {
          string = (TS<StringType>) ts;
        }
        
        // assuming they're in sync for this demo
        if (number != null && string != null) {
          advance();
        }
      }
      
      public void reset() {
        number = null;
        string = null;
      }
      
      void advance() {
        // advance
        has_next = false;
        if (string.iterator().hasNext()) {
          TimeSeriesValue<StringType> s = string.iterator().next();
          TimeSeriesValue<NumericType> n = number.iterator().next();
          while (s != null && !(s.value().values().get(0).equals("foo"))) {
            if (string.iterator().hasNext()) {
              s = string.iterator().next();
              n = number.iterator().next();
            } else {
              s = null;
              n = null;
            }
          }
          
          if (s != null) {
            dp.reset(n);
            has_next = true;
          }
        }
      }
      
      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        last.reset(dp);
        advance();
        return last;
      }

      @Override
      public TimeSeriesId id() {
        return number == null ? string.id() : number.id();
      }

      @Override
      public Iterator<TimeSeriesValue<NumericType>> iterator() {
        return this;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
    }
    
  }
  
  /**
   * Simple SUMming group by. No interpolation at this point, just assumes zero
   * if a series doesn't have a value at some point.
   */
  public static class GroupBy implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
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
      
      for (TS<?> ts : next.series()) {
        if (ts.type() != NumericType.TYPE) {
          continue;
        }
        if (hashes.contains(ts.hashCode())) {
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
        extant.addSource((TS<NumericType>) ts);
        hashes.add(ts.hashCode());
      }
      
      for (TS<?> it : time_series.values()) {
        ((GBIterator) it).reset();
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
      byte[] data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      int cache_idx = 0;
      
      boolean first_run = true;
      boolean has_next = false;
      long next_ts = Long.MAX_VALUE;
      MutableNumericType dp;
      
      public GBIterator(TimeSeriesId id) {
        this.id = id;
        sources = Lists.newArrayList();
        dp = new MutableNumericType();
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
        
        dp.reset(new MillisecondTimeStamp(next_ts), sum);
        if (cache) {
          System.arraycopy(Bytes.fromLong(dp.timestamp().msEpoch()), 0, data, cache_idx, 8);
          cache_idx += 8;
          System.arraycopy(Bytes.fromLong(dp.longValue()), 0, data, cache_idx, 8);
          cache_idx += 8;
          if (!has_next) {
            Map<TimeSeriesId, byte[]> c = parent.local_cache.get(parent.local_cache.size() - 1);
            c.put(id, Arrays.copyOf(data, cache_idx));
          }
        }
        next_ts = next_next_ts;

        return dp;
        } catch (Exception e){ 
          e.printStackTrace();
          throw new RuntimeException("WTF?", e);
        }
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
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
  public static class DiffFromStdD implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
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
      TimeSeriesId id;
      
      public SIt(final TimeSeriesId id) {
        this.id = id;
        dp = new MutableNumericType();
      }
      
      @Override
      public boolean hasNext() {
        return source == null ? false : source.iterator().hasNext();
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        TimeSeriesValue<NumericType> next = source.iterator().next();
        dp.reset(next.timestamp(), stdev - next.value().toDouble());
        return dp;
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
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
    }

    class FirstPassListener implements StreamListener {
      QExecutionPipeline downstream;
      
      @Override
      public void onComplete() {
        // setup the new iterators
        for (Entry<TimeSeriesId, Pair<Long, Double>> series : sums.entrySet()) {
          SIt it = new SIt(series.getKey());
          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
          // PURPOSELY not setting the source here.
          time_series.put(it.id(), it);
        }
        
        DiffFromStdD.this.fetchNext();
      }

      @Override
      public void onNext(QResult next) {
        for (TS<?> ts : next.series()) {
          if (ts.type() != NumericType.TYPE) {
            continue;
          }
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
  
  /**
   * A simple, naive expression processor that just sums up the time series.
   */
  public static class ExpressionProc implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TimeSeriesId, TS<?>> time_series = Maps.newHashMap();
    Set<Integer> hashes = Sets.newHashSet();
    
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
        if (ts.type() != NumericType.TYPE) {
          continue;
        }
        if (hashes.contains(ts.hashCode())) {
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
    
    class ExpressionIterator implements TS<NumericType>, Iterator<TimeSeriesValue<NumericType>> {
      Map<String, TS<?>> series = Maps.newHashMap();
      boolean has_next = false;
      MutableNumericType dp;
      TimeSeriesId id;
      
      public ExpressionIterator(TimeSeriesId id) {
        this.id = id;
        dp = new MutableNumericType();
      }
      
      public void addSeries(TS<?> ts) {
        if ("sys.if.out".equals(ts.id().metric())) {
          series.put("sys.if.out", ts);
        } else {
          series.put("sys.if.in", ts);
        }
        if (!has_next) {
          has_next = ts.iterator().hasNext();
        }
      }
      
      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        double sum = 0;
        long timestamp = Long.MAX_VALUE;
        has_next = false;
        // TODO - we'd actually bind variables properly here and count the reals.
        for (TS<?> ts : series.values()) {
          TimeSeriesValue<NumericType> v = ((TimeSeriesValue<NumericType>) ts.iterator().next());
          sum += v.value().toDouble();
          if (v.timestamp().msEpoch() < timestamp) {
            timestamp = v.timestamp().msEpoch();
          }
          if (!has_next) {
            has_next = ts.iterator().hasNext();
          }
        }
        dp.reset(new MillisecondTimeStamp(timestamp), sum);
        return dp;
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
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
    }
  }
}
