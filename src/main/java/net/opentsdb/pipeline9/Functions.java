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
package net.opentsdb.pipeline9;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline9.TimeSortedDataStore;
import net.opentsdb.pipeline9.Implementations.*;
import net.opentsdb.pipeline9.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Functions {

  public static class FilterNumsByString implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    QResult next;
    
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
    public Collection<TSByteId> series() {
      return next.series();
    }
    
    public Collection<TS<?>> getSeries(final TSByteId id) {
      FilterIterator it = new FilterIterator(
          (TS<NType>) next.getSeries(id, NType.TYPE),
          (TS<StringType>) next.getSeries(id, StringType.TYPE));
      return Lists.newArrayList(it);
    }
    
    public TS<?> getSeries(TSByteId id, TypeToken<?> type) {
      FilterIterator it = new FilterIterator(
          (TS<NType>) next.getSeries(id, NType.TYPE),
          (TS<StringType>) next.getSeries(id, StringType.TYPE));
      return it;
    }
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      this.next = next;
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class FilterIterator implements TS<NType> {
      TS<NType> number;
      TS<StringType> string;
      
      public FilterIterator(TS<NType> number, TS<StringType> string) {
        this.number = number;
        this.string = string;
      }
      
      class LocalIterator implements Iterator<TSValue<NType>>, TSValue<NType>, NType {
        Iterator<TSValue<NType>> nit;
        Iterator<TSValue<StringType>> sit;
        long current_value;
        TimeStamp current_ts = new MillisecondTimeStamp(0);
        long next_value;
        TimeStamp next_ts = new MillisecondTimeStamp(0);
        TimeStamp ts = new MillisecondTimeStamp(0);
        int ts_idx = 0;

        boolean has_next = false;
        
        LocalIterator() {
          nit = number.iterator();
          sit = string.iterator();
          next.timeSpec().updateTimestamp(ts_idx, ts);
          advance();
        }
        
        void advance() {
          // advance
          has_next = false;
          while (sit.hasNext()) {
            TSValue<StringType> s = sit.next();
            TSValue<NType> n = nit.next();
            if (s.value().values().get(0).equals("foo")) {
              next_value = n.value().longValue();
              next_ts.update(n.timestamp());
              has_next = true;
              break;
            }
          }
        }
        
        @Override
        public boolean hasNext() {
          return has_next;
        }

        @Override
        public TSValue<NType> next() {
          has_next = false;
          current_value = next_value;
          current_ts.update(next_ts);
          
          next.timeSpec().updateTimestamp(ts_idx++, ts);
          if (ts.compare(RelationalOperator.NE, current_ts)) {
            current_value = -1; // would be a nan or fill
            current_ts.update(ts);
          } else {
            advance();
          }
          if (ts.msEpoch() + next.timeSpec().interval() < next.timeSpec().end().msEpoch()) {
            has_next = true;
          }
          return this;
        }

        @Override
        public NumberType numberType() {
          return NumberType.DOUBLE;
        }

        @Override
        public long longValue() {
          return current_value;
        }

        @Override
        public double doubleValue() {
          return current_value;
        }

        @Override
        public long unsignedLongValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double toDouble() {
          return current_value;
        }

        @Override
        public TimeStamp timestamp() {
          return current_ts;
        }

        @Override
        public NType value() {
          return this;
        }
      }
      
      @Override
      public TSByteId id() {
        return number == null ? string.id() : number.id();
      }

      @Override
      public Iterator<TSValue<NType>> iterator() {
        return new LocalIterator();
      }

      @Override
      public TypeToken<NType> type() {
        return NType.TYPE;
      }

      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }
      
    }

    @Override
    public TimeSpec timeSpec() {
      return next.timeSpec();
    }
    
  }
  
  public static class GroupBy implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TSByteId, TS<?>> time_series = Maps.newHashMap();
    TimeSpec tspec;
    QResult next;
    
    GroupBy parent;
    boolean cache = false;
    List<Map<TSByteId, byte[]>> local_cache = Lists.newArrayList();
    List<TimeSpec> specs = Lists.newArrayList();
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
      this.next = next;
      tspec = next.timeSpec();
      try {
      if (cache) {
        parent.local_cache.add(Maps.newHashMap());
        parent.specs.add(next.timeSpec());
      }
      
      time_series.clear();
      for (TSByteId tsid : next.series()) {        
        // naive group by on the host tag.
        StringTSByteId id = new StringTSByteId();
        id.addMetric(tsid.metrics().get(0));
        id.addTag("host", tsid.tags().get("host"));
        id.addAggTag("dc");
        GBIterator extant = (GBIterator) time_series.get(id);
        if (extant == null) { 
          extant = new GBIterator(id);
          time_series.put(id, extant);
        }
        extant.addSource(tsid);
      }
      
      upstream.onNext(this);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }

    class GBIterator implements TS<NType> {
      TSByteId id;
      List<TSByteId> sources;
      
      byte[] data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      int cache_idx = 0;
            
      public GBIterator(TSByteId id) {
        this.id = id;
        sources = Lists.newArrayList();
      }
      
      public void reset() {
//        has_next = false;
//        for (final TS<NType> source : sources) {
//          if (source.iterator().hasNext()) {
//            has_next = true;
//            break;
//          }
//        }
//        first_run = true;
//        next_ts = Long.MAX_VALUE;
        cache_idx = 0;
        data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
      }
      
      public void addSource(TSByteId source) {
        sources.add(source);
      }
      
      @Override
      public TSByteId id() {
        return id;
      }

      @Override
      public Iterator<TSValue<NType>> iterator() {
        return new LocalIterator();
      }
      
      class LocalIterator implements Iterator<TSValue<NType>>, TSValue<NType>, NType {
        boolean has_next = false;
        long next_ts = Long.MAX_VALUE;
        TimeStamp ts = new MillisecondTimeStamp(0);
        long sum = 0;
        int time_idx;
        
        Iterator<TSValue<NType>>[] iterators;
        TSValue<NType>[] values;
        
        public LocalIterator() {
          iterators = new Iterator[sources.size()];
          values = new TSValue[sources.size()];
          for (int i = 0; i < iterators.length; i++) {
            iterators[i] = ((TS<NType>) GroupBy.this.next.getSeries(sources.get(i), NType.TYPE)).iterator();
            if (iterators[i].hasNext()) {
              values[i] = iterators[i].next();
              if (values[i].timestamp().msEpoch() < next_ts) {
                next_ts = values[i].timestamp().msEpoch();
              }
              has_next = true;
            }
          }
        }
        
        @Override
        public boolean hasNext() {
          return has_next;
        }

        @Override
        public TSValue<NType> next() {
          has_next = false;
          try {
          
          long next_next_ts = Long.MAX_VALUE;
          sum = 0;
          for (int i = 0; i < sources.size(); i++) {
            if (values[i] == null) {
              // TODO - fill
              continue;
            }
            if (values[i].timestamp().msEpoch() == next_ts) {
              sum += values[i].value().longValue();
              if (iterators[i].hasNext()) {
                values[i] = iterators[i].next();
                if (values[i].timestamp().msEpoch() < next_next_ts) {
                  next_next_ts = values[i].timestamp().msEpoch();
                }
                has_next = true;
              } else {
                values[i] = null;
              }
            } else {
              if (values[i].timestamp().msEpoch() > next_next_ts) {
                next_next_ts = values[i].timestamp().msEpoch();
                has_next = true;
              }
            }
          }
          
          //tspec.updateTimestamp(time_idx++, ts);
          ts.updateMsEpoch(next_ts);
          if (cache) {
            System.arraycopy(Bytes.fromLong(sum), 0, data, cache_idx, 8);
            cache_idx += 8;
            if (!has_next) {
              Map<TSByteId, byte[]> c = parent.local_cache.get(parent.local_cache.size() - 1);
              c.put(id, Arrays.copyOf(data, cache_idx));
            }
          }
          next_ts = next_next_ts;

          return this;
          } catch (Exception e){ 
            e.printStackTrace();
            throw new RuntimeException("WTF?", e);
          }
        }

        @Override
        public NumberType numberType() {
          return NumberType.INTEGER;
        }

        @Override
        public long longValue() {
          return sum;
        }

        @Override
        public double doubleValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public long unsignedLongValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double toDouble() {
          return (double) sum;
        }

        @Override
        public TimeStamp timestamp() {
          return ts;
        }

        @Override
        public NType value() {
          return this;
        }
      }
      
      @Override
      public TypeToken<NType> type() {
        return NType.TYPE;
      }

      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }
      
    }

    @Override
    public Collection<TSByteId> series() {
      return time_series.keySet();
    }
    
    @Override
    public Collection<TS<?>> getSeries(final TSByteId id) {
      return Lists.newArrayList(time_series.get(id));
    }
    
    @Override
    public TS<?> getSeries(TSByteId id, TypeToken<?> type) {
      return time_series.get(id);
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
        Map<TSByteId, byte[]> chunk = local_cache.get(cache_idx);
        TimeSpec spec = specs.get(cache_idx++);
        for (Entry<TSByteId, byte[]> entry : chunk.entrySet()) {
          ArrayBackedLongTS extant = new ArrayBackedLongTS(entry.getKey(), spec);
          time_series.put(entry.getKey(), extant);
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

    @Override
    public TimeSpec timeSpec() {
      return tspec;
    }
  }

  public static class DiffFromStdD implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    //Map<TSByteId, TS<?>> time_series = Maps.newHashMap();
    Map<TSByteId, Pair<Long, Double>> sums = Maps.newHashMap();
    TimeSpec tspec;
    QResult next;
    
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
    public Collection<TSByteId> series() {
      return sums.keySet();
    }

    @Override
    public Collection<TS<?>> getSeries(final TSByteId id) {
      Pair<Long, Double> sum = sums.get(id);
      if (sum == null) {
        return null;
      }
      
      TS<NType> ts = new SIt(id, Math.sqrt((sum.getValue() / (double)sum.getKey())));
      return Lists.newArrayList(ts);
    }
    
    @Override
    public TS<?> getSeries(final TSByteId id, TypeToken<?> type) {
      Pair<Long, Double> sum = sums.get(id);
      if (sum == null) {
        return null;
      }
      
      return new SIt(id, Math.sqrt((sum.getValue() / (double)sum.getKey())));
    }
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      this.next = next;
      tspec = next.timeSpec();
//      for (TS<?> ts : next.series()) {
//        if (ts.type() != NType.TYPE) {
//          continue;
//        }
//        SIt it = (SIt) time_series.get(ts.id());
//        it.source = (TS<NType>) ts;
//      }

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
    
    class SIt implements TS<NType> {

      TS<NType> source;
      double stdev;
      TSByteId id;
      
      public SIt(TSByteId id, double stdev) {
        this.id = id;
        this.stdev = stdev;
        source = (TS<NType>) next.getSeries(id, NType.TYPE);
      }
      
      class LocalIterator implements Iterator<TSValue<NType>>, TSValue<NType>, NType {
      
        Iterator<TSValue<NType>> iterator = source.iterator();
        TSValue<NType> dp;
        
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }
  
        @Override
        public TSValue<NType> next() {
          dp = iterator.next();
          return this;
        }

        @Override
        public NumberType numberType() {
          return NumberType.DOUBLE;
        }

        @Override
        public long longValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double doubleValue() {
          return stdev - dp.value().toDouble();
        }

        @Override
        public long unsignedLongValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double toDouble() {
          return stdev - dp.value().toDouble();
        }

        @Override
        public TimeStamp timestamp() {
          return dp.timestamp();
        }

        @Override
        public NType value() {
          return this;
        }

      }
      
      @Override
      public TSByteId id() {
        return id;
      }

      @Override
      public Iterator<TSValue<NType>> iterator() {
        return new LocalIterator();
      }

      @Override
      public TypeToken<NType> type() {
        return NType.TYPE;
      }

      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }
      
    }

    class FirstPassListener implements StreamListener {
      QExecutionPipeline downstream;
      
      @Override
      public void onComplete() {
        // setup the new iterators
//        for (Entry<TSByteId, Pair<Long, Double>> series : sums.entrySet()) {
//          SIt it = new SIt(series.getKey());
//          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
//          // PURPOSELY not setting the source here.
//          time_series.put(it.id(), it);
//        }
        
        DiffFromStdD.this.fetchNext();
      }

      @Override
      public void onNext(QResult next) {
        for (TSByteId tsid : next.series()) {
          TS<NType> ts = (TS<NType>) next.getSeries(tsid, NType.TYPE);
          Pair<Long, Double> pair = sums.get(ts.id());
          double sum_of_squares = pair == null ? 0 : pair.getValue();
          long count = pair == null ? 0 : pair.getKey();
          
          Iterator<?> it = ts.iterator();
          while(it.hasNext()) {
            TSValue<NType> v = (TSValue<NType>) it.next();
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

    @Override
    public TimeSpec timeSpec() {
      return tspec;
    }
  }

  public static class ExpressionProc implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
    StreamListener upstream;
    QExecutionPipeline downstream;
    Map<TSByteId, TS<?>> time_series = Maps.newHashMap();
    TimeSpec tspec;
    
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
    public Collection<TSByteId> series() {
      return time_series.keySet();
    }
    
    @Override
    public Collection<TS<?>> getSeries(final TSByteId id) {
      return Lists.newArrayList(time_series.get(id));
    }
    
    @Override
    public TS<?> getSeries(TSByteId id, TypeToken<?> type) {
      return time_series.get(id);
    }
    
    @Override
    public void onComplete() {
      upstream.onComplete();
    }

    @Override
    public void onNext(QResult next) {
      tspec = next.timeSpec();
      time_series.clear();
      for (TSByteId tsid : next.series()) {
        TS<NType> ts = (TS<NType>) next.getSeries(tsid, NType.TYPE);
        
        StringTSByteId builder = new StringTSByteId();
        builder.addMetric("Sum of if in and out");
        for (Entry<String, String> pair : ts.id().tags().entrySet()) {
          builder.addTag(pair.getKey(), pair.getValue());
        }
        for (String tag : ts.id().aggregatedTags()) {
          builder.addAggTag(tag);
        }
        
        TS<?> it = time_series.get(builder);
        if (it == null) {
          it = new ExpressionIterator(builder);
          time_series.put(builder, it);
        }
        ((ExpressionIterator) it).addSeries(ts);
      }
      upstream.onNext(this);
    }

    @Override
    public void onError(Throwable t) {
      upstream.onError(t);
    }
    
    class ExpressionIterator implements TS<NType> {
      Map<String, TS<?>> series = Maps.newHashMap();
      TSByteId id;
      
      public ExpressionIterator(TSByteId id) {
        this.id = id;
      }
      
      public void addSeries(TS<?> ts) {
        if (ts.id().metrics().get(0).equals("sys.if.out")) {
          series.put("sys.if.out", ts);
        } else {
          series.put("sys.if.in", ts);
        }
      }
      
      class LocalIterator implements Iterator<TSValue<NType>>, TSValue<NType>, NType {
        boolean has_next;
        Iterator<TSValue<NType>>[] iterators;
        double value;
        TimeStamp ts;
        
        public LocalIterator() {
          int i = 0;
          iterators = new Iterator[series.size()];
          for (TS<?> ts : series.values()) {
            iterators[i] = ((TS<NType>) ts).iterator();
            if (iterators[i].hasNext()) {
              has_next = true;
            }
            i++;
          }
        }
        
        @Override
        public boolean hasNext() {
          return has_next;
        }
  
        @Override
        public TSValue<NType> next() {
          double sum = 0;
          long timestamp = Long.MAX_VALUE;
          has_next = false;
          // TODO - we'd actually bind variables properly here and count the reals.
          for (Iterator<TSValue<NType>> it : iterators) {
            TSValue<NType> v = it.next();
            sum += v.value().toDouble();
            ts = v.timestamp();
            if (v.timestamp().msEpoch() < timestamp) {
              timestamp = v.timestamp().msEpoch();
            }
            if (!has_next) {
              has_next = it.hasNext();
            }
          }
          value = sum;
          return this;
        }

        @Override
        public NumberType numberType() {
          return NumberType.DOUBLE;
        }

        @Override
        public long longValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double doubleValue() {
          return value;
        }

        @Override
        public long unsignedLongValue() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public double toDouble() {
          return value;
        }

        @Override
        public TimeStamp timestamp() {
          return ts;
        }

        @Override
        public NType value() {
          return this;
        }

      }
      
      @Override
      public TSByteId id() {
        return id;
      }

      @Override
      public Iterator<TSValue<NType>> iterator() {
        return new LocalIterator();
      }

      @Override
      public TypeToken<NType> type() {
        return NType.TYPE;
      }

      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }
      
    }

    @Override
    public TimeSpec timeSpec() {
      return tspec;
    }
  }

}
