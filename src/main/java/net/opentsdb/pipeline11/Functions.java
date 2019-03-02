// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.pipeline11;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

import net.opentsdb.pipeline11.Implementations.*;
import net.opentsdb.pipeline11.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Functions {

//  public static class FilterNumsByString implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
//    StreamListener upstream;
//    QExecutionPipeline downstream;
//    Map<TSByteId, TS> time_series = Maps.newHashMap();
//    
//    protected FilterNumsByString() { }
//    
//    public FilterNumsByString(QExecutionPipeline downstream_execution) {
//      this.upstream = downstream_execution.getListener();
//      this.downstream = downstream_execution;
//      downstream_execution.setListener(this);
//    }
//    
//    @Override
//    public void setListener(StreamListener listener) {
//      upstream = listener;
//    }
//
//    @Override
//    public StreamListener getListener() {
//      return upstream;
//    }
//
//    @Override
//    public void fetchNext() {
//      downstream.fetchNext();
//    }
//
//    @Override
//    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
//      FilterNumsByString clone = new FilterNumsByString();
//      clone.downstream = downstream.getMultiPassClone(clone);
//      clone.upstream = listener;
//      return clone;
//    }
//
//    @Override
//    public void setCache(boolean cache) {
//      // TODO Auto-generated method stub
//      
//    }
//
//    @Override
//    public QueryMode getMode() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public Collection<TS> series() {
//      return time_series.values();
//    }
//    
//    @Override
//    public void onComplete() {
//      upstream.onComplete();
//    }
//
//    @Override
//    public void onNext(QResult next) {
//      for (TS ts : next.series()) {
//        TS it = time_series.get(ts.id());
//        if (it == null) {
//          it = new FilterIterator();
//          time_series.put(ts.id(), it);
//        }
//        ((FilterIterator) it).setTS(ts);
//      }
//      upstream.onNext(this);
//    }
//
//    @Override
//    public void onError(Throwable t) {
//      upstream.onError(t);
//    }
//    
//    class FilterIterator implements TS {
//      TS source;
//      
//      public void setTS(TS ts) {
//        source = ts;
//      }
//      
//      class LocalIterator implements Iterator<TSValue<?>>, TSValue<NType>, NType {
//        Iterator<TSValue<?>> nit;
//        Iterator<TSValue<?>> sit;
//        long current_value;
//        TimeStamp current_ts = new MillisecondTimeStamp(0);
//        long next_value;
//        TimeStamp next_ts = new MillisecondTimeStamp(0);
//
//        boolean has_next = false;
//        
//        LocalIterator() {
//          nit = source.iterator(NType.TYPE);
//          sit = source.iterator(StringType.TYPE);
//          advance();
//        }
//        
//        void advance() {
//          // advance
//          has_next = false;
//          while (sit.hasNext()) {
//            TSValue<StringType> s = (TSValue<StringType>) sit.next();
//            TSValue<NType> n = (TSValue<NType>) nit.next();
//            if (s.value().values().get(0).equals("foo")) {
//              next_value = n.value().longValue();
//              next_ts.update(n.timestamp());
//              has_next = true;
//              break;
//            }
//          }
//        }
//        
//        @Override
//        public boolean hasNext() {
//          return has_next;
//        }
//
//        @Override
//        public TSValue<NType> next() {
//          current_value = next_value;
//          current_ts.update(next_ts);
//          advance();
//          return this;
//        }
//
//        @Override
//        public NumberType numberType() {
//          return NumberType.DOUBLE;
//        }
//
//        @Override
//        public long longValue() {
//          return current_value;
//        }
//
//        @Override
//        public double doubleValue() {
//          return current_value;
//        }
//
//        @Override
//        public long unsignedLongValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double toDouble() {
//          return current_value;
//        }
//
//        @Override
//        public TimeStamp timestamp() {
//          return current_ts;
//        }
//
//        @Override
//        public NType value() {
//          return this;
//        }
//      }
//      
//      @Override
//      public TSByteId id() {
//        return source.id();
//      }
//
//      @Override
//      public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
//        return new LocalIterator();
//      }
//
//      @Override
//      public void close() {
//        // TODO Auto-generated method stub
//        
//      }
//
//      @Override
//      public Collection<TypeToken<?>> types() {
//        return Lists.newArrayList(NType.TYPE);
//      }
//      
//    }
//
//    @Override
//    public TimeSpec timeSpec() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//    
//  }
//  
//  public static class GroupBy implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
//    StreamListener upstream;
//    QExecutionPipeline downstream;
//    Map<TSByteId, TS> time_series = Maps.newHashMap();
//    
//    GroupBy parent;
//    boolean cache = false;
//    List<Map<TSByteId, byte[]>> local_cache = Lists.newArrayList();
//    int cache_idx = 0;
//    
//    protected GroupBy() { }
//    
//    public GroupBy(QExecutionPipeline downstream_execution) {
//      this.upstream = downstream_execution.getListener();
//      this.downstream = downstream_execution;
//      downstream_execution.setListener(this);
//    }
//    
//    @Override
//    public void onComplete() {
//      upstream.onComplete();
//    }
//
//    @Override
//    public void onNext(QResult next) {
//      try {
//      if (cache) {
//        parent.local_cache.add(Maps.newHashMap());
//      }
//      
//      time_series.clear();
//      for (TS ts : next.series()) {       
//        // naive group by on the host tag.
//        StringTSByteId id = new StringTSByteId();
//        id.addMetric(ts.id().metrics().get(0));
//        id.addTag("host", ts.id().tags().get("host"));
//        id.addAggTag("dc");
//        GBIterator extant = (GBIterator) time_series.get(id);
//        if (extant == null) {
//          extant = new GBIterator(id);
//          time_series.put(id, extant);
//        }
//        extant.addSource(ts);
//      }
//      
//      upstream.onNext(this);
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//    }
//
//    @Override
//    public void onError(Throwable t) {
//      upstream.onError(t);
//    }
//
//    class GBIterator implements TS {
//      TSByteId id;
//      List<TS> sources;
//      
//      byte[] data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
//      int cache_idx = 0;
//            
//      public GBIterator(TSByteId id) {
//        this.id = id;
//        sources = Lists.newArrayList();
//      }
//      
//      public void reset() {
////        has_next = false;
////        for (final TS<NType> source : sources) {
////          if (source.iterator().hasNext()) {
////            has_next = true;
////            break;
////          }
////        }
////        first_run = true;
////        next_ts = Long.MAX_VALUE;
//        cache_idx = 0;
//        data = cache ? new byte[TimeSortedDataStore.INTERVALS_PER_CHUNK * 16] : null;
//      }
//      
//      public void addSource(TS source) {
//        sources.add(source);
//      }
//      
//      @Override
//      public TSByteId id() {
//        return id;
//      }
//
//      @Override
//      public Iterator<TSValue<? extends TimeSeriesDataType>> iterator(TypeToken<?> type) {
//        return new LocalIterator();
//      }
//      
//      class LocalIterator implements Iterator<TSValue<?>>, TSValue<NType>, NType {
//        boolean has_next = false;
//        long next_ts = Long.MAX_VALUE;
//        TimeStamp ts = new MillisecondTimeStamp(0);
//        long sum = 0;
//        
//        Iterator<TSValue<?>>[] iterators;
//        TSValue<NType>[] values;
//        
//        public LocalIterator() {
//          iterators = new Iterator[sources.size()];
//          values = new TSValue[sources.size()];
//          for (int i = 0; i < iterators.length; i++) {
//            iterators[i] = sources.get(i).iterator(NType.TYPE);
//            if (iterators[i].hasNext()) {
//              values[i] = (TSValue<NType>) iterators[i].next();
//              if (values[i].timestamp().msEpoch() < next_ts) {
//                next_ts = values[i].timestamp().msEpoch();
//              }
//              has_next = true;
//            }
//          }
//        }
//        
//        @Override
//        public boolean hasNext() {
//          return has_next;
//        }
//
//        @Override
//        public TSValue<NType> next() {
//          has_next = false;
//          try {
//          
//          long next_next_ts = Long.MAX_VALUE;
//          sum = 0;
//          for (int i = 0; i < sources.size(); i++) {
//            if (values[i] == null) {
//              // TODO - fill
//              continue;
//            }
//            if (values[i].timestamp().msEpoch() == next_ts) {
//              sum += values[i].value().longValue();
//              if (iterators[i].hasNext()) {
//                values[i] = (TSValue<NType>) iterators[i].next();
//                if (values[i].timestamp().msEpoch() < next_next_ts) {
//                  next_next_ts = values[i].timestamp().msEpoch();
//                }
//                has_next = true;
//              } else {
//                values[i] = null;
//              }
//            } else {
//              if (values[i].timestamp().msEpoch() > next_next_ts) {
//                next_next_ts = values[i].timestamp().msEpoch();
//                has_next = true;
//              }
//            }
//          }
//          
//          ts.updateMsEpoch(next_ts);
//          if (cache) {
//            System.arraycopy(Bytes.fromLong(next_ts), 0, data, cache_idx, 8);
//            cache_idx += 8;
//            System.arraycopy(Bytes.fromLong(sum), 0, data, cache_idx, 8);
//            cache_idx += 8;
//            if (!has_next) {
//              Map<TSByteId, byte[]> c = parent.local_cache.get(parent.local_cache.size() - 1);
//              c.put(id, Arrays.copyOf(data, cache_idx));
//            }
//          }
//          next_ts = next_next_ts;
//
//          return this;
//          } catch (Exception e){ 
//            e.printStackTrace();
//            throw new RuntimeException("WTF?", e);
//          }
//        }
//
//        @Override
//        public NumberType numberType() {
//          return NumberType.INTEGER;
//        }
//
//        @Override
//        public long longValue() {
//          return sum;
//        }
//
//        @Override
//        public double doubleValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public long unsignedLongValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double toDouble() {
//          return (double) sum;
//        }
//
//        @Override
//        public TimeStamp timestamp() {
//          return ts;
//        }
//
//        @Override
//        public NType value() {
//          return this;
//        }
//      }
//      
//      public Collection<TypeToken<?>> types() {
//        return Lists.newArrayList(NType.TYPE);
//      }
//      
//      @Override
//      public void close() {
//        // TODO Auto-generated method stub
//        
//      }
//      
//    }
//
//    @Override
//    public Collection<TS> series() {
//      return time_series.values();
//    }
//
//    @Override
//    public void setListener(StreamListener listener) {
//      upstream = listener;
//    }
//    
//    @Override
//    public void fetchNext() {
//      if (local_cache.size() > 0) {
//        if (cache_idx >= local_cache.size()) {
//          upstream.onComplete();
//          return;
//        }
//        
//        // work from cache.
//        // TODO - fall through in case the cache has been exhausted. That'll get ugly.
//        Map<TSByteId, byte[]> chunk = local_cache.get(cache_idx++);
//        for (Entry<TSByteId, byte[]> entry : chunk.entrySet()) {
//          ArrayBackedLongTS extant = (ArrayBackedLongTS) time_series.get(entry.getKey());
//          if (extant == null) {
//            extant = new ArrayBackedLongTS(entry.getKey(), null);
//            time_series.put(entry.getKey(), extant);
//          }
//          extant.nextChunk(entry.getValue());
//        }
//        upstream.onNext(this);
//      } else {
//        downstream.fetchNext();
//      }
//    }
//    
//    @Override
//    public StreamListener getListener() {
//      return upstream;
//    }
//
//    @Override
//    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
//      GroupBy clone = new GroupBy();
//      clone.downstream = downstream.getMultiPassClone(clone);
//      clone.parent = this;
//      clone.cache = true;
//      clone.upstream = listener;
//      return clone;
//    }
//
//    @Override
//    public void setCache(boolean cache) {
//      this.cache = cache;
//    }
//    
//    @Override
//    public TimeSpec timeSpec() {
//      return null;
//    }
//    
//    @Override
//    public QueryMode getMode() {
//      return downstream.getMode();
//    }
//  }
//
//  public static class DiffFromStdD implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
//    StreamListener upstream;
//    QExecutionPipeline downstream;
//    Map<TSByteId, TS> time_series = Maps.newHashMap();
//    Map<TSByteId, Pair<Long, Double>> sums = Maps.newHashMap();
//    
//    boolean initialized = false;
//    
//    public DiffFromStdD(QExecutionPipeline downstream_execution) {
//      this.upstream = downstream_execution.getListener();
//      this.downstream = downstream_execution;
//      downstream_execution.setListener(this);
//    }
//    
//    @Override
//    public void setListener(StreamListener listener) {
//      upstream = listener;
//    }
//
//    @Override
//    public StreamListener getListener() {
//      return upstream;
//    }
//    
//    @Override
//    public void fetchNext() {
//      if (initialized) {
//        downstream.fetchNext();
//      } else {
//        FirstPassListener fpl = new FirstPassListener();
//        QExecutionPipeline fp = downstream.getMultiPassClone(fpl);
//        fpl.downstream = fp;
//        initialized = true;
//        fp.fetchNext();
//      }
//    }
//
//    @Override
//    public Collection<TS> series() {
//      return time_series.values();
//    }
//
//    @Override
//    public void onComplete() {
//      upstream.onComplete();
//    }
//
//    @Override
//    public void onNext(QResult next) {
//      for (TS ts : next.series()) {
//        TS extant = time_series.get(ts.id());
//        if (extant == null) {
//          extant = new SIt(ts);
//          time_series.put(ts.id(), extant);
//          Pair<Long, Double> deets = sums.get(ts.id());
//          ((SIt) extant).stdev = Math.sqrt((deets.getValue() / (double) deets.getKey()));
//        } else {
//          ((SIt) extant).source = ts;
//        }
//      }
//      upstream.onNext(this);
//    }
//
//    @Override
//    public void onError(Throwable t) {
//      upstream.onError(t);
//    }
//
//    @Override
//    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//    
//    public void setCache(boolean cache) {
//      // TODO Auto-generated method stub
//    }
//    
//    class SIt implements TS {
//      TS source;
//      double stdev;
//      
//      public SIt(TS source) {
//        this.source = source;
//      }
//      
//      class LocalIterator implements Iterator<TSValue<?>>, TSValue<NType>, NType {
//      
//        Iterator<TSValue<?>> iterator = source.iterator(NType.TYPE);
//        TSValue<NType> dp;
//        
//        @Override
//        public boolean hasNext() {
//          return iterator.hasNext();
//        }
//  
//        @Override
//        public TSValue<NType> next() {
//          dp = (TSValue<NType>) iterator.next();
//          return this;
//        }
//
//        @Override
//        public NumberType numberType() {
//          return NumberType.DOUBLE;
//        }
//
//        @Override
//        public long longValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double doubleValue() {
//          return stdev - dp.value().toDouble();
//        }
//
//        @Override
//        public long unsignedLongValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double toDouble() {
//          return stdev - dp.value().toDouble();
//        }
//
//        @Override
//        public TimeStamp timestamp() {
//          return dp.timestamp();
//        }
//
//        @Override
//        public NType value() {
//          return this;
//        }
//
//      }
//      
//      @Override
//      public TSByteId id() {
//        return source.id();
//      }
//
//      @Override
//      public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
//        return new LocalIterator();
//      }
//
//      @Override
//      public void close() {
//        // TODO Auto-generated method stub
//        
//      }
//
//      @Override
//      public Collection<TypeToken<?>> types() {
//        return Lists.newArrayList(NType.TYPE);
//      }
//      
//    }
//
//    class FirstPassListener implements StreamListener {
//      QExecutionPipeline downstream;
//      
//      @Override
//      public void onComplete() {
//        // setup the new iterators
////        for (Entry<TSByteId, Pair<Long, Double>> series : sums.entrySet()) {
////          SIt it = new SIt(series.getKey());
////          it.stdev = Math.sqrt((series.getValue().getValue() / (double)series.getValue().getKey()));
////          // PURPOSELY not setting the source here.
////          time_series.put(it.id(), it);
////        }
//        
//        DiffFromStdD.this.fetchNext();
//      }
//
//      @Override
//      public void onNext(QResult next) {
//        for (TS ts : next.series()) {
//          
//          Pair<Long, Double> pair = sums.get(ts.id());
//          double sum_of_squares = pair == null ? 0 : pair.getValue();
//          long count = pair == null ? 0 : pair.getKey();
//          
//          Iterator<?> it = ts.iterator(NType.TYPE);
//          while(it.hasNext()) {
//            TSValue<NType> v = (TSValue<NType>) it.next();
//            sum_of_squares += Math.pow(v.value().toDouble(), 2);
//            count++;
//          }
//          sums.put(ts.id(), new Pair<Long, Double>(count, sum_of_squares));
//        }
//        downstream.fetchNext();
//      }
//
//      @Override
//      public void onError(Throwable t) {
//        DiffFromStdD.this.onError(t);
//      }
//      
//    }
//
//    @Override
//    public QueryMode getMode() {
//      return downstream.getMode();
//    }
//
//    @Override
//    public TimeSpec timeSpec() {
//      return null;
//    }
//  }
//
//  public static class ExpressionProc implements TSProcessor, StreamListener, QResult, QExecutionPipeline {
//    StreamListener upstream;
//    QExecutionPipeline downstream;
//    Map<TSByteId, TS> time_series = Maps.newHashMap();
//    
//    public ExpressionProc(QExecutionPipeline downstream_execution) {
//      this.upstream = downstream_execution.getListener();
//      this.downstream = downstream_execution;
//      downstream_execution.setListener(this);
//    }
//    
//    @Override
//    public void setListener(StreamListener listener) {
//      upstream = listener;      
//    }
//
//    @Override
//    public StreamListener getListener() {
//      return upstream;
//    }
//
//    @Override
//    public void fetchNext() {
//      downstream.fetchNext();
//    }
//
//    @Override
//    public QExecutionPipeline getMultiPassClone(StreamListener listener) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public void setCache(boolean cache) {
//      // TODO Auto-generated method stub
//      
//    }
//
//    @Override
//    public QueryMode getMode() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public Collection<TS> series() {
//      return time_series.values();
//    }
//    
//    @Override
//    public void onComplete() {
//      upstream.onComplete();
//    }
//
//    @Override
//    public void onNext(QResult next) {
//      time_series.clear();
//      for (TS ts : next.series()) {
//        
//        StringTSByteId builder = new StringTSByteId();
//        builder.addMetric("Sum of if in and out");
//        for (Entry<String, String> pair : ts.id().tags().entrySet()) {
//          builder.addTag(pair.getKey(), pair.getValue());
//        }
//        for (String tag : ts.id().aggregatedTags()) {
//          builder.addAggTag(tag);
//        }
//        
//        TS it = time_series.get(builder);
//        if (it == null) {
//          it = new ExpressionIterator(builder);
//          time_series.put(builder, it);
//        }
//        ((ExpressionIterator) it).addSeries(ts);
//      }
//      upstream.onNext(this);
//    }
//
//    @Override
//    public void onError(Throwable t) {
//      upstream.onError(t);
//    }
//    
//    class ExpressionIterator implements TS {
//      Map<String, TS> series = Maps.newHashMap();
//      TSByteId id;
//      
//      public ExpressionIterator(TSByteId id) {
//        this.id = id;
//      }
//      
//      public void addSeries(TS ts) {
//        if (ts.id().metrics().get(0).equals("sys.if.out")) {
//          series.put("sys.if.out", ts);
//        } else {
//          series.put("sys.if.in", ts);
//        }
//      }
//      
//      class LocalIterator implements Iterator<TSValue<?>>, TSValue<NType>, NType {
//        boolean has_next;
//        Iterator<TSValue<?>>[] iterators;
//        double value;
//        TimeStamp ts;
//        
//        public LocalIterator() {
//          int i = 0;
//          iterators = new Iterator[series.size()];
//          for (TS ts : series.values()) {
//            iterators[i] = ts.iterator(NType.TYPE);
//            if (iterators[i].hasNext()) {
//              has_next = true;
//            }
//            i++;
//          }
//        }
//        
//        @Override
//        public boolean hasNext() {
//          return has_next;
//        }
//  
//        @Override
//        public TSValue<NType> next() {
//          double sum = 0;
//          long timestamp = Long.MAX_VALUE;
//          has_next = false;
//          // TODO - we'd actually bind variables properly here and count the reals.
//          for (Iterator<TSValue<?>> it : iterators) {
//            TSValue<NType> v = (TSValue<NType>) it.next();
//            sum += v.value().toDouble();
//            ts = v.timestamp();
//            if (v.timestamp().msEpoch() < timestamp) {
//              timestamp = v.timestamp().msEpoch();
//            }
//            if (!has_next) {
//              has_next = it.hasNext();
//            }
//          }
//          value = sum;
//          return this;
//        }
//
//        @Override
//        public NumberType numberType() {
//          return NumberType.DOUBLE;
//        }
//
//        @Override
//        public long longValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double doubleValue() {
//          return value;
//        }
//
//        @Override
//        public long unsignedLongValue() {
//          // TODO Auto-generated method stub
//          return 0;
//        }
//
//        @Override
//        public double toDouble() {
//          return value;
//        }
//
//        @Override
//        public TimeStamp timestamp() {
//          return ts;
//        }
//
//        @Override
//        public NType value() {
//          return this;
//        }
//
//      }
//      
//      @Override
//      public TSByteId id() {
//        return id;
//      }
//
//      @Override
//      public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
//        return new LocalIterator();
//      }
//
//      @Override
//      public void close() {
//        // TODO Auto-generated method stub
//        
//      }
//
//      @Override
//      public Collection<TypeToken<?>> types() {
//        return Lists.newArrayList(NType.TYPE);
//      }
//      
//    }
//
//    @Override
//    public TimeSpec timeSpec() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//  }

}
