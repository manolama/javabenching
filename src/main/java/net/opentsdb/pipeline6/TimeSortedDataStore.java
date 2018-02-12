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
package net.opentsdb.pipeline6;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.google.common.collect.Lists;

import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.common.Const;
import net.opentsdb.pipeline6.Abstracts.*;
import net.opentsdb.pipeline6.Implementations.*;
import net.opentsdb.pipeline6.Interfaces.*;
import net.opentsdb.utils.Bytes;

/**
 * And example data source. 
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

  public enum DataType {
    NUMBERS,
    STRINGS
  }
  
  public ExecutorService pool = Executors.newFixedThreadPool(1);
  public List<TSByteId> timeseries;
  public long start_ts = 0; // in ms
  public boolean with_strings;
  
  public TimeSortedDataStore(boolean with_strings) {
    this.with_strings = with_strings;
    timeseries = Lists.newArrayList();
    
    for (final String metric : METRICS) {
      for (final String dc : DATACENTERS) {
        for (int h = 0; h < HOSTS; h++) {
          StringTSByteId id = new StringTSByteId();
          id.addMetric(metric);
          id.addTag("dc", dc);
          id.addTag("host", String.format("web%02d", h + 1));
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
    
    Map<TSByteId, TS<?>> num_map = Maps.newHashMap();
    Map<TSByteId, TS<?>> string_map = Maps.newHashMap();
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
      
      Map<TSByteId, byte[]> nums = getChunk(DataType.NUMBERS, ts, reverse_chunks);
      for (Entry<TSByteId, byte[]> entry : nums.entrySet()) {
        TS<?> t = num_map.get(entry.getKey());
        if (t == null) {
          t = new ArrayBackedLongTS(entry.getKey());
          num_map.put(entry.getKey(), t);
        }
        ((BaseTS<?>) t).nextChunk(entry.getValue());
      }
      
      if (with_strings) {
        Map<TSByteId, byte[]> strings = getChunk(DataType.STRINGS, ts, reverse_chunks);
        for (Entry<TSByteId, byte[]> entry : strings.entrySet()) {
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

    Map<TSByteId, TS<?>> num_map;
    Map<TSByteId, TS<?>> string_map;
    
    public Results(Map<TSByteId, TS<?>> num_map, Map<TSByteId, TS<?>> string_map) {
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
  
  /**
   * Generates an "encoded" chunk of data as if we fetched a compressed set of
   * info from storage.
   * @param type The type of data to return.
   * @param ts The timestamp to start from.
   * @param reverse Whether or not we're walking down or up.
   * @return An "encoded" byte array.
   */
  public Map<TSByteId, byte[]> getChunk(DataType type, long ts, boolean reverse) {
    switch (type) {
    case NUMBERS:
      return getNumbersChunk(ts, reverse);
    case STRINGS:
      return getStringsChunk(ts, reverse);
      default:
        throw new RuntimeException("WTF?");
    }
  }
  
  Map<TSByteId, byte[]> getNumbersChunk(long ts, boolean reverse) {
    Map<TSByteId, byte[]> results = Maps.newHashMap();
    for (int x = 0; x < timeseries.size(); x++) {
      if (Bytes.memcmp("sys.if.out".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0 && 
          Bytes.memcmp("sys.if.in".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0) {
        continue;
      }
      
      byte[] data = new byte[INTERVALS_PER_CHUNK * 16];
      long local_ts = ts;
      int idx = reverse ? data.length - 8 : 0;
      
      if (reverse) {
        for (int i = INTERVALS_PER_CHUNK - 1; i >= 0; i--) {
          //System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
          System.arraycopy(Bytes.fromLong(1), 0, data, idx, 8);
          idx -= 8;
          System.arraycopy(Bytes.fromLong(local_ts), 0, data, idx, 8);
          idx -= 8;
          local_ts -= INTERVAL;
        }
      } else {
        for (int i = 0; i < INTERVALS_PER_CHUNK; i++) {
          System.arraycopy(Bytes.fromLong(local_ts), 0, data, idx, 8);
          idx += 8;
          //System.arraycopy(Bytes.fromLong(i + 1 * x), 0, payload, idx, 8);
          System.arraycopy(Bytes.fromLong(1), 0, data, idx, 8);
          idx += 8;
          local_ts += INTERVAL;
        }
      }
      results.put(timeseries.get(x), data);
    }
    return results;
  }
  
  Map<TSByteId, byte[]> getStringsChunk(long ts, boolean reverse) {
    Map<TSByteId, byte[]> results = Maps.newHashMap();
    for (int x = 0; x < timeseries.size(); x++) {
      if (Bytes.memcmp("sys.if.out".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0 && 
          Bytes.memcmp("sys.if.in".getBytes(Const.UTF8_CHARSET), timeseries.get(x).metrics().get(0)) != 0) {
        continue;
      }
      
      byte[] data = new byte[INTERVALS_PER_CHUNK * (8 + 3)];
      long local_ts = ts;
      int idx = reverse ? data.length - 3 : 0;
      
      if (reverse) {
        for (int i = INTERVALS_PER_CHUNK - 1; i >= 0; i--) {
          System.arraycopy(i % 2 == 0 ? "foo".getBytes(Const.UTF8_CHARSET) : "bar".getBytes(Const.UTF8_CHARSET), 
              0, data, idx, 3);
          idx -= 8;
          System.arraycopy(Bytes.fromLong(local_ts), 0, data, idx, 8);
          idx -= 3;
          local_ts -= INTERVAL;
        }
      } else {
        for (int i = 0; i < INTERVALS_PER_CHUNK; i++) {
          System.arraycopy(Bytes.fromLong(local_ts), 0, data, idx, 8);
          idx += 8;
          System.arraycopy(i % 2 == 0 ? "foo".getBytes(Const.UTF8_CHARSET) : "bar".getBytes(Const.UTF8_CHARSET), 
              0, data, idx, 3);
          idx += 3;
          local_ts += INTERVAL;
        }
      }
      results.put(timeseries.get(x), data);
    }
    return results;
  }
}
