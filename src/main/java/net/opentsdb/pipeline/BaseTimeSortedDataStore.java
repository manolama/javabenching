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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;

import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.utils.Bytes;

/**
 * And example data source. It just
 */
public abstract class BaseTimeSortedDataStore {
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
  public List<TimeSeriesStringId> timeseries;
  public long start_ts = 0; // in ms
  public boolean with_strings;
  
  public BaseTimeSortedDataStore(boolean with_strings) {
    this.with_strings = with_strings;
    timeseries = Lists.newArrayList();
    
    for (final String metric : METRICS) {
      for (final String dc : DATACENTERS) {
        for (int h = 0; h < HOSTS; h++) {
          TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
              .setMetric(metric)
              .addTags("dc", dc)
              .addTags("host", String.format("web%02d", h + 1))
              .build();
          timeseries.add(id);
        }
      }
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
  public Map<TimeSeriesStringId, byte[]> getChunk(DataType type, long ts, boolean reverse) {
    switch (type) {
    case NUMBERS:
      return getNumbersChunk(ts, reverse);
    case STRINGS:
      return getStringsChunk(ts, reverse);
      default:
        throw new RuntimeException("WTF?");
    }
  }
  
  Map<TimeSeriesStringId, byte[]> getNumbersChunk(long ts, boolean reverse) {
    Map<TimeSeriesStringId, byte[]> results = Maps.newHashMap();
    for (int x = 0; x < timeseries.size(); x++) {
      if ("sys.if.out".equals(timeseries.get(x).metric()) && 
          "sys.if.in".equals(timeseries.get(x).metric())) {
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
  
  Map<TimeSeriesStringId, byte[]> getStringsChunk(long ts, boolean reverse) {
    Map<TimeSeriesStringId, byte[]> results = Maps.newHashMap();
    for (int x = 0; x < timeseries.size(); x++) {
      if ("sys.if.out".equals(timeseries.get(x).metric()) && 
          "sys.if.in".equals(timeseries.get(x).metric())) {
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
