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
package net.opentsdb.timeseries;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.openjdk.jmh.infra.Blackhole;

import com.google.flatbuffers.FlatBufferBuilder;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.SerDesBytes;
import net.opentsdb.common.Const;
import net.opentsdb.flatbuffers.ByteArray;
import net.opentsdb.flatbuffers.TimeSeries;
import net.opentsdb.flatbuffers.TimeSeriesId;

public class FlatB implements TimeSeriesBench {
  FlatBufferBuilder builder;
  TimeSeries ts;
  List<Long> timestamps;
  List<Double> values;
  List<String> namespaces;
  List<String> metrics;
  Map<String, String> tags;
  List<String> agg_tags;
  byte[] payload;
  long basetime;
  int encoding;
  
  public FlatB() {
    builder = new FlatBufferBuilder();
    timestamps = Lists.newArrayList();
    values = Lists.newArrayList();
    namespaces = Lists.newArrayList();
    metrics = Lists.newArrayList();
    tags = Maps.newHashMap();
    agg_tags = Lists.newArrayList();
  }
  
  public FlatB(final byte[] raw) {
    ts = TimeSeries.getRootAsTimeSeries(ByteBuffer.wrap(raw));
  }
  
  @Override
  public void addDP(long timestamp, double value) {
    timestamps.add(timestamp);
    values.add(value);
  }

  @Override
  public void setDPs(byte[] dps) {
    payload = dps;
  }

  @Override
  public void addMetric(String metric) {
    metrics.add(metric);
  }

  @Override
  public void addNamespace(String namespace) {
    namespaces.add(namespace);
  }

  @Override
  public void addTag(String tagk, String tagv) {
    tags.put(tagk, tagv);
  }

  @Override
  public void addAggTag(String tagk) {
    agg_tags.add(tagk);
  }

  @Override
  public void setBasetime(long base) {
    basetime = base;
  }

  @Override
  public void setEncoding(int encoding) {
    this.encoding = encoding;
  }

  @Override
  public byte[] getBytes() {
//    if (timestamps.size() > 0) {
//      TimeSeries.startTimestampsVector(builder, timestamps.size());
//      for (long ts : timestamps) {
//        builder.addLong(ts);
//      }
//      TimeSeries.startValuesVector(builder, values.size());
//      for (double v : values) {
//        builder.addDouble(v);
//      }
//    }
    
    //TimeSeriesId.startTimeSeriesId(builder);
    //TimeSeriesId.startNamespacesVector(builder, namespaces.size());
    int[] offsets = new int[namespaces.size()];
    int i = 0;
    for (String ns : namespaces) {
      offsets[i++] = ByteArray.createBytearrayVector(builder, ns.getBytes(Const.UTF8_CHARSET));
    }
    int offset = TimeSeriesId.createNamespacesVector(builder, offsets);
    
//    offsets = new int[metrics.size()];
//    i = 0;
//    for (String m : metrics) {
//      offsets[i++] = ByteArray.createBytearrayVector(builder, m.getBytes(Const.UTF8_CHARSET));
//    }
//    offset = TimeSeriesId.createMetricsVector(builder, offsets);
    
    TimeSeriesId.startTimeSeriesId(builder);
    TimeSeriesId.addNamespaces(builder, offset);
    offset = TimeSeriesId.endTimeSeriesId(builder);
    
    int po = TimeSeries.createPayloadVector(builder, payload);
    
    TimeSeries.startTimeSeries(builder);
    TimeSeries.addId(builder, offset);
    TimeSeries.addBasetime(builder, basetime);
    TimeSeries.addEncoding(builder, encoding);
    TimeSeries.addPayload(builder, po);
    offset = TimeSeries.endTimeSeries(builder);
    
    builder.finish(offset);
    return builder.sizedByteArray();
  }

  @Override
  public void consume(Blackhole blackHole) {
    if (blackHole == null) {
      System.out.println("Base: " + ts.basetime());
      System.out.println("Encoding: " + ts.encoding());
    } else {
      blackHole.consume(ts.basetime());
      blackHole.consume(ts.encoding());
    }
    
    if (ts.payloadAsByteBuffer() != null) {
      byte[] buf = new byte[ts.payloadLength()];
      ts.payloadAsByteBuffer().get(buf);
      SerDesBytes.consumePayload(blackHole, buf);
    }
    
    System.out.println("NSs: " + ts.id().namespacesLength());
    for (int i = 0; i < ts.id().namespacesLength(); i++) {
      //ByteBuffer b = ts.id().namespaces(i).getByteBuffer();
      System.out.println(ts.id().namespaces(i).bytearrayAsByteBuffer());
      byte[] buf = new byte[ts.id().namespaces(i).bytearrayLength()];
      //b.get(buf);
      String s = new String(buf, Const.UTF8_CHARSET);
      if (blackHole == null) {
        System.out.println("Namespace: " + s);
      } else {
        blackHole.consume(s);
      }
    }
  }

}
