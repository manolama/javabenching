//// This file is part of OpenTSDB.
//// Copyright (C) 2017  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.timeseries;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import org.apache.avro.io.BinaryEncoder;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.Decoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.avro.specific.SpecificDatumReader;
//import org.apache.avro.specific.SpecificDatumWriter;
//import org.openjdk.jmh.infra.Blackhole;
//
//import avro.shaded.com.google.common.collect.Lists;
//import avro.shaded.com.google.common.collect.Maps;
//import net.opentsdb.SerDesBytes;
//import net.opentsdb.avro.TimeSeries;
//import net.opentsdb.avro.TimeSeriesId;
//import net.opentsdb.common.Const;
//
//public class TSAvro implements TimeSeriesBench {
//  TimeSeries ts;
//  List<Long> timestamps;
//  List<Double> values;
//  List<String> namespaces;
//  List<String> metrics;
//  Map<String, String> tags;
//  List<String> agg_tags;
//  byte[] payload;
//  long basetime;
//  int encoding;
//  
//  public TSAvro() {
//    timestamps = Lists.newArrayList();
//    values = Lists.newArrayList();
//    namespaces = Lists.newArrayList();
//    metrics = Lists.newArrayList();
//    tags = Maps.newHashMap();
//    agg_tags = Lists.newArrayList();
//  }
//  
//  public TSAvro(final byte[] raw) {
//    Decoder decoder = DecoderFactory.get().binaryDecoder(raw, null);
//    DatumReader<TimeSeries> reader = new SpecificDatumReader<TimeSeries>(TimeSeries.getClassSchema());
//    try {
//      ts = reader.read(null, decoder);
//    } catch (IOException e) {
//      throw new RuntimeException("WTF?", e);
//    }
//  }
//  
//  @Override
//  public void addDP(long timestamp, double value) {
//    timestamps.add(timestamp);
//    values.add(value);
//  }
//
//  @Override
//  public void setDPs(byte[] dps) {
//    payload = dps;
//  }
//
//  @Override
//  public void addMetric(String metric) {
//    metrics.add(metric);
//  }
//
//  @Override
//  public void addNamespace(String namespace) {
//    namespaces.add(namespace);
//  }
//
//  @Override
//  public void addTag(String tagk, String tagv) {
//    tags.put(tagk, tagv);
//  }
//
//  @Override
//  public void addAggTag(String tagk) {
//    agg_tags.add(tagk);
//  }
//
//  @Override
//  public void setBasetime(long base) {
//    basetime = base;
//  }
//
//  @Override
//  public void setEncoding(int encoding) {
//    this.encoding = encoding;
//  }
//
//  @Override
//  public byte[] getBytes() {
//    TimeSeriesQueryId.Builder id = TimeSeriesQueryId.newBuilder()
//        .setAlias("")
//        .setAggregatedTagValues(Lists.newArrayListWithCapacity(0))
//        .setDisjointTags(Lists.newArrayListWithCapacity(0))
//        .setDisjointTagValues(Lists.newArrayListWithCapacity(0));
//    List<ByteBuffer> buf = Lists.newArrayListWithCapacity(namespaces.size());
//    for (String ns : namespaces) {
//      buf.add(ByteBuffer.wrap(ns.getBytes(Const.UTF8_CHARSET)));
//    }
//    id.setNamespaces(buf);
//    
//    buf = Lists.newArrayListWithCapacity(metrics.size());
//    for (String m : metrics) {
//      buf.add(ByteBuffer.wrap(m.getBytes(Const.UTF8_CHARSET)));
//    }
//    id.setMetrics(buf);
//    
//    buf = Lists.newArrayListWithCapacity(tags.size());
//    List<ByteBuffer> tagvs = Lists.newArrayListWithCapacity(tags.size());
//    for (Entry<String, String> entry : tags.entrySet()) {
//      buf.add(ByteBuffer.wrap(entry.getKey().getBytes(Const.UTF8_CHARSET)));
//      tagvs.add(ByteBuffer.wrap(entry.getValue().getBytes(Const.UTF8_CHARSET)));
//    }
//    id.setTagKeys(buf);
//    id.setTagValues(tagvs);
//    
//    buf = Lists.newArrayListWithCapacity(agg_tags.size());
//    for (String t : agg_tags) {
//      buf.add(ByteBuffer.wrap(t.getBytes(Const.UTF8_CHARSET)));
//    }
//    id.setAggregatedTags(buf);
//    
//    TimeSeries.Builder builder = TimeSeries.newBuilder()
//        .setId(id.build())
//        .setBasetime(basetime)
//        .setEncoding(encoding);
//    if (payload != null) {
//      builder.setPayload(ByteBuffer.wrap(payload));
//      builder.setTimestamps(Lists.newArrayListWithCapacity(0))
//             .setValues(Lists.newArrayListWithCapacity(0));
//    } else {
//      builder.setTimestamps(timestamps);
//      builder.setValues(values);
//    }
//    
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
//    DatumWriter<TimeSeries> writer = new SpecificDatumWriter<TimeSeries>(TimeSeries.getClassSchema());
//    try {
//      writer.write(builder.build(), encoder);
//      encoder.flush();
//      baos.close();
//      return baos.toByteArray();
//    } catch (IOException e) {
//      throw new RuntimeException("WTF?", e);
//    }
//  }
//
//  @Override
//  public void consume(Blackhole blackHole) {
//    if (blackHole == null) {
//      System.out.println("Base: " + ts.getBasetime());
//      System.out.println("Encoding: " + ts.getEncoding());
//    } else {
//      blackHole.consume(ts.getBasetime());
//      blackHole.consume(ts.getEncoding());
//    }
//    
//    if (ts.getPayload() != null) {
//      byte[] buf = new byte[ts.getPayload().remaining()];
//      ts.getPayload().get(buf);
//      SerDesBytes.consumePayload(blackHole, buf);
//    } else {
//      for (int i = 0; i < ts.getTimestamps().size(); i++) {
//        if (blackHole == null) {
//          System.out.println(ts.getTimestamps().get(i) + " " + ts.getValues().get(i));
//        } else {
//          blackHole.consume(ts.getTimestamps().get(i));
//          blackHole.consume(ts.getValues().get(i));
//        }
//      }
//    }
//    
//    for (final ByteBuffer ns : ts.getId().getNamespaces()) {
//      byte[] buf = new byte[ns.remaining()];
//      ns.get(buf);
//      String s = new String(buf, Const.UTF8_CHARSET);
//      if (blackHole == null) {
//        System.out.println("Namespace: " + s);
//      } else {
//        blackHole.consume(s);
//      }
//    }
//    
//    for (final ByteBuffer m : ts.getId().getMetrics()) {
//      byte[] buf = new byte[m.remaining()];
//      m.get(buf);
//      String s = new String(buf, Const.UTF8_CHARSET);
//      if (blackHole == null) {
//        System.out.println("Metric: " + s);
//      } else {
//        blackHole.consume(s);
//      }
//    }
//    
//    for (int i = 0; i < ts.getId().getTagKeys().size(); i++) {
//      ByteBuffer b = ts.getId().getTagKeys().get(i);
//      byte[] btagk = new byte[b.remaining()];
//      b.get(btagk);
//      String tagk = new String(btagk, Const.UTF8_CHARSET);
//      
//      b = ts.getId().getTagValues().get(i);
//      byte[] btagv = new byte[b.remaining()];
//      b.get(btagv);
//      String tagv = new String(btagv, Const.UTF8_CHARSET);
//      
//      if (blackHole == null) {
//        System.out.println("Tags: " + tagk + "=" + tagv);
//      } else {
//        blackHole.consume(tagk);
//        blackHole.consume(tagv);
//      }
//    }
//    
//    for (final ByteBuffer t : ts.getId().getAggregatedTags()) {
//      byte[] buf = new byte[t.remaining()];
//      t.get(buf);
//      String s = new String(buf, Const.UTF8_CHARSET);
//      if (blackHole == null) {
//        System.out.println("Agg Tag: " + s);
//      } else {
//        blackHole.consume(s);
//      }
//    }
//  }
//
//}
