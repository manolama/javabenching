package net.opentsdb.timeseries;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.capnproto.BufferedOutputStream;
import org.capnproto.BufferedOutputStreamWrapper;
import org.capnproto.Data;
import org.capnproto.DataList;
import org.capnproto.MessageReader;
import org.capnproto.PrimitiveList;
import org.capnproto.PrimitiveList.Long.Builder;
import org.capnproto.StructList;
import org.openjdk.jmh.infra.Blackhole;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.SerDesBytes;
import net.opentsdb.capnp.TimeSeriesCapnp.TimeSeries;
import net.opentsdb.capnp.TimeSeriesIdCapnp.TimeSeriesId;
import net.opentsdb.common.Const;

public class Capnp implements TimeSeriesBench {
  org.capnproto.MessageBuilder message;
  TimeSeries.Reader deser;
  TimeSeries.Builder builder;
  List<Long> timestamps;
  List<Double> values;
  List<String> namespaces;
  List<String> metrics;
  Map<String, String> tags;
  List<String> agg_tags;
  
  public Capnp() {
    message = new org.capnproto.MessageBuilder();
    builder = message.initRoot(TimeSeries.factory);
    timestamps = Lists.newArrayList();
    values = Lists.newArrayList();
    namespaces = Lists.newArrayList();
    metrics = Lists.newArrayList();
    tags = Maps.newHashMap();
    agg_tags = Lists.newArrayList();
  }
  
  public Capnp(final byte[] raw) {
    ByteArrayInputStream bais = new ByteArrayInputStream(raw);
    try {
      MessageReader reader = org.capnproto.SerializePacked.readFromUnbuffered(Channels.newChannel(bais));
      deser = reader.getRoot(TimeSeries.factory);
    } catch (IOException e) {
      throw new RuntimeException("WTF", e);
    }
  }
  
  @Override
  public void addDP(long timestamp, double value) {
    timestamps.add(timestamp);
    values.add(value);
  }

  @Override
  public void setDPs(byte[] dps) {
    builder.setPayload(dps);
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
    builder.setBasetime(base);
  }

  @Override
  public void setEncoding(int encoding) {
    builder.setEncoding(encoding);
  }

  @Override
  public byte[] getBytes() {
    if (timestamps.size() > 0) {
      PrimitiveList.Long.Builder tss = builder.initTimestamps(timestamps.size());
      PrimitiveList.Double.Builder vs = builder.initValues(values.size());
      for (int i = 0; i < timestamps.size(); i++) {
        tss.set(i, timestamps.get(i));
        vs.set(i, values.get(i));
      }
    }
    
    TimeSeriesId.Builder id = builder.initId();
    DataList.Builder ns = id.initNamespaces(namespaces.size());
    for (int i = 0; i < namespaces.size(); i++) {
      ns.set(i, new Data.Reader(namespaces.get(i).getBytes(Const.UTF8_CHARSET)));
    }
    
    DataList.Builder ms = id.initMetrics(metrics.size());
    for (int i = 0; i < metrics.size(); i++) {
      ms.set(i, new Data.Reader(metrics.get(i).getBytes(Const.UTF8_CHARSET)));
    }
    
    DataList.Builder tagks = id.initTagKeys(tags.size());
    DataList.Builder tagvs = id.initTagValues(tags.size());
    int i = 0;
    for (Entry<String, String> pair : tags.entrySet()) {
      tagks.set(i, new Data.Reader(pair.getKey().getBytes(Const.UTF8_CHARSET)));
      tagvs.set(i++, new Data.Reader(pair.getValue().getBytes(Const.UTF8_CHARSET)));
    }
    
    DataList.Builder agg = id.initAggregatedTags(agg_tags.size());
    for (i = 0; i < agg_tags.size(); i++) {
      agg.set(i, new Data.Reader(agg_tags.get(i).getBytes(Const.UTF8_CHARSET)));
    }
    
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    try {
      org.capnproto.SerializePacked.writeToUnbuffered(Channels.newChannel(bas), message);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    return bas.toByteArray();
  }

  @Override
  public void consume(Blackhole blackHole) {
    if (blackHole == null) {
      System.out.println("Base: " + deser.getBasetime());
      System.out.println("Encoding: " + deser.getEncoding());
    } else {
      blackHole.consume(deser.getBasetime());
      blackHole.consume(deser.getEncoding());
    }
    
    if (deser.getPayload() != null) {
      SerDesBytes.consumePayload(blackHole, deser.getPayload().toArray());
    } else {
      for (int i = 0; i < deser.getTimestamps().size(); i++) {
        if (blackHole == null) {
          System.out.println(deser.getTimestamps().get(i) + " " + deser.getValues().get(i));
        } else {
          blackHole.consume(deser.getTimestamps().get(i));
          blackHole.consume(deser.getValues().get(i));
        }
      }
    }
    
    for (int i = 0; i < deser.getId().getNamespaces().size(); i++) {
      if (blackHole == null) {
        System.out.println("Namespace: " + new String(deser.getId().getNamespaces().get(i).toArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(deser.getId().getNamespaces().get(i).toArray(), Const.UTF8_CHARSET));
      }
    }
    
    for (int i = 0; i < deser.getId().getMetrics().size(); i++) {
      if (blackHole == null) {
        System.out.println("Metric: " + new String(deser.getId().getMetrics().get(i).toArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(deser.getId().getMetrics().get(i).toArray(), Const.UTF8_CHARSET));
      }
    }
    
    for (int i = 0; i < deser.getId().getTagKeys().size(); i++) {
      if (blackHole == null) {
        System.out.println("Tags: " + new String(deser.getId().getTagKeys().get(i).toArray(), Const.UTF8_CHARSET)
            + "=" + new String(deser.getId().getTagValues().get(i).toArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(deser.getId().getTagKeys().get(i).toArray(), Const.UTF8_CHARSET));
        blackHole.consume(new String(deser.getId().getTagValues().get(i).toArray(), Const.UTF8_CHARSET));
      }
    }
    
    for (int i = 0; i < deser.getId().getAggregatedTags().size(); i++) {
      if (blackHole == null) {
        System.out.println("Agg Tag: " + new String(deser.getId().getAggregatedTags().get(i).toArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(deser.getId().getAggregatedTags().get(i).toArray(), Const.UTF8_CHARSET));
      }
    }
  }

}
