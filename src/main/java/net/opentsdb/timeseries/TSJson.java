package net.opentsdb.timeseries;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openjdk.jmh.infra.Blackhole;

import com.fasterxml.jackson.annotation.JsonIgnore;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Maps;
import net.opentsdb.SerDesBytes;
import net.opentsdb.common.Const;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Bytes.ByteMap;

public class TSJson implements TimeSeriesBench {
  List<Long> timestamps;
  List<Double> values;
  List<byte[]> namespaces;
  List<byte[]> metrics;
  ByteMap<byte[]> tags;
  List<byte[]> agg_tags;
  byte[] payload;
  long basetime;
  int encoding;
  
  public TSJson() {
    timestamps = Lists.newArrayList();
    values = Lists.newArrayList();
    namespaces = Lists.newArrayList();
    metrics = Lists.newArrayList();
    tags = new ByteMap();
    agg_tags = Lists.newArrayList();
  }
  
  public TSJson(final byte[] raw) {
    TSJson temp = JSON.parseToObject(raw, TSJson.class);
    this.timestamps = temp.timestamps;
    this.values = temp.values;
    this.namespaces = temp.namespaces;
    this.metrics = temp.metrics;
    this.tags = temp.tags;
    this.agg_tags = temp.agg_tags;
    this.payload = temp.payload;
    this.basetime = temp.basetime;
    this.encoding = temp.encoding;
  }
  
  @Override
  public void addDP(long timestamp, double value) {
    timestamps.add(timestamp);
    values.add(value);
  }
  
  @JsonIgnore
  @Override
  public void setDPs(byte[] dps) {
    payload = dps;
  }
  
  public void setPayload(final String dps) {
    payload = UniqueId.stringToUid(dps);
  }
  
  public String getPayload() {
    return UniqueId.uidToString(payload);
  }
  
  public void setMetrics(final List<String> metrics) {
    if (metrics != null) {
      for (String m : metrics) {
        this.metrics.add(UniqueId.stringToUid(m));
      }
    }
  }
  
  public List<String> getMetrics() {
    List<String> ms = Lists.newArrayListWithCapacity(metrics.size());
    for (final byte[] m : metrics) {
      ms.add(UniqueId.uidToString(m));
    }
    return ms;
  }
  
  @Override
  public void addMetric(String metric) {
    metrics.add(metric.getBytes(Const.UTF8_CHARSET));
  }
  
  public void setNamespaces(final List<String> namespaces) {
    if (namespaces != null) {
      for (String m : namespaces) {
        this.namespaces.add(UniqueId.stringToUid(m));
      }
    }
  }
  
  public List<String> getNamespaces() {
    List<String> ms = Lists.newArrayListWithCapacity(namespaces.size());
    for (final byte[] m : namespaces) {
      ms.add(UniqueId.uidToString(m));
    }
    return ms;
  }
  
  @Override
  public void addNamespace(String namespace) {
    namespaces.add(namespace.getBytes(Const.UTF8_CHARSET));
  }
  
  public void setTags(final Map<String, String> tags) {
    if (tags != null) {
      for (Entry<String, String> entry : tags.entrySet()) {
        this.tags.put(UniqueId.stringToUid(entry.getKey()), 
            UniqueId.stringToUid(entry.getValue()));
      }
    }
  }
  
  public Map<String, String> getTags() {
    Map<String, String> r = Maps.newHashMapWithExpectedSize(tags.size());
    for (Entry<byte[], byte[]> entry : tags.entrySet()) {
      r.put(UniqueId.uidToString(entry.getKey()), 
          UniqueId.uidToString(entry.getValue()));
    }
    return r;
  }
  
  @Override
  public void addTag(String tagk, String tagv) {
    tags.put(tagk.getBytes(Const.UTF8_CHARSET), tagv.getBytes(Const.UTF8_CHARSET));
  }
  
  public void setAggTags(final List<String> aggTags) {
    if (aggTags != null) {
      for (String m : aggTags) {
        this.agg_tags.add(UniqueId.stringToUid(m));
      }
    }
  }
  
  public List<String> getAggTags() {
    List<String> ms = Lists.newArrayListWithCapacity(agg_tags.size());
    for (final byte[] m : agg_tags) {
      ms.add(UniqueId.uidToString(m));
    }
    return ms;
  }
  
  @Override
  public void addAggTag(String tagk) {
    agg_tags.add(tagk.getBytes(Const.UTF8_CHARSET));
  }
  
  @Override
  public void setBasetime(long base) {
    basetime = base;
  }
  
  public long getBasetime() {
    return basetime;
  }
  
  @Override
  public void setEncoding(int encoding) {
    this.encoding = encoding;
  }
  
  public int getEncoding() {
    return encoding;
  }
  
  @JsonIgnore
  @Override
  public byte[] getBytes() {
    return JSON.serializeToBytes(this);
  }
  
  @Override
  public void consume(Blackhole blackHole) {
    if (blackHole == null) {
      System.out.println("Base: " + basetime);
      System.out.println("Encoding: " + encoding);
    } else {
      blackHole.consume(basetime);
      blackHole.consume(encoding);
    }
    
    if (payload != null) {
      SerDesBytes.consumePayload(blackHole, payload);
    } else {
      for (int i = 0; i < timestamps.size(); i++) {
        if (blackHole == null) {
          System.out.println(timestamps.get(i) + " " + values.get(i));
        } else {
          blackHole.consume(timestamps.get(i));
          blackHole.consume(values.get(i));
        }
      }
    }
    
    for (final byte[] ns : namespaces) {
      if (blackHole == null) {
        System.out.println("Namespace: " + new String(ns, Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(ns, Const.UTF8_CHARSET));
      }
    }
    
    for (final byte[] m : metrics) {
      if (blackHole == null) {
        System.out.println("Metric: " + new String(m, Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(m, Const.UTF8_CHARSET));
      }
    }
    
    for (Entry<byte[], byte[]> entry : tags) {
      if (blackHole == null) {
        System.out.println("Tags: " + new String(entry.getKey(), Const.UTF8_CHARSET) + "=" +
            new String(entry.getValue(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(entry.getKey(), Const.UTF8_CHARSET));
        blackHole.consume(new String(entry.getValue(), Const.UTF8_CHARSET));
      }
    }
    
    for (final byte[] t : agg_tags) {
      if (blackHole == null) {
        System.out.println("Agg Tag: " + new String(t, Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(t, Const.UTF8_CHARSET));
      }
    }
  }
}
