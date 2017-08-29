package net.opentsdb.timeseries;

import org.openjdk.jmh.infra.Blackhole;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import net.opentsdb.SerDesBytes;
import net.opentsdb.common.Const;
import net.opentsdb.pbuf.TimeSeriesIdPB.TimeSeriesId;
import net.opentsdb.pbuf.TimeSeriesPB.TimeSeries;

public class Pbuf implements TimeSeriesBench {
  private TimeSeriesId.Builder id_builder;
  private TimeSeries.Builder builder;
  private TimeSeries ts;
  
  public Pbuf() {
    id_builder = TimeSeriesId.newBuilder();
    builder = TimeSeries.newBuilder();
  }
  
  public Pbuf(final byte[] raw) {
    try {
      ts = TimeSeries.parseFrom(raw);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
  
  @Override
  public void addDP(long timestamp, double value) {
    builder.addTimestamps(timestamp);
    builder.addValues(value);
  }
  
  @Override
  public void setDPs(final byte[] dps) {
    builder.setPayload(ByteString.copyFrom(dps));
  }

  @Override
  public void addMetric(String metric) {
    id_builder.addMetrics(ByteString.copyFrom(metric.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void addNamespace(String namespace) {
    id_builder.addNamespaces(ByteString.copyFrom(namespace.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void addTag(String tagk, String tagv) {
    id_builder.addTagKeys(ByteString.copyFrom(tagk.getBytes(Const.UTF8_CHARSET)));
    id_builder.addTagValues(ByteString.copyFrom(tagv.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void addAggTag(String tagk) {
    id_builder.addAggregatedTags(ByteString.copyFrom(tagk.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void setBasetime(final long base) {
    builder.setBasetime(base);
  }
  
  @Override
  public void setEncoding(final int encoding) {
    builder.setEncoding(encoding);
  }
  
  @Override
  public byte[] getBytes() {
    if (ts == null) {
      ts = builder.setId(id_builder)
                  .build();
    }
    return ts.toByteArray();
  }

  @Override
  public void consume(Blackhole blackHole) {
    if (ts == null) {
      ts = builder.setId(id_builder)
          .build();
    }
    
    if (blackHole == null) {
      System.out.println("Base: " + ts.getBasetime());
      System.out.println("Encoding: " + ts.getEncoding());
    } else {
      blackHole.consume(ts.getBasetime());
      blackHole.consume(ts.getEncoding());
    }
    
    if (ts.getPayload() != null) {
      SerDesBytes.consumePayload(blackHole, ts.getPayload().toByteArray());
    } else {
      for (int i = 0; i < ts.getTimestampsCount(); i++) {
        if (blackHole == null) {
          System.out.println(ts.getTimestamps(i) + " " + ts.getValues(i));
        } else {
          blackHole.consume(ts.getTimestamps(i));
          blackHole.consume(ts.getValues(i));
        }
      }
    }
    
    for (final ByteString ns : ts.getId().getNamespacesList()) {
      if (blackHole == null) {
        System.out.println("Namespace: " + new String(ns.toByteArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(ns.toByteArray(), Const.UTF8_CHARSET));
      }
    }
    
    for (final ByteString metric : ts.getId().getMetricsList()) {
      if (blackHole == null) {
        System.out.println("Metric: " + new String(metric.toByteArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(metric.toByteArray(), Const.UTF8_CHARSET));
      }
    }
    
    for (int i = 0; i < ts.getId().getTagKeysCount(); i++) {
      if (blackHole == null) {
        System.out.println("Tags: " + new String(ts.getId().getTagKeys(i).toByteArray(), Const.UTF8_CHARSET)
            + "=" + new String(ts.getId().getTagValues(i).toByteArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(ts.getId().getTagKeys(i).toByteArray(), Const.UTF8_CHARSET));
        blackHole.consume(new String(ts.getId().getTagValues(i).toByteArray(), Const.UTF8_CHARSET)); 
      }
    }
    
    for (final ByteString agg : ts.getId().getAggregatedTagsList()) {
      if (blackHole == null) {
        System.out.println("Agg Tag: " + new String(agg.toByteArray(), Const.UTF8_CHARSET));
      } else {
        blackHole.consume(new String(agg.toByteArray(), Const.UTF8_CHARSET));
      }
    }
  }

}
