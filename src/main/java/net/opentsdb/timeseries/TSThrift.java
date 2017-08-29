package net.opentsdb.timeseries;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.openjdk.jmh.infra.Blackhole;

import net.opentsdb.SerDesBytes;
import net.opentsdb.common.Const;
import net.opentsdb.thrift.TimeSeries;
import net.opentsdb.thrift.TimeSeriesId;

public class TSThrift implements TimeSeriesBench {
  TimeSeries ts;
  
  public TSThrift() {
    ts = new TimeSeries();
    ts.setTsid(new TimeSeriesId());
  }
  
  public TSThrift(final byte[] raw) {
    ts = new TimeSeries();
    try {
      new TDeserializer().deserialize(ts, raw);
    } catch (TException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
  
  @Override
  public void addDP(long timestamp, double value) {
    ts.addToTimestamps(timestamp);
    ts.addToValues(value);
  }

  @Override
  public void setDPs(byte[] dps) {
    ts.setPayload(dps);
  }

  @Override
  public void addMetric(String metric) {
    ts.tsid.addToMetrics(ByteBuffer.wrap(metric.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void addNamespace(String namespace) {
    ts.tsid.addToNamespaces(ByteBuffer.wrap(namespace.getBytes(Const.UTF8_CHARSET)));
  }
  
  @Override
  public void addTag(String tagk, String tagv) {
    ts.tsid.putToTags(ByteBuffer.wrap(tagk.getBytes(Const.UTF8_CHARSET)), 
        ByteBuffer.wrap(tagv.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void addAggTag(String tagk) {
    ts.tsid.addToAggregated_tags(ByteBuffer.wrap(tagk.getBytes(Const.UTF8_CHARSET)));
  }

  @Override
  public void setBasetime(long base) {
    ts.setBasetime(base);
  }

  @Override
  public void setEncoding(int encoding) {
    ts.setEncoding(encoding);
  }

  @Override
  public byte[] getBytes() {
    try {
      ts.validate();
      return new TSerializer().serialize(ts);
    } catch (TException e) {
      throw new RuntimeException("WTF?", e);
    }
  }

  @Override
  public void consume(Blackhole blackHole) {
    if (blackHole == null) {
      System.out.println("Base: " + ts.getBasetime());
      System.out.println("Encoding: " + ts.getEncoding());
    } else {
      blackHole.consume(ts.getBasetime());
      blackHole.consume(ts.getEncoding());
    }
    
    if (ts.getPayload() != null) {
      SerDesBytes.consumePayload(blackHole, ts.getPayload());
    } else {
      for (int i = 0; i < ts.getTimestampsSize(); i++) {
        if (blackHole == null) {
          System.out.println(ts.getTimestamps().get(i) + " " + ts.getValues().get(i));
        } else {
          blackHole.consume(ts.getTimestamps().get(i));
          blackHole.consume(ts.getValues().get(i));
        }
      }
    }
    
    for (final ByteBuffer ns : ts.getTsid().getNamespaces()) {
      byte[] result = new byte[ns.remaining()];
      ns.get(result);
      String s = new String(result, Const.UTF8_CHARSET);
      if (blackHole == null) {
        System.out.println("Namespace: " + s);
      } else {
        blackHole.consume(s);
      }
    }
    
    for (final ByteBuffer m : ts.getTsid().getMetrics()) {
      byte[] result = new byte[m.remaining()];
      m.get(result);
      String s = new String(result, Const.UTF8_CHARSET);
      if (blackHole == null) {
        System.out.println("Metric: " + s);
      } else {
        blackHole.consume(s);
      }
    }
    
    for (Entry<ByteBuffer, ByteBuffer> entry : ts.getTsid().getTags().entrySet()) {
      byte[] btagk = new byte[entry.getKey().remaining()];
      entry.getKey().get(btagk);
      String tagk = new String(btagk, Const.UTF8_CHARSET);
      
      byte[] btagv = new byte[entry.getValue().remaining()];
      entry.getValue().get(btagv);
      String tagv = new String(btagv, Const.UTF8_CHARSET);
      if (blackHole == null) {
        System.out.println("Tags: " + tagk + "=" + tagv);
      } else {
        blackHole.consume(tagk);
        blackHole.consume(tagv);
      }
    }
    
    for (final ByteBuffer t : ts.getTsid().getAggregated_tags()) {
      byte[] result = new byte[t.remaining()];
      t.get(result);
      String s = new String(result, Const.UTF8_CHARSET);
      if (blackHole == null) {
        System.out.println("Agg Tag: " + s);
      } else {
        blackHole.consume(s);
      }
    }
  }

}
