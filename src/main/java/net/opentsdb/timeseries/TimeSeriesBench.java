package net.opentsdb.timeseries;

import org.openjdk.jmh.infra.Blackhole;

public interface TimeSeriesBench {

  public void addDP(final long timestamp, final double value);
  
  public void setDPs(final byte[] dps);
  
  public void addMetric(final String metric);
  
  public void addNamespace(final String namespace);
  
  public void addTag(final String tagk, final String tagv);
  
  public void addAggTag(final String tagk);
  
  public void setBasetime(final long base);
  
  public void setEncoding(final int encoding);
  
  public byte[] getBytes();
  
  public void consume(final Blackhole blackHole);
}
