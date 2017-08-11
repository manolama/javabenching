package net.opentsdb;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.collect.Lists;

import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class GroupByAndSum {
  /** Key we're grouping on */
  static final byte[] KEY = "tagk".getBytes();
  
  /** How many source time series to create */
  static final int TIMESERIES = 1000;
  
  static final int GROUPS = 10;
  
  @State(Scope.Thread)
  public static class Context
  {
    public List<TS> source;
    
    @Setup
    public void setup() {
      int tagv = 0;
      long timestamp = 1501711500;
      source = Lists.newArrayList();
      for (int i = 0; i < TIMESERIES; i++) {
        if (i % GROUPS == 0) {
          tagv++;
        }
        source.add(new TS(timestamp, 1440, Integer.toString(tagv)));
      }
      Collections.shuffle(source);
    }
  }

  @Benchmark
  public static void runStreamedSerial(Context context, Blackhole blackHole) {
    blackHole.consume(context.source.stream()
        .map(series -> new AbstractMap.SimpleEntry<byte[], TS>(series.id.tags().get(KEY), series))
        .collect(
            groupingBy(Map.Entry::getKey, ByteMap::new, mapping(Map.Entry::getValue, toList()))
         ).entrySet().stream()
        .map(e -> {
          MergedTimeSeriesId.Builder id = MergedTimeSeriesId.newBuilder();
          return e.getValue().stream()
            .flatMap(ts -> {
                id.addSeries(ts.id);
                return StreamSupport.stream(ts.spliterator(), false);
            })
            .collect(groupingBy(DP::getTS,
                TreeMap::new, // otherwise we lose sort order....
                Collectors.summingDouble(DP::getV)))
            .entrySet().stream()
              .collect(() -> new TS(id.build()), 
                  (ts, dp) -> { ts.addDp(dp.getKey(), dp.getValue()); }, 
                  (ts1, ts2) -> { System.out.println("COMBINING..."); });
          
        }).collect(toList()));
  }
  
  @Benchmark
  public static void runStreamedParallel(Context context, Blackhole blackHole) {
    blackHole.consume(context.source.stream()
        .map(series -> new AbstractMap.SimpleEntry<byte[], TS>(series.id.tags().get(KEY), series))
        .collect(
            groupingBy(Map.Entry::getKey, ByteMap::new, mapping(Map.Entry::getValue, toList()))
         ).entrySet().stream()
        .parallel()
        .map(e -> {
          MergedTimeSeriesId.Builder id = MergedTimeSeriesId.newBuilder();
          return e.getValue().stream()
            .flatMap(ts -> {
                id.addSeries(ts.id);
                return StreamSupport.stream(ts.spliterator(), false);
            })
            .collect(groupingBy(DP::getTS,
                TreeMap::new, // otherwise we lose sort order....
                Collectors.summingDouble(DP::getV)))
            .entrySet().stream()
              .collect(() -> new TS(id.build()), 
                  (ts, dp) -> { ts.addDp(dp.getKey(), dp.getValue()); }, 
                  (ts1, ts2) -> { System.out.println("COMBINING..."); });
          
        }).collect(toList()));
  }

  @Benchmark
  public static void runTraditional(Context context, Blackhole blackHole) {
    // group by
    ByteMap<List<TS>> grouped = new ByteMap<List<TS>>();
    for (final TS t : context.source) {
      List<TS> l = grouped.get(t.id.tags().get(KEY));
      if (l == null) {
        l = Lists.newArrayList();
        grouped.put(t.id.tags().get(KEY), l);
      }
      l.add(t);
    }
    
    List<TS> results = Lists.newArrayList();
    for (final List<TS> l : grouped.values()) {
      MergedTimeSeriesId.Builder id = MergedTimeSeriesId.newBuilder();
      Iterator<DP>[] its = new Iterator[l.size()];
      int x = 0;
      for (final TS t : l) {
        id.addSeries(t.id);
        its[x++] = t.iterator();
      }
      
      TS times = new TS(id.build());
      double[] vals = new double[l.size()];
      while (its[0].hasNext()) {
        DP dp = its[0].next();
        vals[0] = dp.getV();
        
        for (int i = 1; i < l.size(); i++) {
          vals[i] = its[i].next().getV();
        }
        
        double sum = 0;
        for (double v : vals) {
          sum += v;
        }
        
        times.addDp(dp.getTS(), sum);
      }
      
      results.add(times);
    }
    blackHole.consume(results);
  }
  
  static class TS implements Iterable<DP> {
    TimeSeriesId id;
    byte[] data;
    int write_idx = 0;
    
    public TS() {
      data = new byte[16];
    }
    
    public TS(TimeSeriesId id) {
      this.id = id;
      data = new byte[16];
    }
    
    public TS(final long ts, final int count, String tagv) {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("Metric1"))
          .addTags("tagk", tagv)
          .build();
      data = new byte[count * 16];
      
      for (int i = 0; i < count; i++) {
        System.arraycopy(Bytes.fromLong(ts + (i * 60)), 0, 
            data, i * 16, 8);
        System.arraycopy(Bytes.fromLong(Double.doubleToLongBits((double) i)), 0, 
            data, ((i * 16) + 8), 8);
      }
      write_idx = data.length;
    }
    
    public void addDp(final long ts, final double v) {
      if (write_idx >= data.length) {
        byte[] temp = new byte[data.length * 2];
        System.arraycopy(data, 0, temp, 0, data.length);
        data = temp;
      }
      
      System.arraycopy(Bytes.fromLong(ts), 0, data, write_idx, 8);
      write_idx += 8;
      System.arraycopy(Bytes.fromLong(Double.doubleToLongBits(v)), 0, 
          data, write_idx, 8);
      write_idx += 8;
    }
    
    @Override
    public Iterator<DP> iterator() {
      return new MyIterator();
    }
    
    class MyIterator implements Iterator<DP>, DP {
      int idx = 0;
      
      @Override
      public boolean hasNext() {
        return idx + 16 < write_idx;
      }

      @Override
      public DP next() {
        idx += 16;
        return this;
      }

      @Override
      public void remove() {
      }

      @Override
      public double getV() {
        return Double.longBitsToDouble(Bytes.getLong(data, idx + 8));
      }

      @Override
      public long getTS() {
        return Bytes.getLong(data, idx);
      }
      
    }
  }
  
  static interface DP {
    public double getV();
    public long getTS();
  }
  
}
