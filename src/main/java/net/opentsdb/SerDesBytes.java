package net.opentsdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import net.opentsdb.timeseries.Capnp;
import net.opentsdb.timeseries.FlatB;
import net.opentsdb.timeseries.Pbuf;
import net.opentsdb.timeseries.TSAvro;
import net.opentsdb.timeseries.TSJson;
import net.opentsdb.timeseries.TSThrift;
import net.opentsdb.timeseries.TimeSeriesBench;
import net.opentsdb.utils.Bytes;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
//@AuxCounters
public class SerDesBytes {
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  public static long size = 0;
  
  static final long START_TS = 1483228800;
  
  /** How many source time series to create */
  static final int TIMESERIES = 100;
  
  static final int DPS = 1440;
  
  static final int METRICS = 1;
  static final int TAGS = 6;
  static final int NAMESPACES = 1;
  static final int AGG_TAGS = 3;
  
  public static void main(final String[] args) {
    Context ctx = new Context();
    ctx.setup();
    Counters ctrs = new Counters();
    //runProtoBuf(ctx, ctrs, null);
    //runThrift(ctx, ctrs, null);
    //runCapnp(ctx, ctrs, null);
    //runFlatBuffers(ctx, ctrs, null);
    //runAvro(ctx, ctrs, null);
    runJson(ctx, ctrs, null);
  }
  
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class Counters {
    public long sizeMax;
    private long sizeMin = Long.MAX_VALUE;
    public long compressedMax;
    private long compressedMin = Long.MAX_VALUE;
  }
  
  @State(Scope.Thread)
  public static class Context {
    public double[] values;
    public String[] strings;
    
    @Setup
    public void setup() {
      Random rnd = new Random(System.currentTimeMillis());
      values = new double[DPS * TIMESERIES];
      for (int i = 0; i < DPS * TIMESERIES; i++) {
        values[i] = rnd.nextDouble() * rnd.nextInt();
      }
      
      RandomString rs = new RandomString(64);
      strings = new String[TIMESERIES * (NAMESPACES + METRICS + (TAGS * 2) + AGG_TAGS)];
      for (int i = 0; i < TIMESERIES * (NAMESPACES + METRICS + (TAGS * 2) + AGG_TAGS); i++) {
        strings[i] = rs.nextString();
      }
    }
  }
  
  @Benchmark
  public static void runProtoBuf(Context context, Counters ctrs, Blackhole blackHole) {
    int idx = 0;
    
    for (int i = 0; i < TIMESERIES; i++) {
      Pbuf bench = new Pbuf();
      idx = serialize(context, bench, idx, i);
      
      byte[] serialized = bench.getBytes();
      setCtrs(ctrs, serialized);
      compress(ctrs, serialized);
      
      bench = new Pbuf(serialized);
      bench.consume(blackHole);
    }
  }

  @Benchmark
  public static void runThrift(Context context, Counters ctrs, Blackhole blackHole) {
    int idx = 0;
    
    for (int i = 0; i < TIMESERIES; i++) {
      TSThrift bench = new TSThrift();
      idx = serialize(context, bench, idx, i);
      
      byte[] serialized = bench.getBytes();
      setCtrs(ctrs, serialized);
      compress(ctrs, serialized);
      
      bench = new TSThrift(serialized);
      bench.consume(blackHole);
    }
  }
  
//  @Benchmark
//  public static void runFlatBuffers(Context context, Counters ctrs, Blackhole blackHole) {
//    int idx = 0;
//    
//    for (int i = 0; i < TIMESERIES; i++) {
//      FlatB bench = new FlatB();
//      idx = serialize(context, bench, idx, i);
//      
//      byte[] serialized = bench.getBytes();
//      setCtrs(ctrs, serialized);
//      compress(ctrs, serialized);
//      
//      bench = new FlatB(serialized);
//      bench.consume(blackHole);
//    }
//  }
  
  @Benchmark
  public static void runAvro(Context context, Counters ctrs, Blackhole blackHole) {
    int idx = 0;
    
    for (int i = 0; i < TIMESERIES; i++) {
      TSAvro bench = new TSAvro();
      idx = serialize(context, bench, idx, i);
      
      byte[] serialized = bench.getBytes();
      setCtrs(ctrs, serialized);
      compress(ctrs, serialized);
      
      bench = new TSAvro(serialized);
      bench.consume(blackHole);
    }
  }
  
  @Benchmark
  public static void runJson(Context context, Counters ctrs, Blackhole blackHole) {
    int idx = 0;
    
    for (int i = 0; i < TIMESERIES; i++) {
      TSJson bench = new TSJson();
      idx = serialize(context, bench, idx, i);
      
      byte[] serialized = bench.getBytes();
      setCtrs(ctrs, serialized);
      compress(ctrs, serialized);
      
      bench = new TSJson(serialized);
      bench.consume(blackHole);
    }
  }
  
  @Benchmark
  public static void runCapnp(Context context, Counters ctrs, Blackhole blackHole) {
    int idx = 0;
    
    for (int i = 0; i < TIMESERIES; i++) {
      Capnp bench = new Capnp();
      idx = serialize(context, bench, idx, i);
      
      byte[] serialized = bench.getBytes();
      setCtrs(ctrs, serialized);
      compress(ctrs, serialized);
      
      bench = new Capnp(serialized);
      bench.consume(blackHole);
    }
  }
  
  static void setCtrs(Counters ctrs, byte[] payload) {
    if (payload.length > ctrs.sizeMax) {
      ctrs.sizeMax = payload.length;
    }
    if (payload.length > 0 && payload.length < ctrs.sizeMin) {
      ctrs.sizeMin = payload.length;
    }
  }
  
  static void compress(Counters ctrs, byte[] payload) {
    try {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      GZIPOutputStream out = new GZIPOutputStream(bas);
      out.write(payload);
      out.flush();
      out.close();
      
      byte[] compressed = bas.toByteArray();
      if (compressed.length > ctrs.compressedMax) {
        ctrs.compressedMax = compressed.length;
      }
      if (compressed.length < ctrs.compressedMin) {
        ctrs.compressedMin = compressed.length;
      }
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
  
  static int serialize(Context context, TimeSeriesBench bench, int startingIdx, int tsIdx) {
    int idx = startingIdx;
    
    for (int x = 0; x < NAMESPACES; x++) {
      bench.addNamespace(context.strings[idx++]);
    }
    
    for (int x = 0; x < METRICS; x++) {
      bench.addMetric(context.strings[idx++]);
    }
    
    for (int x = 0; x < TAGS; x++) {
      bench.addTag(context.strings[idx++], context.strings[idx++]);
    }
    
    for (int x = 0; x < AGG_TAGS; x++) {
      bench.addAggTag(context.strings[idx++]);
    }
    
    bench.setBasetime(START_TS);
    bench.setEncoding(1);
    bench.setDPs(getValues(context, tsIdx));
    return idx;
  }
  
  static byte[] getValues(Context context, int idx) {
    byte[] values = new byte[DPS * 16];
    for (int i = 0; i < values.length; ) {
      System.arraycopy(Bytes.fromLong(START_TS + (i * 60)), 0, values, i, 8);
      i += 8;
      System.arraycopy(Bytes.fromLong(Double.doubleToRawLongBits(context.values[idx + i])), 0, values, i, 8);
      i += 8;
    }
    return values;
  }
  
  /** Shamelessly borrowed from https://stackoverflow.com/questions/41107/how-to-generate-a-random-alpha-numeric-string. */
  public static class RandomString {

    /**
     * Generate a random string.
     */
    public String nextString() {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static final String lower = upper.toLowerCase(Locale.ROOT);

    public static final String digits = "0123456789";

    public static final String alphanum = upper + lower + digits;

    private final Random random;

    private final char[] symbols;

    private final char[] buf;

    public RandomString(int length, Random random, String symbols) {
        if (length < 1) throw new IllegalArgumentException();
        if (symbols.length() < 2) throw new IllegalArgumentException();
        this.random = random;
        this.symbols = symbols.toCharArray();
        this.buf = new char[length];
    }

    /**
     * Create an alphanumeric string generator.
     */
    public RandomString(int length, Random random) {
        this(length, random, alphanum);
    }

    /**
     * Create an alphanumeric strings from a secure generator.
     */
    public RandomString(int length) {
        this(length, new SecureRandom());
    }

    /**
     * Create session identifiers.
     */
    public RandomString() {
        this(21);
    }

}

  public static void consumePayload(final Blackhole blackHole, final byte[] payload) {
    for (int i = 0; i < payload.length; ) {
      if (blackHole == null) {
        System.out.print(Bytes.getLong(payload, i) + " ");
      } else {
        blackHole.consume(Bytes.getLong(payload, i));
      }
      i += 8;
      if (blackHole == null) {
        System.out.println(Double.longBitsToDouble(Bytes.getLong(payload, i)));
      } else {
        blackHole.consume(Double.longBitsToDouble(Bytes.getLong(payload, i)));
      }
      i += 8;
    }
  }
}
