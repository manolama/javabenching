package net.opentsdb.atlas;

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.Lists;
import com.netflix.atlas.core.model.Datapoint;
import com.netflix.atlas.json.Json;
import com.netflix.atlas.webapi.PublishPayloads;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.mutable.Builder;

import java.io.*;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

public class AtlasParsing {

  static byte[] TXT_CTS = new byte[] { '<', 'C', 'T', 'S', '>'};
  static ThreadLocal<byte[]> TL_BUFS = ThreadLocal.withInitial(() -> new byte[4096] );

  @org.openjdk.jmh.annotations.State(Scope.Thread)
  public static class Context {
    byte[] smile;
    byte[] smileGZ;
    byte[] json;
    byte[] jsonGZ;
    byte[] text;
    byte[] textGZ;

    public Context() {
      try {
        FileInputStream fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/smile_515872406");
        smile = fis.readAllBytes();
        fis.close();

        fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/smile_515872406.gz");
        smileGZ = fis.readAllBytes();
        fis.close();

        fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/json_515872406");
        json = fis.readAllBytes();
        fis.close();

        fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/json_515872406.gz");
        jsonGZ = fis.readAllBytes();
        fis.close();

        fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/text_515872406");
        text = fis.readAllBytes();
        fis.close();

        fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/text_515872406.gz");
        textGZ = fis.readAllBytes();
        fis.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasSmile(Context context, Blackhole blackHole) {
    JsonParser p = Json.newSmileParser(new ByteArrayInputStream(context.smile));
    List<Datapoint> datapointList = PublishPayloads.decodeBatchDatapoints(p);
    assert(datapointList.size() == 8610);
    blackHole.consume(datapointList);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasSmileGzipped(Context context, Blackhole blackHole) {
    JsonParser p = null;
    try {
      p = Json.newSmileParser(new GZIPInputStream(new ByteArrayInputStream(context.smileGZ)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    List<Datapoint> datapointList = PublishPayloads.decodeBatchDatapoints(p);
    assert(datapointList.size() == 8610);
    blackHole.consume(datapointList);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasJson(Context context, Blackhole blackHole) {
    JsonParser p = null;
    try {
      p = JSON.getMapper().getFactory().createParser(new ByteArrayInputStream(context.json));
    } catch (IOException e) {
      e.printStackTrace();
    }
    List<Datapoint> datapointList = PublishPayloads.decodeBatchDatapoints(p);
    assert(datapointList.size() == 8610);
    blackHole.consume(datapointList);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasJsonGzipped(Context context, Blackhole blackHole) {
    JsonParser p = null;
    try {
      p = JSON.getMapper().getFactory().createParser(new GZIPInputStream(new ByteArrayInputStream(context.jsonGZ)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    List<Datapoint> datapointList = PublishPayloads.decodeBatchDatapoints(p);
    assert(datapointList.size() == 8610);
    blackHole.consume(datapointList);
  }

  enum State {
    TAG_KEY,
    TAG_VALUE,
    TIMESTAMP,
    VALUE
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasText(Context context, Blackhole blackHole) {
    try {
      ByteArrayInputStream buf = new ByteArrayInputStream(context.text);
      ArrayList<Datapoint> dps = Lists.newArrayList();

      byte[] temp = new byte[4096];
      buf.read(temp, 0, 5);

      String key = null;

      scala.collection.mutable.Map commonTags = new
          scala.collection.mutable.HashMap<String, String>();
      int read = 0;
      int idx = 0;
      int mrk = 0;
      if (Bytes.memcmp(temp, TXT_CTS, 0, 5) == 0) {
        buf.read(); // skip space
        // common tags line
        read = buf.read(temp);
        while (idx < read && temp[idx] != 0x0A) {
          if (temp[idx++] == '=') {
            key = new String(temp, mrk, idx - 2 - mrk);
            mrk = idx;
          } else if (temp[idx++] == ' ') {
            commonTags.put(key, new String(temp, mrk, idx - 2 - mrk));
            mrk = idx;
          } else {
            idx++;
          }
        }
      }
      // flush final tag
      commonTags.put(key, new String(temp, mrk, idx - 2 - mrk));
      mrk = idx;
      System.out.println(commonTags);
//      // now on to the DPs.
//      Builder<Tuple2<String, String>, Map<String, String>> bldr = Map.newBuilder();
//      long ts = 0;
//      double v = 0;
//      boolean parsedVal = false;
//      while ((read = buf.read()) >= 0) {
//        if (read == 0x0A) {
//          // new line so flush, would have a tag value left so handle that
//          bldr.$plus$eq(new Tuple2<>(key, sb.toString()));
//          sb.setLength(0);
//
//          Iterator<Tuple2<String, String>> ctIt = commonTags.iterator();
//          while (ctIt.hasNext()) {
//            Tuple2<String, String> entry = ctIt.next();
//            bldr.$plus$eq(entry);
//          }
//
//          Datapoint dp = new Datapoint(bldr.result(), ts, v, 60L);
//          dps.add(dp);
//          bldr = Map.newBuilder();
//          ts = 0;
//          v = 0;
//          parsedVal = false;
//        } else {
//          if (ts == 0) {
//            if (read == ' ') {
//              ts = Long.parseLong(sb.toString());
//              sb.setLength(0);
//            } else {
//              sb.append((char) read);
//            }
//          } else if (!parsedVal) {
//            if (read == ' ') {
//              v = Double.parseDouble(sb.toString());
//              sb.setLength(0);
//              parsedVal = true;
//            } else {
//              sb.append((char) read);
//            }
//          } else {
//            // tags
//            if (read == '=') {
//              key = sb.toString();
//              sb.setLength(0);
//            } else if (read == ' ') {
//              bldr.$plus$eq(new Tuple2<>(key, sb.toString()));
//              sb.setLength(0);
//            } else {
//              sb.append((char) read);
//            }
//          }
//
//        }
//      }
//
//      assert (dps.size() == 8610);
//      blackHole.consume(dps);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void atlasTextGzipped(Context context, Blackhole blackHole) {
    InputStream buf = null;
    try {
      buf = new GZIPInputStream(new ByteArrayInputStream(context.textGZ));
      ArrayList<Datapoint> dps = Lists.newArrayList();

      byte[] txt = TL_BUFS.get();
      buf.read(txt, 0, 5);
      StringBuilder sb = new StringBuilder();
      String key = null;
      //Map<String, String> commonTags = Maps.newHashMap();

      scala.collection.mutable.Map commonTags = new
          scala.collection.mutable.HashMap<String, String>();
      int read = 0;
      if (Bytes.memcmp(txt, TXT_CTS, 0, 5) == 0) {
        buf.read(); // skip space
        // common tags line
        while ((read = buf.read()) != 0x0A) {
          if (read == '=') {
            key = sb.toString();
            sb.setLength(0);
          } else if (read == ' ') {
            commonTags.put(key, sb.toString());
            sb.setLength(0);
          } else {
            sb.append((char) read);
          }
        }
      }
      // flush final tag
      commonTags.put(key, sb.toString());
      sb.setLength(0);

      // now on to the DPs.
      Builder<Tuple2<String, String>, Map<String, String>> bldr = Map.newBuilder();
      long ts = 0;
      double v = 0;
      boolean parsedVal = false;
      while ((read = buf.read()) >= 0) {
        if (read == 0x0A) {
          // new line so flush, would have a tag value left so handle that
          bldr.$plus$eq(new Tuple2<>(key, sb.toString()));
          sb.setLength(0);

          Iterator<Tuple2<String, String>> ctIt = commonTags.iterator();
          while (ctIt.hasNext()) {
            Tuple2<String, String> entry = ctIt.next();
            bldr.$plus$eq(entry);
          }

          Datapoint dp = new Datapoint(bldr.result(), ts, v, 60L);
          dps.add(dp);
          bldr = Map.newBuilder();
          ts = 0;
          v = 0;
          parsedVal = false;
        } else {
          if (ts == 0) {
            if (read == ' ') {
              ts = Long.parseLong(sb.toString());
              sb.setLength(0);
            } else {
              sb.append((char) read);
            }
          } else if (!parsedVal) {
            if (read == ' ') {
              v = Double.parseDouble(sb.toString());
              sb.setLength(0);
              parsedVal = true;
            } else {
              sb.append((char) read);
            }
          } else {
            // tags
            if (read == '=') {
              key = sb.toString();
              sb.setLength(0);
            } else if (read == ' ') {
              bldr.$plus$eq(new Tuple2<>(key, sb.toString()));
              sb.setLength(0);
            } else {
              sb.append((char) read);
            }
          }

        }
      }

      assert(dps.size() == 8610);
      blackHole.consume(dps);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
