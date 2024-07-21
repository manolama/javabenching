package net.opentsdb.atlas;

import com.fasterxml.jackson.core.JsonGenerator;
import com.netflix.atlas.core.model.Datapoint;
import com.netflix.atlas.json.Json;
import com.netflix.atlas.webapi.PublishPayloads;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;
import scala.collection.immutable.List;

import java.io.*;

public class AtlasWrite {

  @org.openjdk.jmh.annotations.State(Scope.Thread)
  public static class Context {
    List<Datapoint> datapoints;
    public Context() {
      datapoints = Helper.getTestData();
    }
  }

  //@Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeAtlasJson(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newJsonGenerator(baos)) {
      PublishPayloads.encodeBatchDatapoints(json, Helper.commonTags, context.datapoints);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    blackhole.consume(baos.toByteArray());
  }

  //@Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeAtlasSmile(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newSmileGenerator(baos)) {
      PublishPayloads.encodeBatchDatapoints(json, Helper.commonTags, context.datapoints);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    blackhole.consume(baos.toByteArray());
  }

  //@Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeNaiveJson(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newJsonGenerator(baos)) {
      json.writeStartObject();
      json.writeObjectFieldStart("tags");
      json.writeEndObject();

      json.writeArrayFieldStart("metrics");
      context.datapoints.foreach(dp -> {
        try {
          json.writeStartObject();
          json.writeNumberField("timestamp", dp.timestamp());
          json.writeNumberField("value", dp.value());
          json.writeFieldName("tags");
          json.writeStartObject();
          dp.tags().foreach(t -> {
            try {
              json.writeStringField(t._1, t._2);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          });
          json.writeEndObject();
          json.writeEndObject();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      });
      json.writeEndArray();
      json.writeEndObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    blackhole.consume(baos.toByteArray());

  }

  //@Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeNaiveSmile(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newSmileGenerator(baos)) {
      json.writeStartObject();
      json.writeObjectFieldStart("tags");
      json.writeEndObject();

      json.writeArrayFieldStart("metrics");
      context.datapoints.foreach(dp -> {
        try {
          json.writeStartObject();
          json.writeNumberField("timestamp", dp.timestamp());
          json.writeNumberField("value", dp.value());
          json.writeFieldName("tags");
          json.writeStartObject();
          dp.tags().foreach(t -> {
            try {
              json.writeStringField(t._1, t._2);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          });
          json.writeEndObject();
          json.writeEndObject();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      });
      json.writeEndArray();
      json.writeEndObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    blackhole.consume(baos.toByteArray());
  }

  //@Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeProtobuf(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    AtlasMetrics.Datapoints.Builder dps = AtlasMetrics.Datapoints.newBuilder();
    context.datapoints.foreach(dp -> {
      AtlasMetrics.Datapoint.Builder b = AtlasMetrics.Datapoint.newBuilder()
          .setTimestamp(dp.timestamp())
          .setValue(dp.value());
      dp.tags().foreach(t -> b.putTags(t._1, t._2));
      dps.addDatapoints(b);
      return null;
    });
    try {
      dps.build().writeTo(baos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    blackhole.consume(baos.toByteArray());
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeArrowMapDict(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    ArrowMetricsTagsMapWithDict w = new ArrowMetricsTagsMapWithDict(Helper.allocator);
    context.datapoints.foreach(dp -> {
      w.add(dp);
      return null;
    });
    w.flush(baos);
    blackhole.consume(baos.toByteArray());
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeArrowMap(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    ArrowMetricsTagsMap w = new ArrowMetricsTagsMap(Helper.allocator);
    context.datapoints.foreach(dp -> {
      w.add(dp);
      return null;
    });
    w.flush(baos);
    blackhole.consume(baos.toByteArray());
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void writeArrowSingleList(Context context, Blackhole blackhole) {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    ArrowMetricsTagsSingleList w = new ArrowMetricsTagsSingleList(Helper.allocator);
    context.datapoints.foreach(dp -> {
      w.add(dp);
      return null;
    });
    w.flush(baos);
    blackhole.consume(baos.toByteArray());
  }
}
