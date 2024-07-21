package net.opentsdb.atlas;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.atlas.core.model.Datapoint;
import com.netflix.atlas.json.Json;
import com.netflix.atlas.webapi.PublishPayloads;
import com.netflix.spectator.impl.Hash64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;
import scala.collection.immutable.List;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AtlasSharding {

  static int buckets = 64;

  @org.openjdk.jmh.annotations.State(Scope.Thread)
  public static class Context {
    byte[] jsonData = Helper.encodeTestDataAsJson();
    byte[] smileData = Helper.encodeTestDataAsSmile();
    byte[] pbData = Helper.encodeTestDataAsProtobuf();
    byte[] arrowMapData = Helper.encodeTestDataAsArrowMap();
    ThreadLocal<Hash64> hashers = ThreadLocal.withInitial(Hash64::new);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void shardAtlasJsonAsList(Context context, Blackhole blackhole) {
    List<Datapoint> dps;
    try (JsonParser p = Json.newJsonParser(new ByteArrayInputStream(context.jsonData))) {
      dps = PublishPayloads.decodeBatchDatapoints(p);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<Long, AtlasPayloadWriter> shards = new java.util.HashMap<>();
    dps.foreach((dp) -> {
      long key = hash(context, dp) % buckets;
      AtlasPayloadWriter writer = shards.get(key);
      if (writer == null) {
        writer = new AtlasPayloadWriter(false);
        shards.put(key, writer);
      }
      writer.writeDP(dp);
      return null;
    });

    shards.forEach((k, v) -> {
      blackhole.consume(v.serialize());
    });
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void shardAtlasSmileAsList(Context context, Blackhole blackhole) {
    List<Datapoint> dps;
    try (JsonParser p = Json.newSmileParser(new ByteArrayInputStream(context.smileData))) {
      dps = PublishPayloads.decodeBatchDatapoints(p);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<Long, AtlasPayloadWriter> shards = new HashMap<>();
    dps.foreach((dp) -> {
      long key = hash(context, dp) % buckets;
      AtlasPayloadWriter writer = shards.get(key);
      if (writer == null) {
        writer = new AtlasPayloadWriter(true);
        shards.put(key, writer);
      }
      writer.writeDP(dp);
      return null;
    });

    shards.forEach((k, v) -> {
      blackhole.consume(v.serialize());
    });
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void shardAtlasProtobuf(Context context, Blackhole blackhole) {
    Map<Long, AtlasMetrics.Datapoints.Builder> shards = new HashMap<>();

    try {
      AtlasMetrics.Datapoints dps = AtlasMetrics.Datapoints.parseFrom(context.pbData);
      for (int i = 0; i < dps.getDatapointsCount(); i++) {
        AtlasMetrics.Datapoint dp = dps.getDatapoints(i);
        long key = hash(context, dp) % buckets;
        AtlasMetrics.Datapoints.Builder builder = shards.get(key);
        if (builder == null) {
          builder = AtlasMetrics.Datapoints.newBuilder();
          shards.put(key, builder);
        }
        builder.addDatapoints(dp);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    shards.forEach((k, v) -> {
      ByteArrayOutputStream baos = Helper.buffers.get();
      baos.reset();
      try {
        v.build().writeTo(baos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      blackhole.consume(baos.toByteArray());
    });
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void shardAtlasArrowMap(Context context, Blackhole blackhole) {
    Map<Long, ArrowMetricsTagsMap> shards = new HashMap<>();
    ArrowMetricsTagsMap.Reader reader = new ArrowMetricsTagsMap.Reader(context.arrowMapData);
    while (reader.hasNext()) {
      long key = hash(context, reader) % buckets;
      ArrowMetricsTagsMap writer = shards.get(key);
      if (writer == null) {
        writer = new ArrowMetricsTagsMap(Helper.allocator);
        shards.put(key, writer);
      }

      writer.add(reader);

      reader.advance();
    }
    reader.close();

    shards.forEach((k, v) -> {
      ByteArrayOutputStream baos = Helper.buffers.get();
      baos.reset();
      v.flush(baos);
      blackhole.consume(baos.toByteArray());
      v.close();
    });
  }

  static long hash(Context context, Datapoint dp) {
    Hash64 hasher = context.hashers.get();
    dp.tags().foreach((tuple) -> {
      hasher.updateString(tuple._1);
      hasher.updateString(tuple._2);
      return null;
    });
    return hasher.computeAndReset();
  }

  static long hash(Context context, AtlasMetrics.Datapoint dp) {
    Hash64 hasher = context.hashers.get();
    dp.getTagsMap().forEach((k, v) -> {
      hasher.updateString(k);
      hasher.updateString(v);
    });
    return hasher.computeAndReset();
  }

  static long hash(Context context, ArrowMetricsTagsMap.Reader reader) {
    Hash64 hasher = context.hashers.get();
    for (int i = reader.tagIdx; i < reader.tagEnd; i++) {
      hasher.updateBytes(reader.tagKeys.get(i));
      hasher.updateBytes(reader.tagValues.get(i));
    }
    return hasher.computeAndReset();
  }

  static class AtlasPayloadWriter {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json;

    AtlasPayloadWriter(boolean smile) {
      if (smile) {
        json = Json.newSmileGenerator(baos);
      } else {
        json = Json.newJsonGenerator(baos);
      }

      try {
        json.writeStartObject();
        json.writeArrayFieldStart("metrics");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void writeDP(Datapoint dp) {
      try {
        json.writeStartObject();
        json.writeNumberField("timestamp", dp.timestamp());
        json.writeNumberField("value", dp.value());
        json.writeObjectFieldStart("tags");
        dp.tags().foreach((tuple) -> {
          try {
            json.writeStringField(tuple._1, tuple._2);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return null;
        });
        json.writeEndObject();
        json.writeEndObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void startNextMetric() {
      try {
        json.writeStartObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void writeValue(double value) {
      try {
        json.writeNumberField("value", value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void writeTimestamp(long timestamp) {
      try {
        json.writeNumberField("timestamp", timestamp);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void writeTags(Map<String, String> tags) {
      try {
        json.writeFieldName("tags");
        json.writeStartObject();
        tags.forEach((k, v) -> {
          try {
            json.writeStringField(k, v);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
        json.writeEndObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    void finishMetric() {
      try {
        json.writeEndObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    byte[] serialize() {
      try {
        json.writeEndArray();
        json.writeEndObject();
        json.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }
}
