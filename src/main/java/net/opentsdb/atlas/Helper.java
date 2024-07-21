package net.opentsdb.atlas;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.atlas.core.model.Datapoint;
import com.netflix.atlas.json.Json;
import com.netflix.atlas.webapi.PublishPayloads;
import org.apache.arrow.memory.RootAllocator;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Helper {
  private Helper() {
    // do not instantiate me!!!
  }

  public static ThreadLocal<ByteArrayOutputStream> buffers =
      ThreadLocal.withInitial(ByteArrayOutputStream::new);

//  public static ThreadLocal<RootAllocator> allocators =
//      ThreadLocal.withInitial(() -> new RootAllocator(536_870_912));

  public static RootAllocator allocator = new RootAllocator(536_870_912L * 4);

  public static Map<String, String> commonTags = scala.collection.immutable.Map$.MODULE$.<String,String>empty();

  public static List<Datapoint> getTestData() {
    try (FileInputStream fis = new FileInputStream("/Users/clarsen/Documents/netflix/personal/iepscratch/payloads/smile_515872406")) {
      byte[] smile = fis.readAllBytes();
      try (JsonParser p = Json.newSmileParser(new ByteArrayInputStream(smile))) {
        return PublishPayloads.decodeBatchDatapoints(p);
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encodeTestDataAsJson() {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newJsonGenerator(baos)) {
      PublishPayloads.encodeBatchDatapoints(json, Helper.commonTags, getTestData());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  public static byte[] encodeTestDataAsSmile() {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    try (JsonGenerator json = Json.newSmileGenerator(baos)) {
      PublishPayloads.encodeBatchDatapoints(json, Helper.commonTags, getTestData());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  public static byte[] encodeTestDataAsProtobuf() {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    AtlasMetrics.Datapoints.Builder dps = AtlasMetrics.Datapoints.newBuilder();
    getTestData().foreach(dp -> {
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
    return baos.toByteArray();
  }

  public static byte[] encodeTestDataAsArrowMap() {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    ArrowMetricsTagsMap w = new ArrowMetricsTagsMap(allocator);
    getTestData().foreach(dp -> {
      w.add(dp);
      return null;
    });
    w.flush(baos);
    w.close();
    return baos.toByteArray();
  }

  public static byte[] encodeTestDataAsArrowLists() {
    ByteArrayOutputStream baos = Helper.buffers.get();
    baos.reset();
    ArrowMetricsTagsLists w = new ArrowMetricsTagsLists(allocator);
    getTestData().foreach(dp -> {
      w.add(dp);
      return null;
    });
    w.flush(baos);
    w.close();
    return baos.toByteArray();
  }

  public static void main(String[] args) {
    System.out.println("Validating protobuf...");
    List<Datapoint> testDps = getTestData();
    int read = 0;
    try {
      AtlasMetrics.Datapoints datapoints = AtlasMetrics.Datapoints.parseFrom(encodeTestDataAsProtobuf());
      for (int i = 0; i < datapoints.getDatapointsCount(); i++) {
        AtlasMetrics.Datapoint dp = datapoints.getDatapoints(i);
        Datapoint testDp = testDps.apply(i);
        if (dp.getTimestamp() != testDp.timestamp()) {
          throw new IllegalStateException("Incorrect timestamps @" + i);
        }
        if (dp.getValue() != testDp.value()) {
          throw new IllegalStateException("Incorrect values @" + i);
        }
        if (!dp.getTagsMap().equals(JavaConverters.asJava(testDp.tags()))) {
          throw new IllegalStateException("Incorrect tags @" + i);
        }
        read++;
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Protobuf is valid! " + read);

    System.out.println("-----------------------------------------");
    System.out.println("Validating Arrow Map...");
    byte[] arrowMap = encodeTestDataAsArrowMap();
    ArrowMetricsTagsMap.Reader reader = new ArrowMetricsTagsMap.Reader(arrowMap);
    int i = 0;
    while (reader.hasNext()) {
      Datapoint testDp = testDps.apply(i);
      if (reader.timestamp() != testDp.timestamp()) {
        throw new IllegalStateException("Incorrect timestamps@" + i);
      }
      if (reader.value() != testDp.value()) {
        throw new IllegalStateException("Incorrect values @" + i);
      }
      java.util.Map<String, String> arrowTags = reader.tags();
      if (!arrowTags.equals(JavaConverters.asJava(testDp.tags()))) {
        System.out.println("Arrow: \n" + new TreeMap(arrowTags));
        System.out.println("Test: \n" + new TreeMap(JavaConverters.asJava(testDp.tags())));
        throw new IllegalStateException("Incorrect tags @" + i);
      }
      i++;
      reader.advance();
    }
    reader.close();
    System.out.println("Arrow Map is valid! " + i);
    allocator.close();
  }
}
