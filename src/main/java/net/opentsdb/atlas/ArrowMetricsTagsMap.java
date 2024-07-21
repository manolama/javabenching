package net.opentsdb.atlas;

import com.netflix.atlas.core.model.Datapoint;
import com.netflix.spectator.impl.Hash64;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import scala.collection.JavaConverters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ArrowMetricsTagsMap {
  private BufferAllocator allocator;

  private UInt8Vector timestamps;
  private Float8Vector values;
  private MapVector tags;
  private VarCharVector tagKeys;
  private VarCharVector tagValues;

  private ArrayList<FieldVector> vectors = new ArrayList<FieldVector>();
  private int index = 0;
  private int tagsIdx = 0;
  private final int initValues = 10_000;

  public ArrowMetricsTagsMap(BufferAllocator allocator) {
    this.allocator = allocator;
    timestamps = new UInt8Vector("timestamp", allocator);
    vectors.add(timestamps);
    values = new Float8Vector("value", allocator);
    vectors.add(values);

    FieldType struct = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    tags = new MapVector("tags", allocator, new FieldType(false, new ArrowType.Map(false), null, null), null);
    vectors.add(tags);

    AddOrGetResult<StructVector> tagsChildren = tags.addOrGetVector(struct);

    FieldType keyType = new FieldType(false, new ArrowType.Utf8(), null, null);
    tagKeys = tagsChildren.getVector().addOrGet(MapVector.KEY_NAME, keyType, VarCharVector.class);
    tagKeys.allocateNew(1000000, 8160);

    FieldType valueType = new FieldType(false, new ArrowType.Utf8(), null, null);
    tagValues = tagsChildren.getVector().addOrGet(MapVector.VALUE_NAME, valueType, VarCharVector.class);
    tagValues.allocateNew(5000000, 8160);
    //vectors.add(tagValues);
  }

  public void add(Datapoint dp) {
    timestamps.setSafe(index, dp.timestamp());
    values.setSafe(index, dp.value());

    UnionMapWriter writer = tags.getWriter();
    writer.setPosition(index);
    writer.startMap();
    int st = tagsIdx;
    dp.tags().foreach(t -> {
      writer.struct().start();
      tagKeys.setSafe(tagsIdx, t._1.getBytes(StandardCharsets.UTF_8));
      tagValues.setSafe(tagsIdx, t._2.getBytes(StandardCharsets.UTF_8));
      writer.struct().end();
      tagsIdx++;
      return null;
    });
    writer.endMap();
    index++;
  }

  public void add(Reader reader) {
    timestamps.setSafe(index, reader.timestamp());
    values.setSafe(index, reader.value());
    UnionMapWriter writer = tags.getWriter();
    writer.setPosition(index);
    writer.startMap();
    while (reader.hasNextTagPair()) {
      writer.struct().start();
      try {
        tagKeys.setSafe(tagsIdx, reader.tagKeys.get(reader.tagIdx));
      } catch (OutOfMemoryException oom) {
        System.out.println("OOM at index: " + index + " tagsIdx: " + tagsIdx);
        System.out.println("Buffer size: " + tagKeys.getBufferSize() + " value count: " + tagKeys.getValueCount());
        System.out.println("Byte cap: "  + tagKeys.getByteCapacity());
        throw oom;
      }
      tagValues.setSafe(tagsIdx, reader.tagValues.get(reader.tagIdx));
      writer.struct().end();

      reader.advanceTagPair();
      tagsIdx++;
    }
    writer.endMap();
    index++;
  }

  public void mark() {
    timestamps.setValueCount(index);
    values.setValueCount(index);
    tags.setValueCount(index);
    tagKeys.setValueCount(tagsIdx);
    tagValues.setValueCount(tagsIdx);
  }

  public void close() {
    timestamps.close();
    values.close();
    tags.close();
    tagKeys.close();
    tagValues.close();
  }

  public void flush(ByteArrayOutputStream baos) {
    if (index > 0) {
      mark();
      VectorSchemaRoot schema = new VectorSchemaRoot(vectors);
//      ArrowStreamWriter writer = new ArrowStreamWriter(
//          schema,
//          null,
//          Channels.newChannel(baos),
//          IpcOption.DEFAULT,
//          CommonsCompressionFactory.INSTANCE,
//          CompressionUtil.CodecType.ZSTD
//      );
      ArrowStreamWriter writer = new ArrowStreamWriter(
          schema,
          null,
          Channels.newChannel(baos)
      );
      try {
        schema.setRowCount(index);
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        schema.close();
        writer.close();
        close();
      }
    }
  }

  public static class Reader {

    private ArrowStreamReader reader;
    private VectorSchemaRoot vectorSchemaRoot;
    UInt8Vector timestamps;
    Float8Vector values;
    MapVector tags;
    VarCharVector tagKeys;
    VarCharVector tagValues;
    int index = 0;
    int tagIdx = 0;
    int tagEnd = 0;

    public Reader(byte[] data) {
      reader = new ArrowStreamReader(Channels.newChannel(new ByteArrayInputStream(data)), Helper.allocator);
      try {
        reader.loadNextBatch();
        vectorSchemaRoot = reader.getVectorSchemaRoot();
        timestamps = (UInt8Vector) vectorSchemaRoot.getVector("timestamp");
        values = (Float8Vector) vectorSchemaRoot.getVector("value");
        tags = (MapVector) vectorSchemaRoot.getVector("tags");
        StructVector struct = (StructVector) tags.getChildrenFromFields().get(0);
        tagKeys = (VarCharVector) struct.getChild("key");
        tagValues = (VarCharVector) struct.getChild("value");

        tagIdx = tags.getElementStartIndex(index);
        tagEnd = tags.getElementEndIndex(index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean hasNext() {
      return index < timestamps.getValueCount();
    }

    public boolean advance() {
      if (index + 1 < timestamps.getValueCount()) {
        index++;
        tagIdx = tags.getElementStartIndex(index);
        tagEnd = tags.getElementEndIndex(index);
        return true;
      }
      index++;
      return false;
    }

    public long timestamp() {
      return timestamps.get(index);
    }

    public double value() {
      return values.get(index);
    }

    public Map<String, String> tags() {
      Map<String, String> map = new HashMap<>();

      int st = tagIdx;
      for (; tagIdx < tagEnd; tagIdx++) {
        if (!tags.isNull(index)) {
          String key = new String(tagKeys.get(tagIdx));
          String value = new String(tagValues.get(tagIdx));
          map.put(key, value);
        }
      }
      return map;
    }

    public boolean hasNextTagPair() {
      return tagIdx < tagEnd;
    }

    public void advanceTagPair() {
      tagIdx++;
    }

    public String tagKey() {
      return new String(tagKeys.get(tagIdx));
    }

    public String tagValue() {
      return new String(tagValues.get(tagIdx));
    }

    public void close() {
      try {
        timestamps.close();
        values.close();
        tags.close();
        tagKeys.close();
        tagValues.close();
        vectorSchemaRoot.close();
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}


