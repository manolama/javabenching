package net.opentsdb.atlas;

import com.netflix.atlas.core.model.Datapoint;
import com.netflix.spectator.impl.Hash64;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class ArrowMetricsTagsMapWithDict {
  private BufferAllocator allocator;

  private UInt8Vector timestamps;
  private Float8Vector values;
  private MapVector tags;
  private Dict tagKeyDict;
  private UInt4Vector tagKeys;
  private Dict tagValueDict;
  private UInt4Vector tagValues;

  private ArrayList<FieldVector> vectors = new ArrayList<FieldVector>();
  private int index = 0;
  private int tagsIdx = 0;
  private final int initValues = 10_000;

  public ArrowMetricsTagsMapWithDict(BufferAllocator allocator) {
    this.allocator = allocator;
    timestamps = new UInt8Vector("timestamp", allocator);
    timestamps.allocateNew(initValues);
    vectors.add(timestamps);
    values = new Float8Vector("value", allocator);
    values.allocateNew(initValues);
    vectors.add(values);

    FieldType struct = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    tags = new MapVector("tags", allocator, new FieldType(false, new ArrowType.Map(false), null, null), null);
    tags.allocateNew();
    vectors.add(tags);

    AddOrGetResult<StructVector> tagsChildren = tags.addOrGetVector(struct);
    FieldType keyType = new FieldType(false, new ArrowType.Int(32, false), null, null);
    FieldType valueType = new FieldType(false, new ArrowType.Int(32, false), null, null);

    tagKeys = tagsChildren.getVector().addOrGet(MapVector.KEY_NAME, keyType, UInt4Vector.class);
    tagKeys.allocateNew(initValues);
    VarCharVector kd = tagsChildren.getVector().addOrGet(MapVector.KEY_NAME + "_dict", new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), VarCharVector.class);
    tagKeyDict = new Dict("tag_key", allocator, kd);
    tagKeyDict.setVector(tagKeys);

    tagValues = tagsChildren.getVector().addOrGet(MapVector.VALUE_NAME, valueType, UInt4Vector.class);
    tagValues.allocateNew(initValues);
    kd = tagsChildren.getVector().addOrGet(MapVector.VALUE_NAME + "_dict", new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), VarCharVector.class);
    tagValueDict = new Dict("tag_value", allocator, kd);
    tagValueDict.setVector(tagValues);
  }

  public void add(Datapoint dp) {
    timestamps.setSafe(index, dp.timestamp());
    values.setSafe(index, dp.value());

    UnionMapWriter writer = tags.getWriter();
    writer.setPosition(index);
    writer.startMap();
    dp.tags().foreach(t -> {
      writer.struct().start();
      tagKeyDict.add(t._1, tagsIdx);
      tagValueDict.add(t._2, tagsIdx);
      writer.struct().end();
      tagsIdx++;
      return null;
    });
    writer.endMap();
    index++;
  }

  public void mark() {
    timestamps.setValueCount(index);
    values.setValueCount(index);
    tags.setValueCount(index);
    tagKeys.setValueCount(tagsIdx);
    tagValues.setValueCount(tagsIdx);
    tagKeyDict.mark();
    tagValueDict.mark();
  }

  public void close() {
    tagKeyDict.close();
    tagValueDict.close();
    timestamps.close();
    values.close();
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
        writer.start();
        schema.setRowCount(index);
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        writer.close();
        schema.close();
        close();
      }
    }
  }

  class Dict {
    private String name;
    private BufferAllocator allocator;
    private VarCharVector dictionary;
    private int dictIndex = 0;
    private java.util.Map<Long, Integer> dictMap = new HashMap<>();
    private Hash64 hasher = new Hash64();
    private UInt4Vector vector;

    public Dict(String name, BufferAllocator allocator, VarCharVector dv) {
      this.name = name;
      this.allocator = allocator;
      this.dictionary = dv != null ? dv : new VarCharVector(name + "_dict", allocator);
    }

    public int getIndex(String value) {
      hasher.updateString(value);
      long hash = hasher.computeAndReset();
      Integer index = dictMap.get(hash);
      if (index == null) {
        dictMap.put(hash, dictIndex);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        dictionary.setSafe(dictIndex, bytes);
        return dictIndex++;
      } else {
        return index;
      }
    }

    public void setVector(UInt4Vector vector) {
      this.vector = vector;
    }

    public void add(String value, int index) {
      int idx = getIndex(value);
      vector.setSafe(index, idx);
    }

    public void addNull(int index) {
      vector.setNull(index);
    }

    public void mark() {
      dictionary.setValueCount(dictIndex);
    }

    public void close() {
      dictionary.close();
    }

    public void realloc() {
      dictIndex = 0;
      dictionary.reset();
      dictMap.clear();
      if (vector != null) {
        vector.reset();
      }
    }
  }
}


