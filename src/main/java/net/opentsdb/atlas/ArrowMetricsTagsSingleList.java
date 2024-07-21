package net.opentsdb.atlas;

import com.netflix.atlas.core.model.Datapoint;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ArrowMetricsTagsSingleList {
  private BufferAllocator allocator;

  private UInt8Vector timestamps;
  private Float8Vector values;
  private ListVector tags;
  private VarCharVector tagStrings;

  private ArrayList<FieldVector> vectors = new ArrayList<FieldVector>();
  private int index = 0;
  private int tagsIdx = 0;
  private final int initValues = 10_000;

  public ArrowMetricsTagsSingleList(BufferAllocator allocator) {
    this.allocator = allocator;
    timestamps = new UInt8Vector("timestamp", allocator);
    timestamps.allocateNew(initValues);
    vectors.add(timestamps);
    values = new Float8Vector("value", allocator);
    values.allocateNew(initValues);
    vectors.add(values);

    FieldType struct = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    tags = new ListVector("tags", allocator, new FieldType(false, new ArrowType.List(), null, null), null);
    tags.allocateNew();
    vectors.add(tags);

    AddOrGetResult<StructVector> tagsChildren = tags.addOrGetVector(struct);
    FieldType stringType = new FieldType(false, new ArrowType.Utf8(), null, null);

    tagStrings = tagsChildren.getVector().addOrGet("tagStrings", stringType, VarCharVector.class);
    tagStrings.allocateNew(initValues);
    vectors.add(tagStrings);
  }

  public void add(Datapoint dp) {
    timestamps.setSafe(index, dp.timestamp());
    values.setSafe(index, dp.value());

    UnionListWriter writer = tags.getWriter();
    writer.setPosition(index);
    writer.startList();
    dp.tags().foreach(t -> {
      writer.struct().start();
      tagStrings.setSafe(tagsIdx++, t._1.getBytes(StandardCharsets.UTF_8));
      tagStrings.setSafe(tagsIdx, t._2.getBytes(StandardCharsets.UTF_8));
      writer.struct().end();
      tagsIdx++;
      return null;
    });
    writer.endList();
    index++;
  }

  public void mark() {
    timestamps.setValueCount(index);
    values.setValueCount(index);
    tags.setValueCount(index);
  }

  public void close() {
    timestamps.close();
    values.close();
    tags.close();
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
}


