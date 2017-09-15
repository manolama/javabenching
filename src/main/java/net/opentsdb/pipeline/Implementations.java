package net.opentsdb.pipeline;

import java.util.Iterator;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.MyTS;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class ArrayBackedLongTS extends MyTS<NumericType> implements Iterator<TimeSeriesValue<NumericType>> {
    
    TimeStamp ts = new MillisecondTimeStamp(0);
    MutableNumericType dp;
    
    public ArrayBackedLongTS(final TimeSeriesId id) {
      super(id);
      dp = new MutableNumericType(id);
    }
    
    @Override
    public Iterator<TimeSeriesValue<NumericType>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return idx < dps.length;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      ts.updateMsEpoch(Bytes.getLong(dps, idx));
      idx += 8;
      dp.reset(ts, Bytes.getLong(dps, idx), 1);
      idx += 8;
      return dp;
    }
  }
  
}
