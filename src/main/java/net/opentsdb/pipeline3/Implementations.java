// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.pipeline3;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline3.Abstracts.*;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class MyNumeric extends NumericTSDataType {
    long[] timestamps;
    long[] integers;
    double[] doubles;
    
    public MyNumeric(TimeSeriesStringId id) {
      this.id = id;
    }
    
    @Override
    public long[] timestamps() {
      if (timestamps == null) {
        timestamps = new long[dps];
        int idx = 0;
        for (int i = 0; i < data.length; i += 16) {
          timestamps[idx++] = Bytes.getLong(data, i);
        }
      }
      return timestamps;
    }

    @Override
    public long[] integers() {
      if (integers == null) {
        integers = new long[dps];
        int idx = 0;
        for (int i = 8; i < data.length; i += 16) {
          integers[idx++] = Bytes.getLong(data, i);
        }
      }
      return integers;
    }

    @Override
    public double[] doubles() {
      if (doubles == null) {
        doubles = new double[dps];
        int idx = 0;
        for (int i = 8; i < data.length; i += 16) {
          doubles[idx++] = Double.longBitsToDouble(Bytes.getLong(data, i));
        }
      }
      return doubles;
    }

    @Override
    public boolean isIntegers() {
      return is_integers;
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }

    @Override
    protected void reset() {
      timestamps = null;
      integers = null;
      doubles = null;
    }
    
  }
  
  public static class MyString extends StringTSDataType {
    long[] timestamps;
    String[] strings;
    
    public MyString(TimeSeriesStringId id) {
      this.id = id;
    }
    
    @Override
    public long[] timestamps() {
      if (timestamps == null) {
        timestamps = new long[dps];
        int idx = 0;
        for (int i = 0; i < data.length; i += 11) {
          timestamps[idx++] = Bytes.getLong(data, i);
        }
      }
      return timestamps;
    }

    @Override
    public String[] strings() {
      if (strings == null) {
        strings = new String[dps];
        int idx = 0;
        for (int i = 8; i < data.length; i += 11) {
          byte[] s = new byte[3];
          System.arraycopy(data, i, s, 0, 3);
          strings[idx++] = new String(s, Const.UTF8_CHARSET);
        }
      }
      return strings;
    }

    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }

    @Override
    protected void reset() {
      timestamps = null;
      strings = null;
    }
    
  }
}
