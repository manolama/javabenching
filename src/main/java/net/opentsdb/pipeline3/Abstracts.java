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

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline3.Interfaces.*;

public class Abstracts {

  public static abstract class NumericTSDataType implements TS<NumericType> {
    protected TimeSeriesId id;
    protected byte[] data;
    protected int dps;
    protected boolean is_integers = false;
    
    public void setData(byte[] data, int dps, boolean is_integers) {
      this.data = data;
      this.dps = dps;
      this.is_integers = is_integers;
      reset();
    }
    
    public abstract long[] timestamps();
    public abstract long[] integers();
    public abstract double[] doubles();
    public abstract boolean isIntegers();
    public TimeSeriesId id() {
      return id;
    }
    protected abstract void reset();
  }
  
  
  public static abstract class StringTSDataType implements TS<StringType> {
    protected TimeSeriesId id;
    protected byte[] data;
    protected int dps;
    
    public void setData(byte[] data, int dps) {
      this.data = data;
      this.dps = dps;
      reset();
    }
    
    public abstract long[] timestamps();
    public abstract String[] strings();
    public TimeSeriesId id() {
      return id;
    }
    protected abstract void reset();
  }
  
  public static abstract class StringType implements TimeSeriesDataType {
    /** The data type reference to pass around. */
    public static final TypeToken<StringType> TYPE = 
        TypeToken.of(StringType.class);
  }
}
