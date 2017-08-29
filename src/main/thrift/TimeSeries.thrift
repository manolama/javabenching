include "TimeSeriesId.thrift"

namespace java net.opentsdb.thrift

struct TimeSeries {
  1: TimeSeriesId.TimeSeriesId tsid,
  2: i64 basetime,
  3: i32 encoding,
  4: binary payload,
  5: list<i64> timestamps,
  6: list<double> values,
}