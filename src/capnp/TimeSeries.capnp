@0xf29e6d7994ad79fd;

using Java = import "java.capnp";
using TimeSeriesId = import "TimeSeriesId.capnp";

$Java.package("net.opentsdb.capnp");
$Java.outerClassname("TimeSeriesCapnp");

struct TimeSeries {
  id @0 :TimeSeriesId.TimeSeriesId;
  basetime @1 :Int64;
  encoding @2 :Int32;
  payload @3 :Data;
  timestamps @4 :List(Int64);
  values @5 :List(Float64);
}
