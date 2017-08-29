@0xbbc5369f58b5b8ae;

using Java = import "java.capnp";

$Java.package("net.opentsdb.capnp");
$Java.outerClassname("TimeSeriesIdCapnp");

struct TimeSeriesId {
  alias @0 :Text;
  namespaces @1 :List(Data);
  metrics @2 :List(Data);
  tagKeys @3 :List(Data);
  tagValues @4 :List(Data);
  aggregatedTags @5 :List(Data);
  aggregatedTagValues @6 :List(Data);
  disjointTags @7 :List(Data);
  disjointTagValues @8 :List(Data);
}