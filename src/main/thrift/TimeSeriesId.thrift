namespace java net.opentsdb.thrift

struct TimeSeriesId {
  1: string myalias,
  2: list<binary> namespaces,
  3: list<binary> metrics,
  4: map<binary, binary> tags,
  5: list<binary> aggregated_tags,
  6: list<binary> aggregated_tag_values,
  7: list<binary> disjoint_tags,
  8: list<binary> disjoint_tag_values,
}