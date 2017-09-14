package net.opentsdb.distobj;

import java.io.Serializable;

public class DummyQuery implements Comparable<DummyQuery>, Serializable {
  private static final long serialVersionUID = -233261663261005035L;
  public long priority;

  public DummyQuery() { }
  
  public long getPriority() {
    return priority;
  }
  
  public void setPriority(final long priority) {
    this.priority = priority;
  }
  
  @Override
  public int compareTo(DummyQuery o) {
    return (int) (priority - o.priority);
  }
}
