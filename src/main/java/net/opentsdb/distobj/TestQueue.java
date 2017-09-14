package net.opentsdb.distobj;

public interface TestQueue {
  public void put(DummyQuery entry);
  
  public DummyQuery pollTimed();
  
  public void close();
}
