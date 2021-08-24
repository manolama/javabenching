package net.opentsdb.hashmaps;

import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.opentsdb.collections.LongLongHashTable;

import java.util.HashMap;

public class LongLongHashContainers {

  static double[] OP_RATIOS = {
          0.10,
          0.35,
          0.50,
          0.05 //DELETES
  };
  static double[] DRAIN_RATIOS = {0.009, 0.05};
  static int MAP_SIZE = 3_000_000;

  public static void main(String[] args) {
    //new NativeHashMap().run();
    //new LLHashTable().run();
    new Trove().run();
  }

  static class NativeHashMap extends BaseLongLongHashTest {
    HashMap<Long, Long> map;

    NativeHashMap() {
      super(MAP_SIZE, OP_RATIOS, DRAIN_RATIOS);
      map = new HashMap<Long, Long>(size);
    }

    public int size() {
      return map.size();
    }

    @Override
    void put(long key, long val) {
      map.put(key, val);
    }

    @Override
    void delete(long key) {
      map.remove(key);
    }

    @Override
    boolean get(long key) {
      return map.get(key) != null;
    }
  }

  static class LLHashTable extends BaseLongLongHashTest {
    LongLongHashTable tbl;

    LLHashTable() {
      super(MAP_SIZE, OP_RATIOS, DRAIN_RATIOS);
      tbl = new LongLongHashTable(size, "UT");
    }

    @Override
    public int size() {
      return tbl.size();
    }

    @Override
    void put(long key, long val) {
      tbl.put(key, val);
    }

    @Override
    void delete(long key) {
      tbl.remove(key);
    }

    @Override
    boolean get(long key) {
      return tbl.get(key) != LongLongHashTable.NOT_FOUND;
    }
  }

  static class Trove extends BaseLongLongHashTest {
    TLongLongMap map;

    Trove() {
      super(MAP_SIZE, OP_RATIOS, DRAIN_RATIOS);
      map = new TLongLongHashMap(size, 0.75F);
    }

    public int size() {
      return map.size();
    }

    @Override
    void put(long key, long val) {
      map.put(key, val);
    }

    @Override
    void delete(long key) {
      map.remove(key);
    }

    @Override
    boolean get(long key) {
      return map.get(key) != map.getNoEntryValue();
    }
  }
}
