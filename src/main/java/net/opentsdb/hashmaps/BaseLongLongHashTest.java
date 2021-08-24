package net.opentsdb.hashmaps;

import net.opentsdb.utils.DateTime;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Random;

public abstract class BaseLongLongHashTest {
  int size;
  int writeMax;
  int writeAgain;
  int logOps;
  long[] keys;
  int[] ops;
  int keyIdx;
  Random rnd;
  long realTime;
  int wroteHeader;

  enum OpType {
    READ_HIT,
    READ_MISS,
    WRITE,
    DELETE
  }

  class Cntrs {
    int totalOps;
    int[] opCount;
    long[] opTimeSum;
    long[] opTimeMax;
    long[] opTimeMin;

    void resetCounters() {
      totalOps = 0;
      opCount = new int[4];
      opTimeSum = new long[4];
      opTimeMax = new long[4];
      Arrays.fill(opTimeMax, Long.MIN_VALUE);
      opTimeMin = new long[4];
      Arrays.fill(opTimeMin, Long.MAX_VALUE);
    }

    void updateCounters(OpType type, long delta) {
      opCount[type.ordinal()]++;
      opTimeSum[type.ordinal()] += delta;
      if (delta > opTimeMax[type.ordinal()]) {
        opTimeMax[type.ordinal()] = delta;
      }
      if (delta < opTimeMin[type.ordinal()]) {
        opTimeMin[type.ordinal()] = delta;
      }
      totalOps++;
    }
  }
  Cntrs[] counters = new Cntrs[16];
  int cntrIdx;

  long start;
  NumberFormat nf = NumberFormat.getInstance();

  /**
   *
   * @param mapSize
   * @param ratios The ratios for each op based on their ordinal. Must sum to 100
   * @param drainRatios The first is how much space should be left before we drain
   *                    out a chunk. The second is at what point we stop draining.
   *                    E.g 0.05 to drain at 5% space remaining and drain until 0.1
   *                    so that 10% is free.
   */
  BaseLongLongHashTest(int mapSize, double[] ratios, double[] drainRatios) {
    this.size = mapSize;
    double sum = 0;
    for (int i = 0; i < ratios.length; i++) {
      sum += ratios[i];
    }
    if ((int) (sum * 100) != 100) {
      throw new IllegalStateException("Need a sum of 100 != " + (sum * 100));
    }

    writeMax = (int) (size - (size * drainRatios[0]));
    writeAgain = (int) (size - (size * drainRatios[1]));
    logOps = 250_000;
    keys = new long[size];
    keyIdx = 0;
    rnd = new Random(System.currentTimeMillis());

    ops = new int[100];
    int count = (int) (ratios[0] * 100);
    int wrote = 0;
    int type = 0;
    for (int i = 0; i < ops.length; i++) {
      ops[i] = OpType.values()[type].ordinal();
      if (++wrote >= count) {
        type++;
        if (type >= 4) {
          break;
        }
        count = (int) (ratios[type] * 100);
        wrote = 0;
      }
    }

    for (int i = 0; i < counters.length; i++) {
      counters[i] = new Cntrs();
      counters[i].resetCounters();
    }
  }

  public void run() {
    realTime = DateTime.nanoTime();
    while (true) {
      if (size() >= writeMax) {
        // drain
        for (int i = size(); i >= writeAgain; i--) {
          int idx = rnd.nextInt(keyIdx);
          long val = keys[idx];
          keys[idx] = keys[--keyIdx];

          start = DateTime.nanoTime();
          delete(val);
          long delta = DateTime.nanoTime() - start;
          updateCounters(OpType.DELETE, delta);
        }
      } else {
        OpType op = OpType.values()[ops[rnd.nextInt(ops.length)]];
        long delta = 0;

        switch (op) {
          case READ_HIT:
            if (keyIdx < 1) {
              continue;
            }
            int idx = rnd.nextInt(keyIdx);
            start = DateTime.nanoTime();
            get(keys[idx]);
            delta = DateTime.nanoTime() - start;
            break;

          case READ_MISS:
            while (true) {
              start = DateTime.nanoTime();
              long hopefullyNonExtantKey = rnd.nextLong();

              if (!get(hopefullyNonExtantKey)) {
                delta = DateTime.nanoTime() - start;
                break;
              }
            }
            break;

          case WRITE:
            long key = rnd.nextLong();
            long val = rnd.nextLong();
            keys[keyIdx++] = key;

            start = DateTime.nanoTime();
            // yeah it could exist.
            put(key, val);
            delta = DateTime.nanoTime() - start;
            break;

          case DELETE:
            if (keyIdx < 1) {
              continue;
            }
            int idx2 = rnd.nextInt(keyIdx);
            long val2 = keys[idx2];
            keys[idx2] = keys[--keyIdx];

            start = DateTime.nanoTime();
            delete(val2);
            delta = DateTime.nanoTime() - start;
            break;
        }

        updateCounters(op, delta);
      }
    }
  }

  public abstract int size();

  abstract void put(long key, long val);

  abstract void delete(long key);

  abstract boolean get(long key);

  void updateCounters(OpType type, long delta) {
    Cntrs ctrs = counters[cntrIdx];
    ctrs.updateCounters(type, delta);

    if (ctrs.totalOps >= logOps) {
      long rd = DateTime.nanoTime() - realTime;
      Cntrs agg = new Cntrs();
      agg.resetCounters();

      for (int i = 0; i < counters.length; i++) {
        Cntrs c = counters[i];
        for (int x = 0; x < OpType.values().length; x++) {
          agg.opCount[x] += c.opCount[x];
          agg.opTimeSum[x] += c.opTimeSum[x];
          if (c.opTimeMax[x] > agg.opTimeMax[x]) {
            agg.opTimeMax[x] = c.opTimeMax[x];
          }
          if (c.opTimeMin[x] < agg.opTimeMin[x]) {
            agg.opTimeMin[x] = c.opTimeMin[x];
          }
        }
      }
      
      if (wroteHeader == 0) {
        System.out.format("%10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s | %10s",
                "TT ms",
                "Records",
                "Gets",
                "Get ms",
                "Get Avg us",
                "Get Max us",
                "Miss'",
                "Miss ms",
                "Miss Avg",
                "Miss Max",
                "Puts",
                "Put ms",
                "Put Avg us",
                "Put Max us",
                "Dels",
                "Del ms",
                "Del Avg us",
                "Del Max us");
        System.out.println();
        for (int i = 0; i < (16 * 19); i++) {
          System.out.print("-");
        }
        System.out.println();

      }
      wroteHeader++;
      if (wroteHeader > 16) {
        wroteHeader = 0;
      }

      System.out.format("%10s | %10s | %10d | %10s | %10s | %10s | %10d | %10s | %10s | %10s | %10d | %10s | %10s | %10s | %10s | %10s",
              nf.format(DateTime.msFromNano(rd)),
              nf.format(size()),

              agg.opCount[0],
              nf.format(DateTime.msFromNano(agg.opTimeSum[0])),
              nf.format(((double) agg.opTimeSum[0] / (double) agg.opCount[0]) / (double) 1_000),
              agg.opTimeMax[0] > 0 ? nf.format(((double) agg.opTimeMax[0]) / (double) 1_000) : "NA",

              agg.opCount[1],
              nf.format(DateTime.msFromNano(agg.opTimeSum[1])),
              nf.format(((double) agg.opTimeSum[1] / (double) agg.opCount[1]) / (double) 1_000),
              agg.opTimeMax[1] > 0 ? nf.format(((double) agg.opTimeMax[1]) / (double) 1_000) : "NA",

              agg.opCount[2],
              nf.format(DateTime.msFromNano(agg.opTimeSum[2])),
              nf.format(((double) agg.opTimeSum[2] / (double) agg.opCount[2]) / (double) 1_000),
              agg.opTimeMax[2] > 0 ? nf.format(((double) agg.opTimeMax[2]) / (double) 1_000) : "NA",

              agg.opCount[3],
              nf.format(DateTime.msFromNano(agg.opTimeSum[3])),
              nf.format(((double) agg.opTimeSum[3] / (double) agg.opCount[3]) / (double) 1_000),
              agg.opTimeMax[3] > 0 ? nf.format(((double) agg.opTimeMax[3]) / (double) 1_000) : "NA");
      System.out.println();

//      StringBuilder buf = new StringBuilder()
//              .append(nf.format(DateTime.msFromNano(agg.opTimeSum)))
//              .append("ms/")
//              .append(nf.format(((double) agg.opTimeSum / (double) agg.opCount) / (double) 1_000))
//              .append("/")
//              .append(nf.format(((double) agg.opTimeMax) / (double) 1_000))
//              .append("/")
//              .append(nf.format(((double) agg.opTimeMin) / (double) 1_000))
//              .append(" (sum/avg/max/min op time) ")
//              .append(nf.format(agg.scans))
//              .append("/")
//              .append(nf.format(((double) agg.scans / (double) agg.opCount)))
//              .append("/")
//              .append(nf.format(agg.maxScans))
//              .append("/")
//              .append(nf.format(agg.minScans))
//              .append(" (sum/avg/max/min scans) Table Size ")
//              .append(nf.format(size()))
//              .append("  Real Time ")
//              .append(nf.format(DateTime.msFromNano(rd)))
//              .append("ms");
//      System.out.println(buf.toString());


      // roll
      if (++cntrIdx >= counters.length) {
        cntrIdx = 0;
      }
      counters[cntrIdx].resetCounters();
      realTime = DateTime.nanoTime();
    }
  }

}
