/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress;

import akka.actor.ActorRef;

/**
 * INTERNAL API: Count-Min Sketch datastructure.
 *
 * <p>Not thread-safe.
 *
 * <p>An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 * This implementation is mostly taken and adjusted from the Apache V2 licensed project
 * `stream-lib`, located here:
 * https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/frequency/CountMinSketch.java
 */
public class CountMinSketch {

  private int depth;
  private int width;
  private long[][] table;
  private long size;
  private double eps;
  private double confidence;

  private int[] recyclableCMSHashBuckets;

  public CountMinSketch(int depth, int width, int seed) {
    if ((width & (width - 1)) != 0) {
      throw new IllegalArgumentException("width must be a power of 2, was: " + width);
    }
    this.depth = depth;
    this.width = width;
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);
    recyclableCMSHashBuckets = preallocateHashBucketsArray(depth);
    initTablesWith(depth, width, seed);
  }

  private void initTablesWith(int depth, int width, int seed) {
    this.table = new long[depth][width];
  }

  /** Referred to as {@code epsilon} in the whitepaper */
  public double relativeError() {
    return eps;
  }

  public double confidence() {
    return confidence;
  }

  /**
   * Similar to {@code add}, however we reuse the fact that the hask buckets have to be calculated
   * for {@code add} already, and a separate {@code estimateCount} operation would have to calculate
   * them again, so we do it all in one go.
   */
  public long addObjectAndEstimateCount(Object item, long count) {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    Murmur3.hashBuckets(item, recyclableCMSHashBuckets, width);
    for (int i = 0; i < depth; ++i) {
      table[i][recyclableCMSHashBuckets[i]] += count;
    }
    size += count;
    return estimateCount(recyclableCMSHashBuckets);
  }

  public long size() {
    return size;
  }

  /**
   * The estimate is correct within {@code 'epsilon' * (total item count)}, with probability {@code
   * confidence}.
   */
  public long estimateCount(Object item) {
    Murmur3.hashBuckets(item, recyclableCMSHashBuckets, width);
    return estimateCount(recyclableCMSHashBuckets);
  }

  /**
   * The estimate is correct within {@code 'epsilon' * (total item count)}, with probability {@code
   * confidence}.
   *
   * @param buckets the "indexes" of buckets from which we want to calculate the count
   */
  private long estimateCount(int[] buckets) {
    long res = Long.MAX_VALUE;
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][buckets[i]]);
    }
    return res;
  }

  /**
   * Local implementation of murmur3 hash optimized to used in count min sketch
   *
   * <p>Inspired by scala (scala.util.hashing.MurmurHash3) and C port of MurmurHash3
   *
   * <p>scala.util.hashing =>
   * https://github.com/scala/scala/blob/2.12.x/src/library/scala/util/hashing/MurmurHash3.scala C
   * port of MurmurHash3 => https://github.com/PeterScott/murmur3/blob/master/murmur3.c
   */
  private static class Murmur3 {

    /** Force all bits of the hash to avalanche. Used for finalizing the hash. */
    private static int avalanche(int hash) {
      int h = hash;

      h ^= h >>> 16;
      h *= 0x85ebca6b;
      h ^= h >>> 13;
      h *= 0xc2b2ae35;
      h ^= h >>> 16;

      return h;
    }

    private static int mixLast(int hash, int data) {
      int k = data;

      k *= 0xcc9e2d51; // c1
      k = Integer.rotateLeft(k, 15);
      k *= 0x1b873593; // c2

      return hash ^ k;
    }

    private static int mix(int hash, int data) {
      int h = mixLast(hash, data);
      h = Integer.rotateLeft(h, 13);
      return h * 5 + 0xe6546b64;
    }

    public static int hash(Object o) {
      if (o == null) {
        return 0;
      }
      if (o instanceof ActorRef) { // TODO possibly scary optimisation
        // ActorRef hashcode is the ActorPath#uid, which is a random number assigned at its
        // creation,
        // thus no hashing happens here - the value is already cached.
        // TODO it should be thought over if this preciseness (just a random number, and not
        // hashing) is good enough here?
        // this is not cryptographic one, anything which is stable and random is good enough
        return o.hashCode();
      }
      if (o instanceof String) {
        return hash(((String) o).getBytes());
      }
      if (o instanceof Long) {
        return hashLong((Long) o, 0);
      }
      if (o instanceof Integer) {
        return hashLong((Integer) o, 0);
      }
      if (o instanceof Double) {
        return hashLong(Double.doubleToRawLongBits((Double) o), 0);
      }
      if (o instanceof Float) {
        return hashLong(Float.floatToRawIntBits((Float) o), 0);
      }
      if (o instanceof byte[]) {
        return bytesHash((byte[]) o, 0);
      }
      return hash(o.toString());
    }

    static int hashLong(long value, int seed) {
      int h = seed;
      h = mix(h, (int) (value));
      h = mixLast(h, (int) (value >>> 32));
      return avalanche(h ^ 2);
    }

    static int bytesHash(final byte[] data, int seed) {
      int len = data.length;
      int h = seed;

      // Body
      int i = 0;
      while (len >= 4) {
        int k = data[i] & 0xFF;
        k |= (data[i + 1] & 0xFF) << 8;
        k |= (data[i + 2] & 0xFF) << 16;
        k |= (data[i + 3] & 0xFF) << 24;

        h = mix(h, k);

        i += 4;
        len -= 4;
      }

      // Tail
      int k = 0;
      if (len == 3) k ^= (data[i + 2] & 0xFF) << 16;
      if (len >= 2) k ^= (data[i + 1] & 0xFF) << 8;
      if (len >= 1) {
        k ^= (data[i] & 0xFF);
        h = mixLast(h, k);
      }

      // Finalization
      return avalanche(h ^ data.length);
    }

    /**
     * Hash item using pair independent hash functions.
     *
     * <p>Implementation based on "Less Hashing, Same Performance: Building a Better Bloom Filter"
     * https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
     *
     * @param item what should be hashed
     * @param hashBuckets where hashes should be placed
     * @param limit value to shrink result
     */
    static void hashBuckets(Object item, final int[] hashBuckets, int limit) {
      final int hash1 = hash(item); // specialized hash for ActorRef and Strings
      final int hash2 = hashLong(hash1, hash1);
      final int depth = hashBuckets.length;
      final int mask = limit - 1;
      for (int i = 0; i < depth; i++) {
        hashBuckets[i] =
            Math.abs(
                (hash1 + i * hash2)
                    & mask); // shrink done by AND instead MOD. Assume limit is power of 2
      }
    }
  }

  private int[] preallocateHashBucketsArray(int depth) {
    return new int[depth];
  }

  @Override
  public String toString() {
    return "CountMinSketch{"
        + "confidence="
        + confidence
        + ", size="
        + size
        + ", depth="
        + depth
        + ", width="
        + width
        + '}';
  }
}
