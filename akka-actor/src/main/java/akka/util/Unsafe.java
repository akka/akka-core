/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util;

import akka.annotation.InternalApi;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** INTERNAL API */
@InternalApi
public final class Unsafe {
  public static final sun.misc.Unsafe instance;

  private static final long stringValueFieldOffset;
  private static final boolean isJavaVersion9Plus;
  private static final int copyUSAsciiStrToBytesAlgorithm;

  static {
    try {
      sun.misc.Unsafe found = null;
      for (Field field : sun.misc.Unsafe.class.getDeclaredFields()) {
        if (field.getType() == sun.misc.Unsafe.class) {
          field.setAccessible(true);
          found = (sun.misc.Unsafe) field.get(null);
          break;
        }
      }
      if (found == null) throw new IllegalStateException("Can't find instance of sun.misc.Unsafe");
      else instance = found;

      long fo;
      try {
        fo = instance.objectFieldOffset(String.class.getDeclaredField("value"));
      } catch (NoSuchFieldException nsfe) {
        // The platform's implementation of String doesn't have a 'value' field, so we have to use
        // algorithm 0
        fo = -1;
      }
      stringValueFieldOffset = fo;

      isJavaVersion9Plus = isIsJavaVersion9Plus();

      if (stringValueFieldOffset > -1) {
        // Select optimization algorithm for `copyUSAciiBytesToStr`.
        // For example algorithm 1 will fail with JDK 11 on ARM32 (Raspberry Pi),
        // and therefore algorithm 0 is selected on that architecture.
        String testStr = "abc";
        if (isJavaVersion9Plus && testUSAsciiStrToBytesAlgorithm1(testStr))
          copyUSAsciiStrToBytesAlgorithm = 1;
        else if (testUSAsciiStrToBytesAlgorithm2(testStr)) copyUSAsciiStrToBytesAlgorithm = 2;
        else copyUSAsciiStrToBytesAlgorithm = 0;
      } else
        // We know so little about the platform's String implementation that we have
        // no choice but to select algorithm 0
        copyUSAsciiStrToBytesAlgorithm = 0;
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }

  static boolean isIsJavaVersion9Plus() {
    // See Oracle section 1.5.3 at:
    // https://docs.oracle.com/javase/8/docs/technotes/guides/versioning/spec/versioning2.html
    final int[] version =
        Arrays.stream(System.getProperty("java.specification.version").split("\\."))
            .mapToInt(Integer::parseInt)
            .toArray();
    final int javaVersion = version[0] == 1 ? version[1] : version[0];
    return javaVersion > 8;
  }

  static boolean testUSAsciiStrToBytesAlgorithm0(String str) {
    try {
      byte[] bytes = new byte[str.length()];

      // copy of implementation in copyUSAciiBytesToStr
      byte[] strBytes = str.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(strBytes, 0, bytes, 0, str.length());
      // end copy

      String result = copyUSAciiBytesToStr(str.length(), bytes);
      return str.equals(result);
    } catch (Throwable all) {
      return false;
    }
  }

  static boolean testUSAsciiStrToBytesAlgorithm1(String str) {
    try {
      byte[] bytes = new byte[str.length()];

      // copy of implementation in copyUSAciiBytesToStr
      final byte[] chars = (byte[]) instance.getObject(str, stringValueFieldOffset);
      System.arraycopy(chars, 0, bytes, 0, str.length());
      // end copy

      String result = copyUSAciiBytesToStr(str.length(), bytes);
      return str.equals(result);
    } catch (Throwable all) {
      return false;
    }
  }

  static boolean testUSAsciiStrToBytesAlgorithm2(String str) {
    try {
      byte[] bytes = new byte[str.length()];

      // copy of implementation in copyUSAciiBytesToStr
      final char[] chars = (char[]) instance.getObject(str, stringValueFieldOffset);
      int i = 0;
      while (i < str.length()) {
        bytes[i] = (byte) chars[i++];
      }
      // end copy

      String result = copyUSAciiBytesToStr(str.length(), bytes);
      return str.equals(result);
    } catch (Throwable all) {
      return false;
    }
  }

  private static String copyUSAciiBytesToStr(int length, byte[] bytes) {
    char[] resultChars = new char[length];
    int i = 0;
    while (i < length) {
      // UsAscii
      resultChars[i] = (char) bytes[i];
      i += 1;
    }
    return String.valueOf(resultChars, 0, length);
  }

  public static void copyUSAsciiStrToBytes(String str, byte[] bytes) {
    if (copyUSAsciiStrToBytesAlgorithm == 1) {
      final byte[] chars = (byte[]) instance.getObject(str, stringValueFieldOffset);
      System.arraycopy(chars, 0, bytes, 0, str.length());
    } else if (copyUSAsciiStrToBytesAlgorithm == 2) {
      final char[] chars = (char[]) instance.getObject(str, stringValueFieldOffset);
      int i = 0;
      while (i < str.length()) {
        bytes[i] = (byte) chars[i++];
      }
    } else {
      byte[] strBytes = str.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(strBytes, 0, bytes, 0, str.length());
    }
  }

  public static int fastHash(String str) {
    long s0 = 391408;
    long s1 = 601258;
    int i = 0;

    if (copyUSAsciiStrToBytesAlgorithm == 1) {
      final byte[] chars = (byte[]) instance.getObject(str, stringValueFieldOffset);
      while (i < str.length()) {
        long x = s0 ^ (long) chars[i++]; // Mix character into PRNG state
        long y = s1;

        // Xorshift128+ round
        s0 = y;
        x ^= x << 23;
        y ^= y >>> 26;
        x ^= x >>> 17;
        s1 = x ^ y;
      }
    } else if (copyUSAsciiStrToBytesAlgorithm == 2) {
      final char[] chars = (char[]) instance.getObject(str, stringValueFieldOffset);
      while (i < str.length()) {
        long x = s0 ^ (long) chars[i++]; // Mix character into PRNG state
        long y = s1;

        // Xorshift128+ round
        s0 = y;
        x ^= x << 23;
        y ^= y >>> 26;
        x ^= x >>> 17;
        s1 = x ^ y;
      }
    } else {
      byte[] chars = str.getBytes(StandardCharsets.US_ASCII);
      while (i < str.length()) {
        long x = s0 ^ (long) chars[i++]; // Mix character into PRNG state
        long y = s1;

        // Xorshift128+ round
        s0 = y;
        x ^= x << 23;
        y ^= y >>> 26;
        x ^= x >>> 17;
        s1 = x ^ y;
      }
    }

    return (int) (s0 + s1);
  }
}
