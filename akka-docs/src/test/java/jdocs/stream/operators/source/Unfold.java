/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import java.math.BigInteger;
import java.util.Optional;

interface Unfold {

  // #countdown
  public static Source<Integer, NotUsed> countDown(Integer from) {
    return Source.unfold(
        from,
        current -> {
          if (current == 0) return Optional.empty();
          else return Optional.of(Pair.create(current - 1, current));
        });
  }
  // #countdown

  // #fibonacci
  public static Source<BigInteger, NotUsed> fibonacci() {
    return Source.unfold(
        Pair.create(BigInteger.ZERO, BigInteger.ONE),
        current -> {
          BigInteger a = current.first();
          BigInteger b = current.second();
          Pair<BigInteger, BigInteger> next = Pair.create(b, a.add(b));
          return Optional.of(Pair.create(next, a));
        });
  }
  // #fibonacci

}
