/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import static akka.pattern.Patterns.pipe;

import akka.actor.AbstractActor;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

// #backend
public class FactorialBackend extends AbstractActor {

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            Integer.class,
            n -> {
              CompletableFuture<FactorialResult> result =
                  CompletableFuture.supplyAsync(() -> factorial(n))
                      .thenApply((factorial) -> new FactorialResult(n, factorial));

              pipe(result, getContext().dispatcher()).to(getSender());
            })
        .build();
  }

  BigInteger factorial(int n) {
    BigInteger acc = BigInteger.ONE;
    for (int i = 1; i <= n; ++i) {
      acc = acc.multiply(BigInteger.valueOf(i));
    }
    return acc;
  }
}
// #backend
