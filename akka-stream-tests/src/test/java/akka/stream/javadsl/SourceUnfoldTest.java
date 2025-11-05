/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.function.Function;
import java.util.Optional;

public class SourceUnfoldTest {

  public static // #signature
  <S, E> Source<E, NotUsed> unfold(S zero, Function<S, Optional<Pair<S, E>>> f)
        // #signature
      {
    return Source.unfold(zero, f);
  }
}
