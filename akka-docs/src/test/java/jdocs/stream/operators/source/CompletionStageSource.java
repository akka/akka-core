/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

// #sourceCompletionStageSource
import akka.NotUsed;
import akka.stream.javadsl.Source;
import java.util.concurrent.CompletionStage;

public class CompletionStageSource {

  public static void sourceCompletionStageSource() {
    UserRepository userRepository = null; // an abstraction over the remote service
    Source<User, CompletionStage<NotUsed>> userCompletionStageSource =
        Source.completionStageSource(userRepository.loadUsers());
    // ...
  }

  interface UserRepository {
    CompletionStage<Source<User, NotUsed>> loadUsers();
  }

  static class User {}
}
// #sourceCompletionStageSource
