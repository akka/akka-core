/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn

import akka.NotUsed
import akka.stream.testkit.StreamSpec

@nowarn("msg=deprecated") @nowarn("cat=lint-infer-any")
class SetupSpec extends StreamSpec {

  "Source.setup" should {

    "expose materializer" in {
      val source = Source.setup { (mat, _) =>
        Source.single(mat.isShutdown)
      }

      source.runWith(Sink.head).futureValue shouldBe false
    }

    "expose attributes" in {
      val source = Source.setup { (_, attr) =>
        Source.single(attr.attributeList)
      }

      source.runWith(Sink.head).futureValue should not be empty
    }

    "propagate materialized value" in {
      val source = Source.setup { (_, _) =>
        Source.maybe[NotUsed]
      }

      val (completion, element) = source.toMat(Sink.head)(Keep.both).run()
      completion.futureValue.trySuccess(Some(NotUsed))
      element.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val source = Source
        .setup { (_, attr) =>
          Source.single(attr.nameLifted)
        }
        .named("my-name")

      source.runWith(Sink.head).futureValue shouldBe Some("setup-my-name")
    }

    "propagate attributes when nested" in {
      val source = Source
        .setup { (_, _) =>
          Source.setup { (_, attr) =>
            Source.single(attr.nameLifted)
          }
        }
        .named("my-name")

      source.runWith(Sink.head).futureValue shouldBe Some("setup-my-name-setup")
    }

    "handle factory failure" in {
      val error = new Error("boom")
      val source = Source.setup { (_, _) =>
        throw error
      }

      val (materialized, completion) = source.toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val source = Source.setup { (_, _) =>
        Source.empty.mapMaterializedValue(_ => throw error)
      }

      val (materialized, completion) = source.toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

  }

  "Flow.setup" should {

    "expose materializer" in {
      val flow = Flow.setup { (mat, _) =>
        Flow.fromSinkAndSource(Sink.ignore, Source.single(mat.isShutdown))
      }

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe false
    }

    "expose attributes" in {
      val flow = Flow.setup { (_, attr) =>
        Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.attributeList))
      }

      Source.empty.via(flow).runWith(Sink.head).futureValue should not be empty
    }

    "propagate materialized value" in {
      val flow = Flow.setup { (_, _) =>
        Flow.fromSinkAndSourceMat(Sink.ignore, Source.maybe[NotUsed])(Keep.right)
      }

      val (completion, element) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      completion.futureValue.trySuccess(Some(NotUsed))
      element.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val flow = Flow
        .setup { (_, attr) =>
          Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.nameLifted))
        }
        .named("my-name")

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some("setup-my-name")
    }

    "propagate attributes when nested" in {
      val flow = Flow
        .setup { (_, _) =>
          Flow.setup { (_, attr) =>
            Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.nameLifted))
          }
        }
        .named("my-name")

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some("setup-my-name-setup")
    }

    "handle factory failure" in {
      val error = new Error("boom")
      val flow = Flow.setup { (_, _) =>
        throw error
      }

      val (materialized, completion) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val flow = Flow.setup { (_, _) =>
        Flow[NotUsed].mapMaterializedValue(_ => throw error)
      }

      val (materialized, completion) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

  }

  "Sink.setup" should {

    "expose materializer" in {
      val sink = Sink.setup { (mat, _) =>
        Sink.fold(mat.isShutdown)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue shouldBe false
    }

    "expose attributes" in {
      val sink = Sink.setup { (_, attr) =>
        Sink.fold(attr.attributeList)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue should not be empty
    }

    "propagate materialized value" in {
      val sink = Sink.setup { (_, _) =>
        Sink.fold(NotUsed)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val sink = Sink
        .setup { (_, attr) =>
          Sink.fold(attr.nameLifted)(Keep.left)
        }
        .named("my-name")

      Source.empty.runWith(sink).flatten.futureValue shouldBe Some("my-name-setup")
    }

    "propagate attributes when nested" in {
      val sink = Sink
        .setup { (_, _) =>
          Sink.setup { (_, attr) =>
            Sink.fold(attr.nameLifted)(Keep.left)
          }
        }
        .named("my-name")

      Source.empty.runWith(sink).flatten.flatten.futureValue shouldBe Some("my-name-setup-setup")
    }

    "handle factory failure" in {
      val error = new Error("boom")
      val sink = Sink.setup { (_, _) =>
        throw error
      }

      Source.empty.runWith(sink).failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val sink = Sink.setup { (_, _) =>
        Sink.ignore.mapMaterializedValue(_ => throw error)
      }

      Source.empty.runWith(sink).failed.futureValue.getCause shouldBe error
    }

  }

}
