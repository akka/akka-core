/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.Attributes.Attribute
import akka.stream.scaladsl.AttributesSpec.{ whateverAttribute, WhateverAttribute }
import akka.stream.testkit.StreamSpec

import scala.annotation.nowarn

class FromMaterializerSpec extends StreamSpec {

  case class MyAttribute() extends Attribute
  val myAttributes = Attributes(MyAttribute())

  "Source.fromMaterializer" should {

    "expose materializer" in {
      val source = Source.fromMaterializer { (mat, _) =>
        Source.single(mat.isShutdown)
      }

      source.runWith(Sink.head).futureValue shouldBe false
    }

    "expose attributes" in {
      val source = Source.fromMaterializer { (_, attr) =>
        Source.single(attr.attributeList)
      }

      source.runWith(Sink.head).futureValue should not be empty
    }

    "propagate materialized value" in {
      val source = Source.fromMaterializer { (_, _) =>
        Source.maybe[NotUsed]
      }

      val (completion, element) = source.toMat(Sink.head)(Keep.both).run()
      completion.futureValue.trySuccess(Some(NotUsed))
      element.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val source = Source
        .fromMaterializer { (_, attr) =>
          Source.single(attr.nameLifted)
        }
        .named("my-name")

      source.runWith(Sink.head).futureValue shouldBe Some("setup-my-name")
    }

    "propagate attributes when nested" in {
      val source = Source
        .fromMaterializer { (_, _) =>
          Source.fromMaterializer { (_, attr) =>
            Source.single(attr.nameLifted)
          }
        }
        .named("my-name")

      source.runWith(Sink.head).futureValue shouldBe Some("setup-my-name-setup")
    }

    "preserve attributes of inner source" in {
      val source = Source.fromMaterializer { (_, _) =>
        Source
          .fromMaterializer { (_, attr) =>
            Source.single(attr.get[MyAttribute])
          }
          .addAttributes(myAttributes)
      }

      source.runWith(Sink.head).futureValue shouldBe Some(MyAttribute())
    }

    "give priority to attributes of inner source" in {
      val source = Source
        .fromMaterializer { (_, _) =>
          Source
            .fromMaterializer { (_, attr) =>
              Source.single(attr.get[WhateverAttribute])
            }
            .addAttributes(whateverAttribute("inner"))
        }
        .addAttributes(whateverAttribute("outer"))

      source.runWith(Sink.head).futureValue shouldBe Some(WhateverAttribute("inner"))
    }

    "handle factory failure" in {
      val error = new Error("boom")
      val source = Source.fromMaterializer { (_, _) =>
        throw error
      }

      val (materialized, completion) = source.toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val source = Source.fromMaterializer { (_, _) =>
        Source.empty.mapMaterializedValue(_ => throw error)
      }

      val (materialized, completion) = source.toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

  }

  "Flow.fromMaterializer" should {

    "expose materializer" in {
      val flow = Flow.fromMaterializer { (mat, _) =>
        Flow.fromSinkAndSource(Sink.ignore, Source.single(mat.isShutdown))
      }

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe false
    }

    "expose attributes" in {
      val flow = Flow.fromMaterializer { (_, attr) =>
        Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.attributeList))
      }

      Source.empty.via(flow).runWith(Sink.head).futureValue should not be empty
    }

    "propagate materialized value" in {
      val flow = Flow.fromMaterializer { (_, _) =>
        Flow.fromSinkAndSourceMat(Sink.ignore, Source.maybe[NotUsed])(Keep.right)
      }

      val (completion, element) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      completion.futureValue.trySuccess(Some(NotUsed))
      element.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val flow = Flow
        .fromMaterializer { (_, attr) =>
          Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.nameLifted))
        }
        .named("my-name")

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some("setup-my-name")
    }

    "propagate attributes when nested" in {
      val flow = Flow
        .fromMaterializer { (_, _) =>
          Flow.fromMaterializer { (_, attr) =>
            Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.nameLifted))
          }
        }
        .named("my-name")

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some("setup-my-name-setup")
    }

    "preserve attributes of inner flow" in {
      val flow = Flow.fromMaterializer { (_, _) =>
        Flow
          .fromMaterializer { (_, attr) =>
            Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.get[MyAttribute]))
          }
          .addAttributes(myAttributes)
      }

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some(MyAttribute())
    }

    "give priority to attributes of inner flow" in {
      val flow = Flow
        .fromMaterializer { (_, _) =>
          Flow
            .fromMaterializer { (_, attr) =>
              Flow.fromSinkAndSource(Sink.ignore, Source.single(attr.get[WhateverAttribute]))
            }
            .addAttributes(whateverAttribute("inner"))
        }
        .addAttributes(whateverAttribute("outer"))

      Source.empty.via(flow).runWith(Sink.head).futureValue shouldBe Some(WhateverAttribute("inner"))
    }

    "handle factory failure" in {
      val error = new Error("boom")
      @nowarn("cat=lint-infer-any")
      val flow = Flow.fromMaterializer { (_, _) =>
        throw error
      }

      val (materialized, completion) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val flow = Flow.fromMaterializer { (_, _) =>
        Flow[NotUsed].mapMaterializedValue(_ => throw error)
      }

      val (materialized, completion) = Source.empty.viaMat(flow)(Keep.right).toMat(Sink.head)(Keep.both).run()
      materialized.failed.futureValue.getCause shouldBe error
      completion.failed.futureValue.getCause shouldBe error
    }

  }

  "Sink.fromMaterializer" should {

    "expose materializer" in {
      val sink = Sink.fromMaterializer { (mat, _) =>
        Sink.fold(mat.isShutdown)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue shouldBe false
    }

    "expose attributes" in {
      val sink = Sink.fromMaterializer { (_, attr) =>
        Sink.fold(attr.attributeList)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue should not be empty
    }

    "propagate materialized value" in {
      val sink = Sink.fromMaterializer { (_, _) =>
        Sink.fold(NotUsed)(Keep.left)
      }

      Source.empty.runWith(sink).flatten.futureValue shouldBe NotUsed
    }

    "propagate attributes" in {
      val sink = Sink
        .fromMaterializer { (_, attr) =>
          Sink.fold(attr.nameLifted)(Keep.left)
        }
        .named("my-name")

      Source.empty.runWith(sink).flatten.futureValue shouldBe Some("my-name-setup")
    }

    "propagate attributes when nested" in {
      val sink = Sink
        .fromMaterializer { (_, _) =>
          Sink.fromMaterializer { (_, attr) =>
            Sink.fold(attr.nameLifted)(Keep.left)
          }
        }
        .named("my-name")

      Source.empty.runWith(sink).flatten.flatten.futureValue shouldBe Some("my-name-setup-setup")
    }

    "preserve attributes of inner sink" in {
      val sink = Sink.fromMaterializer { (_, _) =>
        Sink
          .fromMaterializer { (_, attr) =>
            Sink.fold(attr.get[MyAttribute])(Keep.left)
          }
          .addAttributes(myAttributes)
      }

      Source.empty.runWith(sink).flatten.flatten.futureValue shouldBe Some(MyAttribute())
    }

    "give priority to attributes of inner sink" in {
      val sink = Sink
        .fromMaterializer { (_, _) =>
          Sink
            .fromMaterializer { (_, attr) =>
              Sink.fold(attr.get[WhateverAttribute])(Keep.left)
            }
            .addAttributes(whateverAttribute("inner"))
        }
        .addAttributes(whateverAttribute("outer"))

      Source.empty.runWith(sink).flatten.flatten.futureValue shouldBe Some(WhateverAttribute("inner"))
    }

    "handle factory failure" in {
      val error = new Error("boom")
      @nowarn("cat=lint-infer-any")
      val sink = Sink.fromMaterializer { (_, _) =>
        throw error
      }

      Source.empty.runWith(sink).failed.futureValue.getCause shouldBe error
    }

    "handle materialization failure" in {
      val error = new Error("boom")
      val sink = Sink.fromMaterializer { (_, _) =>
        Sink.ignore.mapMaterializedValue(_ => throw error)
      }

      Source.empty.runWith(sink).failed.futureValue.getCause shouldBe error
    }

  }

}
