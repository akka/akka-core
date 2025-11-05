/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.{ Future, Promise }

import akka.NotUsed
import akka.stream.{ AbruptStageTerminationException, Attributes, Materializer, NeverMaterializedException }
import akka.stream.SubscriptionWithCancelException.NonFailureCancellation
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE

class FlowFutureFlowSpec extends StreamSpec {
  def src10(i: Int = 0) = Source(i until (i + 10))
  def src10WithFailure(i: Int = 0)(failOn: Int) = src10(i).map {
    case `failOn` => throw TE(s"fail on $failOn")
    case x        => x
  }

  //this stage's behaviour in case of an 'early' downstream cancellation is governed by an attribute
  //so we run all tests cases using both modes of the attributes.
  //please notice most of the cases don't exhibit any difference in behaviour between the two modes
  for {
    (att, name) <- List(
      (Attributes.NestedMaterializationCancellationPolicy.EagerCancellation, "EagerCancellation"),
      (Attributes.NestedMaterializationCancellationPolicy.PropagateToNested, "PropagateToNested"))
    delayDownstreamCancellation = att.propagateToNestedMaterialization
    attributes = Attributes(att)
  } {

    s"a futureFlow with $name (delayDownstreamCancellation=$delayDownstreamCancellation)" must {
      "work in the simple case with a completed future" in {
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful(Flow[Int])
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(0 until 10)
      }

      "work in the simple case with a late future" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success(Flow[Int])

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(0 until 10)
      }

      "fail properly when future is a completed failed future" in {
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.failed[Flow[Int, Int, NotUsed]](TE("damn!"))
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.failed.futureValue should be(a[NeverMaterializedException])
        fNotUsed.failed.futureValue.getCause should equal(TE("damn!"))

        fSeq.failed.futureValue should equal(TE("damn!"))

      }

      "fail properly when future is late completed failed future" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.failure(TE("damn!"))

        fNotUsed.failed.futureValue should be(a[NeverMaterializedException])
        fNotUsed.failed.futureValue.getCause should equal(TE("damn!"))

        fSeq.failed.futureValue should equal(TE("damn!"))

      }

      "handle upstream failure when future is pre-completed" in {
        val (fNotUsed, fSeq) = src10WithFailure()(5)
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int].recover {
                  case TE("fail on 5") => 99
                }
              }
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(List(0, 1, 2, 3, 4, 99))
      }

      "handle upstream failure when future is late-completed" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10WithFailure()(5)
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success {
          Flow[Int].recover {
            case TE("fail on 5") => 99
          }
        }

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(List(0, 1, 2, 3, 4, 99))
      }

      "propagate upstream failure when future is pre-completed" in {
        val (fNotUsed, fSeq) = src10WithFailure()(5)
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int]
              }
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.failed.futureValue should equal(TE("fail on 5"))
      }

      "propagate upstream failure when future is late-completed" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10WithFailure()(5)
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success {
          Flow[Int]
        }

        fNotUsed.futureValue should be(NotUsed)
        fSeq.failed.futureValue should equal(TE("fail on 5"))
      }

      "handle early upstream error when flow future is pre-completed" in {
        val (fNotUsed, fSeq) = Source
          .failed(TE("not today my friend"))
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int]
                  .recover {
                    case TE("not today my friend") => 99
                  }
                  .concat(src10())
              }
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(99 +: (0 until 10))

      }

      "handle early upstream error when flow future is late-completed" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = Source
          .failed(TE("not today my friend"))
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success {
          Flow[Int]
            .recover {
              case TE("not today my friend") => 99
            }
            .concat(src10())
        }

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(99 +: (0 until 10))

      }

      "handle closed downstream when flow future is pre completed" in {
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int].alsoToMat(Sink.seq)(Keep.right)
              }
            }
          }(Keep.right)
          .mapMaterializedValue(_.flatten)
          .take(0)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fSeq1.futureValue should be(empty)
        fSeq2.futureValue should be(empty)

      }

      "handle closed downstream when flow future is completed after downstream cancel" in {
        val prFlow = Promise[Flow[Int, Int, Future[collection.immutable.Seq[Int]]]]()
        val (fNestedFlowMatVal, fSinkCompletion) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .mapMaterializedValue(_.flatten)
          .take(0) // cancel asap
          .toMat(Sink.ignore)(Keep.both)
          .withAttributes(attributes)
          .run()

        fSinkCompletion.futureValue // downstream has completed/cancelled
        if (delayDownstreamCancellation) {
          fNestedFlowMatVal.value should be(empty)

          prFlow.success {
            Flow[Int].alsoToMat(Sink.seq)(Keep.right)
          }

          // was materialized but cancelled
          fNestedFlowMatVal.futureValue should be(empty)
        } else {
          // was never materialized
          fNestedFlowMatVal.failed.futureValue should be(a[NeverMaterializedException])
          fNestedFlowMatVal.failed.futureValue.getCause should be(a[NonFailureCancellation])
        }
      }

      "handle early downstream failure when flow future is pre-completed" in {
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int].alsoToMat(Sink.seq)(Keep.right)
              }
            }
          }(Keep.right)
          .mapMaterializedValue(_.flatten)
          .prepend(Source.failed(TE("damn!")))
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fSeq1.failed.futureValue should equal(TE("damn!"))
        fSeq2.failed.futureValue should equal(TE("damn!"))
      }

      "handle early downstream failure when flow future is late completed" in {
        val prFlow = Promise[Flow[Int, Int, Future[collection.immutable.Seq[Int]]]]()
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .mapMaterializedValue(_.flatten)
          .prepend(Source.failed(TE("damn!")))
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        if (delayDownstreamCancellation) {
          fSeq2.failed.futureValue should equal(TE("damn!"))
          fSeq1.value should be(empty)

          prFlow.success {
            Flow[Int].alsoToMat(Sink.seq)(Keep.right)
          }

          fSeq1.failed.futureValue should equal(TE("damn!"))
        } else {
          fSeq1.failed.futureValue should be(a[NeverMaterializedException])
          fSeq1.failed.futureValue.getCause should equal(TE("damn!"))
          fSeq2.failed.futureValue should equal(TE("damn!"))
        }
      }

      "handle early upstream completion when flow future is pre-completed" in {
        val (fNotUsed, fSeq) = Source
          .empty[Int]
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int].orElse(Source.single(99))
              }
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(99 :: Nil)
      }

      "handle early upstream completion when flow future is late-completed" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = Source
          .empty[Int]
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success {
          Flow[Int].orElse(Source.single(99))
        }

        fNotUsed.futureValue should be(NotUsed)
        fSeq.futureValue should equal(99 :: Nil)
      }

      "fails properly on materialization failure with a completed future" in {
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful(Flow[Int].mapMaterializedValue[NotUsed](_ => throw TE("BBOM!")))
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.failed.futureValue should be(a[NeverMaterializedException])
        fNotUsed.failed.futureValue.getCause should equal(TE("BBOM!"))
        fSeq.failed.futureValue should equal(TE("BBOM!"))
      }

      "fails properly on materialization failure with a late future" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success(Flow[Int].mapMaterializedValue[NotUsed](_ => throw TE("BBOM!")))

        fNotUsed.failed.futureValue should be(a[NeverMaterializedException])
        fNotUsed.failed.futureValue.getCause should equal(TE("BBOM!"))
        fSeq.failed.futureValue should equal(TE("BBOM!"))
      }

      "propagate flow failures with a completed future" in {
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow[Int].map {
                  case 5 => throw TE("fail on 5")
                  case x => x
                }
              }
            }
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.futureValue should be(NotUsed)
        fSeq.failed.futureValue should equal(TE("fail on 5"))
      }

      "propagate flow failures with a late future" in {
        val prFlow = Promise[Flow[Int, Int, NotUsed]]()
        val (fNotUsed, fSeq) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fNotUsed.value should be(empty)
        fSeq.value should be(empty)

        prFlow.success {
          Flow[Int].map {
            case 5 => throw TE("fail on 5")
            case x => x
          }
        }

        fNotUsed.futureValue should be(NotUsed)
        fSeq.failed.futureValue should equal(TE("fail on 5"))
      }

      "allow flow to handle downstream completion with a completed future" in {
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow {
              Future.successful {
                Flow.fromSinkAndSourceMat(Sink.seq[Int], src10(10))(Keep.left)
              }
            }
          }(Keep.right)
          .take(5)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fSeq1.flatten.futureValue should be(0 until 10)
        fSeq2.futureValue should equal(10 until 15)
      }

      "allow flow to handle downstream completion with a late future" in {
        val pr = Promise[Flow[Int, Int, Future[Seq[Int]]]]()
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow(pr.future)
          }(Keep.right)
          .take(5)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        fSeq1.value should be(empty)
        fSeq2.value should be(empty)

        pr.success {
          Flow.fromSinkAndSourceMat(Sink.seq[Int], src10(10))(Keep.left)
        }

        fSeq1.flatten.futureValue should be(0 until 10)
        fSeq2.futureValue should equal(10 until 15)
      }

      "abrupt termination before future completion" in {
        val mat = Materializer(system)
        val prFlow = Promise[Flow[Int, Int, Future[collection.immutable.Seq[Int]]]]()
        val (fSeq1, fSeq2) = src10()
          .viaMat {
            Flow.futureFlow(prFlow.future)
          }(Keep.right)
          .take(5)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()(mat)

        fSeq1.value should be(empty)
        fSeq2.value should be(empty)

        mat.shutdown()

        fSeq1.failed.futureValue should be(a[AbruptStageTerminationException])
        fSeq2.failed.futureValue should be(a[AbruptStageTerminationException])
      }
    }
  }

  "NestedMaterializationCancellationPolicy" must {
    "default to false" in {
      val fl = Flow.fromMaterializer {
        case (_, attributes) =>
          val att = attributes.mandatoryAttribute[Attributes.NestedMaterializationCancellationPolicy]
          att.propagateToNestedMaterialization should be(false)
          Flow[Any]
      }
      Source.empty.via(fl).runWith(Sink.headOption).futureValue should be(empty)
    }
  }
}
