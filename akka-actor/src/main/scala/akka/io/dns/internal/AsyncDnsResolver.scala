/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.{ Inet4Address, Inet6Address, InetAddress, InetSocketAddress }

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Status }
import akka.annotation.InternalApi
import akka.io.SimpleDnsCache
import akka.io.dns._
import akka.io.dns.CachePolicy.{ CachePolicy, Never, Ttl }
import akka.io.dns.DnsProtocol.{ Ip, RequestType, Srv }
import akka.io.dns.internal.DnsClient._
import akka.pattern.{ ask, pipe }
import akka.pattern.AskTimeoutException
import akka.util.{ Helpers, Timeout }
import akka.util.PrettyDuration._

/**
 * INTERNAL API
 */
@InternalApi
private[io] final class AsyncDnsResolver(
    settings: DnsSettings,
    cache: SimpleDnsCache,
    clientFactory: (ActorRefFactory, List[InetSocketAddress]) => List[ActorRef])
    extends Actor
    with ActorLogging {

  import AsyncDnsResolver._

  // avoid ever looking up localhost by pre-populating cache
  {
    val loopback = InetAddress.getLoopbackAddress
    val (ipv4Address, ipv6Address) = loopback match {
      case ipv6: Inet6Address => (InetAddress.getByName("127.0.0.1"), ipv6)
      case ipv4: Inet4Address => (ipv4, InetAddress.getByName("::1"))
      case unknown            => throw new IllegalArgumentException(s"Loopback address was [$unknown]")
    }
    cache.put(
      "localhost" -> Ip(),
      DnsProtocol.Resolved("localhost", ARecord("localhost", Ttl.effectivelyForever, loopback) :: Nil),
      Ttl.effectivelyForever)
    cache.put(
      "localhost" -> Ip(ipv6 = false, ipv4 = true),
      DnsProtocol.Resolved("localhost", ARecord("localhost", Ttl.effectivelyForever, ipv4Address) :: Nil),
      Ttl.effectivelyForever)
    cache.put(
      "localhost" -> Ip(ipv6 = true, ipv4 = false),
      DnsProtocol.Resolved("localhost", ARecord("localhost", Ttl.effectivelyForever, ipv6Address) :: Nil),
      Ttl.effectivelyForever)

  }

  // For ask to DNS Client
  implicit val timeout: Timeout = Timeout(settings.ResolveTimeout)

  val nameServers = settings.NameServers

  val positiveCachePolicy = settings.PositiveCachePolicy
  val negativeCachePolicy = settings.NegativeCachePolicy
  log.debug(
    "Using name servers [{}] and search domains [{}] with ndots={}",
    nameServers,
    settings.SearchDomains,
    settings.NDots)

  private val resolvers: List[ActorRef] = clientFactory(context, nameServers)

  private val requestIdInjector: ActorRef = context.actorOf(injectorProps(settings), "requestIdInjector")

  private val inflightRequests: mutable.Map[DnsProtocol.Resolve, Set[ActorRef]] = mutable.Map.empty

  // only supports DnsProtocol, not the deprecated Dns protocol
  // AsyncDnsManager converts between the protocols to support the deprecated protocol
  override def receive: Receive = {
    case resolve @ DnsProtocol.Resolve(name, mode) =>
      cache.get((name, mode)) match {
        case Some(resolved) =>
          log.debug("{} cached {}", mode, resolved)
          sender() ! resolved
        case None =>
          // check if we're being asked to resolve an IP address, in which case no need to do anything async
          if (isInetAddress(name)) {
            Try {
              InetAddress.getByName(name) match { // only a validity check, doesn't do blocking I/O
                case ipv4: Inet4Address => ARecord(name, Ttl.effectivelyForever, ipv4)
                case ipv6: Inet6Address => AAAARecord(name, Ttl.effectivelyForever, ipv6)
                case unexpected         => throw new IllegalArgumentException(s"Unexpected address: $unexpected")
              }
            }.fold(ex => { sender() ! Status.Failure(ex) }, record => {
              val resolved = DnsProtocol.Resolved(name, record :: Nil)
              cache.put(name -> mode, resolved, record.ttl)
              sender() ! resolved
            })
          } else if (resolvers.isEmpty) {
            sender() ! Status.Failure(failToResolve(name, nameServers))
          } else {
            inflightRequests.get(resolve) match {
              case Some(otherRequestors) =>
                // Currently resolving this same question, so add to the waiting list
                inflightRequests.update(resolve, otherRequestors + sender())

              case None =>
                // spawn an actor to manage this resolution (apply search names, failover to other resolvers, etc.)
                context.actorOf(
                  DnsResolutionActor.props(settings, requestIdInjector, resolve, sender(), self, resolvers))
                inflightRequests += (resolve -> Set.empty)
            }
          }
      }

    case ResolutionFinished(resolve, resolved, ttl) =>
      inflightRequests.get(resolve).foreach { requestors =>
        requestors.foreach(_ ! resolved)
      }
      inflightRequests.remove(resolve)

      ttl.foreach { t =>
        cache.put(resolve.name -> resolve.requestType, resolved, t)
      }

    case ResolutionFailed(resolve, failure) =>
      inflightRequests.get(resolve).foreach { requestors =>
        requestors.foreach(_ ! failure)
      }

      inflightRequests.remove(resolve)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object AsyncDnsResolver {

  private val ipv4Address =
    """^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$""".r

  private val ipv6Address =
    """^\s*((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%.+)?\s*$""".r

  private[akka] def isIpv4Address(name: String): Boolean =
    ipv4Address.findAllMatchIn(name).nonEmpty

  private[akka] def isIpv6Address(name: String): Boolean =
    ipv6Address.findAllMatchIn(name).nonEmpty

  private def isInetAddress(name: String): Boolean =
    isIpv4Address(name) || isIpv6Address(name)

  private val Empty =
    Future.successful(Answer(-1, immutable.Seq.empty[ResourceRecord], immutable.Seq.empty[ResourceRecord]))

  private def failToResolve(name: String, nameServers: List[InetSocketAddress]): ResolveFailedException =
    ResolveFailedException(s"Failed to resolve $name with nameservers ${nameServers}")

  case class ResolveFailedException(msg: String) extends Exception(msg)

  // ttl is defined if this resolution should be cached
  private final case class ResolutionFinished(
      resolve: DnsProtocol.Resolve,
      resolved: DnsProtocol.Resolved,
      ttl: Option[CachePolicy])

  private final case class ResolutionFailed(resolve: DnsProtocol.Resolve, failure: Status.Failure)

  // RequestIdInjector handles these
  private final case class DnsQuestionPreInjection(resolver: ActorRef, inject: Short => DnsQuestion, timeout: Timeout)
  private final case class DidntDrop(id: Short)

  private def injectorProps(settings: DnsSettings): Props = Props(new RequestIdInjector(settings.idGenerator))

  // Drop the request if this many attempts to generate an id fail to find a definitely-safe id
  val MaxIdGenerationAttempts: Int = 2 * Short.MaxValue.toInt - 1

  // tracks and injects a request ID before forwarding to the resolver

  private class RequestIdInjector(idGenerator: () => Short) extends Actor with ActorLogging {
    // immutable set: Scala optimizes for the case where it's small
    //  (since there isn't typically much request concurrency, this will tend to have 4 or fewer elements)
    private var activeRequestIds: Set[Short] = Set.empty

    private def generateCandidateId(): Short = idGenerator()

    @annotation.tailrec
    private def nextId(count: Int): Short =
      if (count == MaxIdGenerationAttempts) {
        throw new IllegalStateException(s"Too many DNS request IDs in use!  Made [$count] generation attempts.")
      } else {
        val requestId = generateCandidateId()

        if (activeRequestIds.contains(requestId)) nextId(count + 1)
        else requestId
      }

    override def receive: Receive = {
      case DnsQuestionPreInjection(resolver, inject, timeout) =>
        implicit val tmout = timeout

        try {
          val question = inject(nextId(count = 0))
          val forwardAnswerTo = sender()
          val result = (resolver ? question).mapTo[Answer]
          activeRequestIds = activeRequestIds + question.id

          result.onComplete {
            case Success(answer) =>
              // getting an answer implies that the id of our question is now free...
              forwardAnswerTo ! answer
              self ! Dropped(question.id)

            case Failure(_) =>
              implicit val ec = context.dispatcher // needed for pipeTo

              (resolver ? DropRequest(question))
                .mapTo[Dropped]
                .recover {
                  case _ => DidntDrop(question.id)
                }
                .pipeTo(self)
          }(ExecutionContext.parasitic)
        } catch {
          case NonFatal(ex) =>
            log.warning(ex, "Not forwarding DNS question to resolver [{}]", resolver)
        }

      case Dropped(id) =>
        // id is now free
        activeRequestIds = activeRequestIds - id

      case DidntDrop(id) =>
        // only safe thing is to assume that this id is toxic for the rest of time
        // note that if this is just a symptom of an overloaded resolver, assuming
        //  toxicity is another way of introducing some backpressure by increasing
        //  the time spent hunting for an id
        //
        // after 2^15 cumulative DidntDrop messages, the chance a resolution fails to find an id
        // is then 1 in 2^(2^17) (assuming no ids are legitimately considered active)
        log.warning(
          "Cannot be sure that DNS request ID [{}] will ever be safe to use again. " +
          "This is basically harmless but slightly increases the chance of future DNS " +
          "resolution failures.",
          id)
    }
  }

  private object DnsResolutionActor {
    def props(
        settings: DnsSettings,
        requestIdInjector: ActorRef,
        resolve: DnsProtocol.Resolve,
        answerTo: ActorRef,
        cacheActor: ActorRef,
        resolvers: List[ActorRef]): Props =
      Props(new DnsResolutionActor(settings, requestIdInjector, resolve, answerTo, cacheActor, resolvers))
  }

  private class DnsResolutionActor(
      settings: DnsSettings,
      requestIdInjector: ActorRef,
      resolve: DnsProtocol.Resolve,
      answerTo: ActorRef,
      cacheActor: ActorRef,
      resolvers: List[ActorRef])
      extends Actor
      with ActorLogging {

    private implicit val timeout: Timeout = Timeout(settings.ResolveTimeout)

    private def failToResolve(): Unit =
      answerWithFailure(AsyncDnsResolver.failToResolve(name, settings.NameServers))

    private def answerWithFailure(ex: Throwable): Unit = {
      val failure = Status.Failure(ex)
      cacheActor ! ResolutionFailed(resolve, failure)
      answer(failure)
    }

    private def answer(withMsg: Any): Unit = {
      answerTo ! withMsg

      context.stop(self)
    }

    private def name: String = resolve.name
    private def mode: RequestType = resolve.requestType

    // Not strictly necessary, since our parent checks this before spawning, but belt-and-suspenders
    if (resolvers.isEmpty) {
      failToResolve() // (vacuously) exhausted the resolvers
      // fail fast
      throw new IllegalArgumentException("resolvers should not be empty")
    }

    // the fully-qualified domain names (FQDNs), in the order we want to attempt for any given resolver
    private val namesToResolve = {
      val nameWithSearch = settings.SearchDomains.map(sd => s"${name}.${sd}")

      // ndots is a heuristic used to attempt to work out if `name` is an FQDN or a name relative to a search
      // name.  The goal is to not incur the cost of a lookup that's unlikely to resolve because we need to
      // relativize it.  If the name being searched contains less than ndots dots, then we look it up last,
      // rather than first.
      if (name.count(_ == '.') >= settings.NDots) {
        // assume fully-qualified, so try `name` first
        (name :: nameWithSearch).map(Helpers.toRootLowerCase)
      } else {
        // assume relative, so try `name` last
        (nameWithSearch :+ name).map(Helpers.toRootLowerCase)
      }
    }

    // Don't expect a message until startResolution `become`s activelyResolving
    override def receive: Receive = PartialFunction.empty
    // safe, already verified that resolvers is non-empty
    startResolution(namesToResolve, resolvers.head, resolvers.tail)

    private def activelyResolving(
        searchName: String,
        resolver: ActorRef,
        nextNamesToTry: List[String],
        nextResolversToTry: List[ActorRef]): Receive = {
      case resolved: DnsProtocol.Resolved =>
        if (resolved.records.isEmpty) {
          if (nextNamesToTry.nonEmpty) startResolution(nextNamesToTry, resolver, nextResolversToTry)
          else handleResolved(resolved) // empty but successful response
        } else {
          handleResolved(resolved)
        }

      case Status.Failure(ex) =>
        ex match {
          case _: AskTimeoutException =>
            log.info("Resolve of {} timed out after {}.  Trying next name server", searchName, timeout.duration.pretty)
          case _ =>
            log.info("Resolve of {} failed.  Trying next name server.  Exception: {}", name, ex.getMessage)
        }

        // failed, move on to next name server
        if (nextResolversToTry.nonEmpty)
          startResolution(namesToResolve, nextResolversToTry.head, nextResolversToTry.tail)
        else failToResolve() // exhausted the resolvers
    }

    private def startResolution(
        namesToTry: List[String],
        resolverToUse: ActorRef,
        fallbackResolvers: List[ActorRef]): Unit = {
      if (namesToTry.nonEmpty) {
        val searchName = namesToTry.head

        log.debug("Attempting to resolve {} with {}", searchName, resolverToUse)

        implicit val ec = context.dispatcher // for pipeTo
        resolvedFut(searchName, resolverToUse).pipeTo(self)

        context.become(activelyResolving(searchName, resolverToUse, namesToTry.tail, fallbackResolvers))
      } else {
        // shouldn't happen
        log.warning("startResolution called with empty list of namesToTry")
        failToResolve()
      }
    }

    private def handleResolved(resolved: DnsProtocol.Resolved): Unit = {
      if (resolved.records.nonEmpty) {
        val minTtl = (settings.PositiveCachePolicy +: resolved.records.map(_.ttl)).min
        cacheActor ! ResolutionFinished(resolve, resolved, Some(minTtl))
      } else if (settings.NegativeCachePolicy != Never) {
        cacheActor ! ResolutionFinished(resolve, resolved, Some(settings.NegativeCachePolicy))
      } else {
        // don't cache anything, but a subsequent resolve will start another resolution process
        cacheActor ! ResolutionFinished(resolve, resolved, None)
      }

      log.debug("{} resolved {}", mode, resolved)

      answer(resolved)
    }

    private def sendQuestion(resolver: ActorRef, inject: Short => DnsQuestion): Future[Answer] =
      (requestIdInjector ? DnsQuestionPreInjection(resolver, inject, timeout)).mapTo[Answer]

    private def resolvedFut(searchName: String, resolver: ActorRef): Future[DnsProtocol.Resolved] =
      mode match {
        case Ip(ipv4, ipv6) =>
          val ipv4Recs =
            if (ipv4) sendQuestion(resolver, requestId => Question4(requestId, searchName))
            else Empty

          val ipv6Recs =
            if (ipv6) sendQuestion(resolver, requestId => Question6(requestId, searchName))
            else Empty

          ipv4Recs.flatMap { v4 =>
            ipv6Recs.map { v6 =>
              DnsProtocol.Resolved(searchName, v4.rrs ++ v6.rrs, v4.additionalRecs ++ v6.additionalRecs)
            }(ExecutionContext.parasitic)
          }(ExecutionContext.parasitic)

        case Srv =>
          sendQuestion(resolver, requestId => SrvQuestion(requestId, searchName)).map { answer =>
            DnsProtocol.Resolved(searchName, answer.rrs, answer.additionalRecs)
          }(ExecutionContext.parasitic)
      }
  }
}
