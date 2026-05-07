/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import javax.net.ssl.{ SSLContext, SSLEngine, SSLParameters }

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.stream.TLSClientAuth
import akka.stream.TLSProtocol.NegotiateNewSession

class TLSUtilsSpec extends AnyWordSpecLike with Matchers {

  "TlsUtils.applySessionParameters" must {

    "use defaults if not requested otherwise" in {
      // this test confirms the default expectation that forms the basis
      // of the follow-up tests
      val sslEngine: SSLEngine = SSLContext.getDefault.createSSLEngine()
      // by default, need client auth is false
      sslEngine.getSSLParameters.getNeedClientAuth shouldBe false

      val sslParams: SSLParameters = new SSLParameters()
      sslParams.getNeedClientAuth shouldBe false // by default is false

      val negotiableSession: NegotiateNewSession =
        // NegotiateNewSession requested nothing
        NegotiateNewSession(None, None, None, Some(sslParams))

      TlsUtils.applySessionParameters(sslEngine, negotiableSession)

      // NeedClientAuth stick to defaults
      sslEngine.getSSLParameters.getNeedClientAuth shouldBe false

    }

    "set NeedClientAuth to true by applying SSLParameters" in {
      val sslEngine: SSLEngine = SSLContext.getDefault.createSSLEngine()
      val sslParams: SSLParameters = new SSLParameters()
      sslParams.setNeedClientAuth(true)

      val negotiableSession: NegotiateNewSession =
        // NegotiateNewSession requested nothing, so sslParams should prevail
        NegotiateNewSession(None, None, None, Some(sslParams))
      TlsUtils.applySessionParameters(sslEngine, negotiableSession)

      sslEngine.getSSLParameters.getNeedClientAuth shouldBe true
    }

    "set NeedClientAuth to true when TLSClientAuth.Need is requested" in {
      val sslEngine: SSLEngine = SSLContext.getDefault.createSSLEngine()
      val sslParams: SSLParameters = new SSLParameters()

      val negotiableSession: NegotiateNewSession =
        NegotiateNewSession(None, None, Some(TLSClientAuth.Need), Some(sslParams))
      TlsUtils.applySessionParameters(sslEngine, negotiableSession)

      // true because explicitly asked when setting TLSClientAuth.Need
      sslEngine.getSSLParameters.getNeedClientAuth shouldBe true
    }

    "set NeedClientAuth to false when TLSClientAuth.None is requested" in {
      val sslEngine: SSLEngine = SSLContext.getDefault.createSSLEngine()
      val sslParams: SSLParameters = new SSLParameters()
      sslParams.setNeedClientAuth(true)

      {
        val negotiableSession: NegotiateNewSession =
          // NegotiateNewSession requested nothing, so sslParams should prevail
          NegotiateNewSession(None, None, None, Some(sslParams))
        TlsUtils.applySessionParameters(sslEngine, negotiableSession)

        // NeedClientAuth is true because ssl param is applied
        sslEngine.getSSLParameters.getNeedClientAuth shouldBe true
      }

      {
        val negotiableSession: NegotiateNewSession =
          // NegotiateNewSession requested TLSClientAuth.None, sslParams is overridden
          NegotiateNewSession(None, None, Some(TLSClientAuth.None), Some(sslParams))
        TlsUtils.applySessionParameters(sslEngine, negotiableSession)

        // despite ssl params set it to true,
        // NegotiateNewSession explicitly sets it to false
        sslEngine.getSSLParameters.getNeedClientAuth shouldBe false
      }
    }

    "set WantClientAuth to true when TLSClientAuth.Want is requested" in {
      val sslEngine: SSLEngine = SSLContext.getDefault.createSSLEngine()
      val sslParams: SSLParameters = new SSLParameters()

      val negotiableSession: NegotiateNewSession =
        NegotiateNewSession(None, None, Some(TLSClientAuth.Want), Some(sslParams))
      TlsUtils.applySessionParameters(sslEngine, negotiableSession)

      // true because explicitly asked when setting TLSClientAuth.Want
      sslEngine.getSSLParameters.getWantClientAuth shouldBe true

      sslEngine.getSSLParameters.getNeedClientAuth shouldBe false
    }

  }
}
