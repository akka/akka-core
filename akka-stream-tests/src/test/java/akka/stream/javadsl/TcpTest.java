/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.Done;
import akka.japi.function.Function2;
import akka.japi.function.Procedure;
import akka.stream.BindFailedException;
import akka.stream.StreamTcpException;
import akka.stream.StreamTest;

import akka.stream.javadsl.Tcp.IncomingConnection;
import akka.stream.javadsl.Tcp.ServerBinding;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.SocketUtil;
import akka.testkit.javadsl.EventFilter;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import static akka.util.ByteString.emptyByteString;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// #setting-up-ssl-engine
// imports
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import akka.stream.TLSRole;

// #setting-up-ssl-engine

public class TcpTest extends StreamTest {
  public TcpTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("TcpTest", AkkaSpec.testConf());

  final Sink<IncomingConnection, CompletionStage<Done>> echoHandler =
      Sink.foreach(
          new Procedure<IncomingConnection>() {
            public void apply(IncomingConnection conn) {
              conn.handleWith(Flow.of(ByteString.class), system);
            }
          });

  final List<ByteString> testInput = new ArrayList<>();

  {
    for (char c = 'a'; c <= 'z'; c++) {
      testInput.add(ByteString.fromString(String.valueOf(c)));
    }
  }

  @Test
  public void mustWorkInHappyCase() throws Exception {
    final InetSocketAddress serverAddress =
        SocketUtil.temporaryServerAddress(SocketUtil.RANDOM_LOOPBACK_ADDRESS(), false);
    final Source<IncomingConnection, CompletionStage<ServerBinding>> binding =
        Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort());

    final CompletionStage<ServerBinding> future = binding.to(echoHandler).run(system);
    final ServerBinding b = future.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(b.localAddress().getPort(), serverAddress.getPort());

    final CompletionStage<ByteString> resultFuture =
        Source.from(testInput)
            .via(
                Tcp.get(system)
                    .outgoingConnection(serverAddress.getHostString(), serverAddress.getPort()))
            .runFold(
                emptyByteString(),
                new Function2<ByteString, ByteString, ByteString>() {
                  public ByteString apply(ByteString acc, ByteString elem) {
                    return acc.concat(elem);
                  }
                },
                system);

    final byte[] result = resultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS).toArray();
    for (int i = 0; i < testInput.size(); i++) {
      assertEquals(testInput.get(i).head(), result[i]);
    }

    b.unbind();
  }

  @Test
  public void mustReportServerBindFailure() throws Exception {
    final InetSocketAddress serverAddress =
        SocketUtil.temporaryServerAddress(SocketUtil.RANDOM_LOOPBACK_ADDRESS(), false);
    final Source<IncomingConnection, CompletionStage<ServerBinding>> binding =
        Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort());

    final CompletionStage<ServerBinding> future = binding.to(echoHandler).run(system);
    final ServerBinding b = future.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(b.localAddress().getPort(), serverAddress.getPort());

    new TestKit(system) {
      {
        new EventFilter(BindException.class, system)
            .occurrences(1)
            .intercept(
                () -> {
                  ExecutionException executionException =
                      Assert.assertThrows(
                          "CompletableFuture.get() should throw ExecutionException",
                          ExecutionException.class,
                          () ->
                              binding
                                  .to(echoHandler)
                                  .run(system)
                                  .toCompletableFuture()
                                  .get(5, TimeUnit.SECONDS));
                  assertTrue(
                      "The cause of ExecutionException should be instanceof BindFailedException",
                      executionException.getCause() instanceof BindFailedException);
                  b.unbind();
                  return null;
                });
      }
    };
  }

  @Test
  public void mustReportClientConnectFailure() {
    final InetSocketAddress serverAddress = SocketUtil.notBoundServerAddress();
    ExecutionException executionException =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () ->
                Source.from(testInput)
                    .viaMat(
                        Tcp.get(system)
                            .outgoingConnection(
                                serverAddress.getHostString(), serverAddress.getPort()),
                        Keep.right())
                    .to(Sink.ignore())
                    .run(system)
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS));
    assertEquals(
        "The cause of ExecutionException should be StreamTcpException",
        StreamTcpException.class,
        executionException.getCause().getClass());
  }

  // compile only sample
  // #setting-up-ssl-engine
  // initialize SSLContext once
  private final SSLContext sslContext;

  {
    try {
      // Don't hardcode your password in actual code
      char[] password = "abcdef".toCharArray();

      // trust store and keys in one keystore
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      keyStore.load(getClass().getResourceAsStream("/tcp-spec-keystore.p12"), password);

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
      trustManagerFactory.init(keyStore);

      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
      keyManagerFactory.init(keyStore, password);

      // init ssl context
      SSLContext context = SSLContext.getInstance("TLSv1.2");
      context.init(
          keyManagerFactory.getKeyManagers(),
          trustManagerFactory.getTrustManagers(),
          new SecureRandom());

      sslContext = context;

    } catch (KeyStoreException
        | IOException
        | NoSuchAlgorithmException
        | CertificateException
        | UnrecoverableKeyException
        | KeyManagementException e) {
      throw new RuntimeException(e);
    }
  }

  // create new SSLEngine from the SSLContext, which was initialized once
  public SSLEngine createSSLEngine(TLSRole role) {
    SSLEngine engine = sslContext.createSSLEngine();

    engine.setUseClientMode(role.equals(akka.stream.TLSRole.client()));
    engine.setEnabledCipherSuites(new String[] {"TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"});
    engine.setEnabledProtocols(new String[] {"TLSv1.2"});

    return engine;
  }
  // #setting-up-ssl-engine

}
