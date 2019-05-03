/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.MemberConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import io.atomix.utils.stream.StreamFunction;
import io.atomix.utils.stream.StreamHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Netty messaging service test.
 */
public class NettyMessagingServiceTest {

  private static final Logger LOGGER = getLogger(NettyMessagingServiceTest.class);

  NettyMessagingService netty1;
  NettyMessagingService netty2;
  NettyMessagingService nettyv11;
  NettyMessagingService nettyv12;
  NettyMessagingService nettyv21;
  NettyMessagingService nettyv22;

  private static final String IP_STRING = "127.0.0.1";

  Address address1;
  Address address2;
  Address addressv21;
  Address addressv22;
  Address invalidAddress;

  @Before
  public void setUp() throws Exception {
    address1 = Address.from(findAvailablePort(5001));
    netty1 = new NettyMessagingService();
    netty1.start(new ClusterConfig()
        .setClusterId("test")
        .setNodeConfig(new MemberConfig().setAddress(address1))).join();

    address2 = Address.from(findAvailablePort(5002));
    netty2 = new NettyMessagingService();
    netty2.start(new ClusterConfig()
        .setClusterId("test")
        .setNodeConfig(new MemberConfig().setAddress(address2))).join();

    addressv21 = Address.from(findAvailablePort(5005));
    nettyv21 = new NettyMessagingService();
    nettyv21.start(new ClusterConfig()
        .setClusterId("test")
        .setNodeConfig(new MemberConfig().setAddress(addressv21))).join();

    addressv22 = Address.from(findAvailablePort(5006));
    nettyv22 = new NettyMessagingService();
    nettyv22.start(new ClusterConfig()
        .setClusterId("test")
        .setNodeConfig(new MemberConfig().setAddress(addressv22))).join();

    invalidAddress = Address.from(IP_STRING, 5007);
  }

  /**
   * Returns a random String to be used as a test subject.
   *
   * @return string
   */
  private String nextSubject() {
    return UUID.randomUUID().toString();
  }

  @After
  public void tearDown() throws Exception {
    if (netty1 != null) {
      try {
        netty1.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty1", e);
      }
    }

    if (netty2 != null) {
      try {
        netty2.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty2", e);
      }
    }
  }

  @Test
  public void testSendAsync() {
    String subject = nextSubject();
    CountDownLatch latch1 = new CountDownLatch(1);
    CompletableFuture<Void> response = netty1.sendAsync(address2, subject, "hello world".getBytes());
    response.whenComplete((r, e) -> {
      assertNull(e);
      latch1.countDown();
    });
    Uninterruptibles.awaitUninterruptibly(latch1);

    CountDownLatch latch2 = new CountDownLatch(1);
    response = netty1.sendAsync(invalidAddress, subject, "hello world".getBytes());
    response.whenComplete((r, e) -> {
      assertNotNull(e);
      assertTrue(e instanceof ConnectException);
      latch2.countDown();
    });
    Uninterruptibles.awaitUninterruptibly(latch2);
  }

  @Test
  public void testSendAndReceive() {
    String subject = nextSubject();
    AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    AtomicReference<byte[]> request = new AtomicReference<>();
    AtomicReference<Address> sender = new AtomicReference<>();

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
      handlerInvoked.set(true);
      sender.set(ep);
      request.set(data);
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, MoreExecutors.directExecutor());

    CompletableFuture<byte[]> response = netty1.sendAndReceive(address2, subject, "hello world".getBytes());
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertTrue(handlerInvoked.get());
    assertTrue(Arrays.equals(request.get(), "hello world".getBytes()));
    assertEquals(address1.address(), sender.get().address());
  }

  @Test
  public void testTransientSendAndReceive() {
    String subject = nextSubject();
    AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    AtomicReference<byte[]> request = new AtomicReference<>();
    AtomicReference<Address> sender = new AtomicReference<>();

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
      handlerInvoked.set(true);
      sender.set(ep);
      request.set(data);
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, MoreExecutors.directExecutor());

    CompletableFuture<byte[]> response = netty1.sendAndReceive(address2, subject, "hello world".getBytes(), false);
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertTrue(handlerInvoked.get());
    assertTrue(Arrays.equals(request.get(), "hello world".getBytes()));
    assertEquals(address1.address(), sender.get().address());
  }

  @Test
  public void testSendAndReceiveWithFixedTimeout() {
    String subject = nextSubject();
    BiFunction<Address, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
    netty2.registerHandler(subject, handler);

    try {
      netty1.sendAndReceive(address2, subject, "hello world".getBytes(), Duration.ofSeconds(1)).join();
      fail();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void testSendAndReceiveWithDynamicTimeout() {
    String subject = nextSubject();
    BiFunction<Address, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
    netty2.registerHandler(subject, handler);

    try {
      netty1.sendAndReceive(address2, subject, "hello world".getBytes()).join();
      fail();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  @Ignore
  public void testSendAndReceiveWithExecutor() {
    String subject = nextSubject();
    ExecutorService completionExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "completion-thread"));
    ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "handler-thread"));
    AtomicReference<String> handlerThreadName = new AtomicReference<>();
    AtomicReference<String> completionThreadName = new AtomicReference<>();

    final CountDownLatch latch = new CountDownLatch(1);

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
      handlerThreadName.set(Thread.currentThread().getName());
      try {
        latch.await();
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        fail("InterruptedException");
      }
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, handlerExecutor);

    CompletableFuture<byte[]> response = netty1.sendAndReceive(address2,
        subject,
        "hello world".getBytes(),
        completionExecutor);
    response.whenComplete((r, e) -> {
      completionThreadName.set(Thread.currentThread().getName());
    });
    latch.countDown();

    // Verify that the message was request handling and response completion happens on the correct thread.
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertEquals("completion-thread", completionThreadName.get());
    assertEquals("handler-thread", handlerThreadName.get());
  }

  @Test
  public void testSendStream() throws Exception {
    String subject;
    byte[] payload = "Hello world!".getBytes();
    byte[] response;

    subject = nextSubject();
    netty2.registerStreamHandler(subject, a -> new StreamFunction<byte[], CompletableFuture<byte[]>>() {
      private byte[] value;

      @Override
      public void next(byte[] value) {
        this.value = value;
      }

      @Override
      public CompletableFuture<byte[]> complete() {
        return CompletableFuture.completedFuture(value);
      }

      @Override
      public CompletableFuture<byte[]> error(Throwable error) {
        return Futures.exceptionalFuture(error);
      }
    });
    StreamFunction<byte[], CompletableFuture<byte[]>> stream = netty1.sendStreamAndReceive(address2, subject).get(10, TimeUnit.SECONDS);
    stream.next(payload);
    response = stream.complete().get(10, TimeUnit.SECONDS);
    assertArrayEquals(payload, response);
  }

  @Test
  public void testReceiveStream() throws Exception {
    String subject;
    byte[] payload = "Hello world!".getBytes();

    subject = nextSubject();
    netty2.registerStreamingHandler(subject, (address, bytes, handler) -> {
      handler.next(bytes);
      handler.complete();
    });

    CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<byte[]> response = new AtomicReference<>();
    netty1.sendAndReceiveStream(address2, subject, payload, new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        response.set(value);
      }

      @Override
      public void complete() {
        latch.countDown();
      }

      @Override
      public void error(Throwable error) {
        latch.countDown();
      }
    });
    latch.await(10, TimeUnit.SECONDS);
    assertArrayEquals(payload, response.get());
  }

  @Test
  public void testSendAndReceiveStream() throws Exception {
    String subject;
    byte[] payload = "Hello world!".getBytes();

    subject = nextSubject();

    netty2.registerStreamingStreamHandler(subject, (address, handler) -> new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        handler.next(value);
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        handler.error(error);
      }
    });

    CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<byte[]> response = new AtomicReference<>();
    StreamHandler<byte[]> handler = netty1.sendStreamAndReceiveStream(address2, subject, new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        response.set(value);
      }

      @Override
      public void complete() {
        latch.countDown();
      }

      @Override
      public void error(Throwable error) {
        latch.countDown();
      }
    }).get(10, TimeUnit.SECONDS);
    handler.next(payload);
    handler.complete();
    latch.await(10, TimeUnit.SECONDS);
    assertArrayEquals(payload, response.get());
  }

  @Test
  public void testV2() throws Exception {
    String subject;
    byte[] payload = "Hello world!".getBytes();
    byte[] response;

    subject = nextSubject();
    nettyv21.registerHandler(subject, (address, bytes) -> CompletableFuture.completedFuture(bytes));
    response = nettyv22.sendAndReceive(addressv21, subject, payload).get(10, TimeUnit.SECONDS);
    assertArrayEquals(payload, response);
  }

  private static int findAvailablePort(int defaultPort) {
    try {
      ServerSocket socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ex) {
      return defaultPort;
    }
  }
}
