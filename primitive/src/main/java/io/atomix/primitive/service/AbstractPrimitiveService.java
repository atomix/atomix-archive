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
package io.atomix.primitive.service;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.time.Clock;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

/**
 * Raft service.
 */
public abstract class AbstractPrimitiveService implements PrimitiveService {
  private Logger log;
  private ServiceContext context;
  private ServiceExecutor executor;
  private final Map<SessionId, Session> sessions = Maps.newHashMap();

  @Override
  public final void init(ServiceContext context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(context.serviceId())
        .add("type", context.serviceType())
        .add("name", context.serviceName())
        .build());
    configure(executor);
  }

  @Override
  public final void tick(WallClockTimestamp timestamp) {
    executor.tick(timestamp);
  }

  @Override
  public final byte[] apply(Commit<byte[]> commit) {
    return executor.apply(commit);
  }

  /**
   * Configures the state machine.
   * <p>
   * By default, this method will configure state machine operations by extracting public methods with
   * a single {@link Commit} parameter via reflection. Override this method to explicitly register
   * state machine operations via the provided {@link ServiceExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected abstract void configure(ServiceExecutor executor);

  /**
   * Returns the service logger.
   *
   * @return the service logger
   */
  protected Logger getLogger() {
    return log;
  }

  /**
   * Returns the state machine scheduler.
   *
   * @return The state machine scheduler.
   */
  protected Scheduler getScheduler() {
    return executor;
  }

  /**
   * Returns the ID of the cluster member this service instance is running on.
   * Caution: This information should not be used in anyway to modify the machine's state,
   * as it could be used to violate the invariant that all instances of a partition must
   * have the same state.
   * However, it can be used safely for logging purposes or for generating meaningful
   * filenames for instance (this can be useful especially in the case where several
   * cluster members are run on the same host).
   * @return The local member ID
   */
  protected MemberId getLocalMemberId() {
    return context.localMemberId();
  }

  /**
   * Returns the unique state machine identifier.
   *
   * @return The unique state machine identifier.
   */
  protected PrimitiveId getServiceId() {
    return context.serviceId();
  }

  /**
   * Returns the unique state machine name.
   *
   * @return The unique state machine name.
   */
  protected String getServiceName() {
    return context.serviceName();
  }

  /**
   * Returns the state machine's current index.
   *
   * @return The state machine's current index.
   */
  protected long getCurrentIndex() {
    return context.currentIndex();
  }

  /**
   * Returns the current session.
   *
   * @return the current session
   */
  protected Session getCurrentSession() {
    return getSession(context.currentSession().sessionId());
  }

  /**
   * Returns the state machine's clock.
   *
   * @return The state machine's clock.
   */
  protected Clock getClock() {
    return getWallClock();
  }

  /**
   * Returns the state machine's wall clock.
   *
   * @return The state machine's wall clock.
   */
  protected WallClock getWallClock() {
    return context.wallClock();
  }

  /**
   * Returns the state machine's logical clock.
   *
   * @return The state machine's logical clock.
   */
  protected LogicalClock getLogicalClock() {
    return context.logicalClock();
  }

  /**
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected Session getSession(long sessionId) {
    return getSession(SessionId.from(sessionId));
  }

  /**
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected Session getSession(SessionId sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns the collection of open sessions.
   *
   * @return the collection of open sessions
   */
  protected Collection<Session> getSessions() {
    return sessions.values();
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void register(Session session) {
    sessions.put(session.sessionId(), session);
    onOpen(session);
  }

  @Override
  public final void expire(SessionId sessionId) {
    Session session = sessions.remove(sessionId);
    if (session != null) {
      onExpire(session);
    }
  }

  @Override
  public final void close(SessionId sessionId) {
    Session session = sessions.remove(sessionId);
    if (session != null) {
      onClose(session);
    }
  }

  /**
   * Called when a new session is registered.
   * <p>
   * A session is registered when a new client connects to the cluster or an existing client recovers its
   * session after being partitioned from the cluster. It's important to note that when this method is called,
   * the {@link Session} is <em>not yet open</em> and so events cannot be {@link Session#publish(PrimitiveEvent) published}
   * to the registered session. This is because clients cannot reliably track messages pushed from server state machines
   * to the client until the session has been fully registered. Session event messages may still be published to
   * other already-registered sessions in reaction to a session being registered.
   * <p>
   * To push session event messages to a client through its session upon registration, state machines can
   * use an asynchronous callback or schedule a callback to send a message.
   * <pre>
   *   {@code
   *   public void onOpen(RaftSession session) {
   *     executor.execute(() -> session.publish("foo", "Hello world!"));
   *   }
   *   }
   * </pre>
   * Sending a session event message in an asynchronous callback allows the server time to register the session
   * and notify the client before the event message is sent. Published event messages sent via this method will
   * be sent the next time an operation is applied to the state machine.
   *
   * @param session The session that was registered
   */
  protected void onOpen(Session session) {

  }

  /**
   * Called when a session is expired by the system.
   * <p>
   * This method is called when a client fails to keep its session alive with the cluster. If the leader hasn't heard
   * from a client for a configurable time interval, the leader will expire the session to free the related memory.
   * This method will always be called for a given session before {@link #onClose(Session)}, and {@link #onClose(Session)}
   * will always be called following this method.
   *
   * @param session The session that was expired
   */
  protected void onExpire(Session session) {

  }

  /**
   * Called when a session was closed by the client.
   * <p>
   * This method is called when a client explicitly closes a session.
   *
   * @param session The session that was closed
   */
  protected void onClose(Session session) {

  }
}
