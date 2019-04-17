/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.session.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.service.EventRequest;
import io.atomix.primitive.service.PrimitiveEvent;
import io.atomix.primitive.service.ResetRequest;
import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session message listener.
 */
final class PrimitiveSessionListener {
  private final Logger log;
  private final ClusterCommunicationService communicationService;
  private final PrimitiveSessionState state;
  private final Map<String, Set<Consumer<PrimitiveEvent>>> eventListeners = Maps.newHashMap();
  private final PrimitiveSessionSequencer sequencer;
  private final Executor executor;
  private final String eventSubject;
  private final String resetSubject;

  PrimitiveSessionListener(
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      PrimitiveSessionState state,
      PrimitiveSessionSequencer sequencer,
      Executor executor) {
    this.communicationService = checkNotNull(communicationService, "clusterCommunictor cannot be null");
    this.state = checkNotNull(state, "state cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    this.executor = checkNotNull(executor, "executor cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(state.getSessionId())
        .add("type", state.getPrimitiveType())
        .add("name", state.getPrimitiveName())
        .build());
    eventSubject = String.format("%s-%d-event", membershipService.getLocalMember().id().id(), state.getSessionId().id());
    resetSubject = String.format("%s-%d-reset", membershipService.getLocalMember().id().id(), state.getSessionId().id());
    communicationService.subscribe(eventSubject, bytes -> {
      try {
        return EventRequest.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }, this::handlePublish, executor);
  }

  /**
   * Adds an event listener to the session.
   *
   * @param listener the event listener callback
   */
  public void addEventListener(String eventType, Consumer<PrimitiveEvent> listener) {
    executor.execute(() -> eventListeners.computeIfAbsent(eventType, e -> Sets.newLinkedHashSet()).add(listener));
  }

  /**
   * Removes an event listener from the session.
   *
   * @param listener the event listener callback
   */
  public void removeEventListener(String eventType, Consumer<PrimitiveEvent> listener) {
    executor.execute(() -> eventListeners.computeIfAbsent(eventType, e -> Sets.newLinkedHashSet()).remove(listener));
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   */
  @SuppressWarnings("unchecked")
  private void handlePublish(EventRequest request) {
    log.trace("Received {}", request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.getSessionId() != state.getSessionId().id()) {
      log.trace("Inconsistent session ID: {}", request.getSessionId());
      return;
    }

    // Store eventIndex in a local variable to prevent multiple volatile reads.
    long eventIndex = state.getEventIndex();

    // If the request event index has already been processed, return.
    if (request.getEventIndex() <= eventIndex) {
      log.trace("Duplicate event index {}", request.getEventIndex());
      return;
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.getPreviousIndex() != eventIndex) {
      log.trace("Inconsistent event index: {}", request.getPreviousIndex());
      ResetRequest resetRequest = ResetRequest.newBuilder()
          .setSessionId(state.getSessionId().id())
          .setEventIndex(eventIndex)
          .build();
      log.trace("Sending {}", resetRequest);
      communicationService.unicast(
          resetSubject,
          resetRequest,
          ResetRequest::toByteArray,
          MemberId.from(request.getServerId()));
      return;
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.getEventIndex());

    sequencer.sequenceEvent(request, () -> {
      for (PrimitiveEvent event : request.getEventsList()) {
        Set<Consumer<PrimitiveEvent>> listeners = eventListeners.get(event.getType());
        if (listeners != null) {
          for (Consumer<PrimitiveEvent> listener : listeners) {
            listener.accept(event);
          }
        }
      }
    });
  }

  /**
   * Closes the session event listener.
   *
   * @return A completable future to be completed once the listener is closed.
   */
  public CompletableFuture<Void> close() {
    communicationService.unsubscribe(eventSubject);
    return CompletableFuture.completedFuture(null);
  }
}
