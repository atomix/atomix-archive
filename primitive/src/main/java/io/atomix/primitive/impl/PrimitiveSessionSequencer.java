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
package io.atomix.primitive.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.common.annotations.VisibleForTesting;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

/**
 * Client response sequencer.
 * <p>
 * The way operations are applied to replicated state machines, allows responses to be handled in a consistent
 * manner. Command responses will always have an {@code eventIndex} less than the response {@code index}. This is
 * because commands always occur <em>before</em> the events they trigger, and because events are always associated
 * with a command index and never a query index, the previous {@code eventIndex} for a command response will always be less
 * than the response {@code index}.
 * <p>
 * Alternatively, the previous {@code eventIndex} for a query response may be less than or equal to the response
 * {@code index}. However, in contrast to commands, queries always occur <em>after</em> prior events. This means
 * for a given index, the precedence is command -> event -> query.
 * <p>
 * Since operations for an index will always occur in a consistent order, sequencing operations is a trivial task.
 * When a response is received, once the response is placed in sequential order, pending events up to the response's
 * {@code eventIndex} may be completed. Because command responses will never have an {@code eventIndex} equal to their
 * own response {@code index}, events will always stop prior to the command. But query responses may have an
 * {@code eventIndex} equal to their own response {@code index}, and in that case the event will be completed prior
 * to the completion of the query response.
 * <p>
 * Events can also be received later than sequenced operations. When an event is received, it's first placed in
 * sequential order as is the case with operation responses. Once placed in sequential order, if no requests are
 * outstanding, the event is immediately completed. This ensures that events that are published during a period
 * of inactivity in the session can still be completed upon reception since the event is guaranteed not to have
 * occurred concurrently with any other operation. If requests for the session are outstanding, the event is placed
 * in a queue and the algorithm for checking sequenced responses is run again.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class PrimitiveSessionSequencer {
  private final Logger log;
  private final PrimitiveSessionState state;
  @VisibleForTesting
  long requestSequence;
  @VisibleForTesting
  long responseSequence;
  @VisibleForTesting
  long streamIndex;
  private final Map<Long, StreamSequencer> streams = new HashMap<>();
  private final Map<Long, ResponseCallback> responseCallbacks = new HashMap<>();

  PrimitiveSessionSequencer(PrimitiveSessionState state, ManagedPrimitiveContext context) {
    this.state = state;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(state.getSessionId())
        .add("type", context.type().name())
        .add("name", context.name())
        .build());
  }

  /**
   * Returns the next request sequence number.
   *
   * @return The next request sequence number.
   */
  public long nextRequest() {
    return ++requestSequence;
  }

  /**
   * Sequences an event.
   * <p>
   * This method relies on the session event protocol to ensure that events are applied in sequential order.
   * When an event is received, if no operations are outstanding, the event is immediately completed since
   * the event could not have occurred concurrently with any other operation. Otherwise, the event is queued
   * and the next response in the sequence of responses is checked to determine whether the event can be
   * completed.
   *
   * @param context  The publish request.
   * @param callback The callback to sequence.
   */
  public void sequenceStream(SessionStreamContext context, Runnable callback) {
    streams.computeIfAbsent(context.getStreamId(), StreamSequencer::new)
        .sequenceEvent(context, callback);
  }

  /**
   * Sequences a response.
   * <p>
   * When an operation is sequenced, it's first sequenced in the order in which it was submitted to the cluster.
   * Once placed in sequential request order, if a response's {@code eventIndex} is greater than the last completed
   * {@code eventIndex}, we attempt to sequence pending events. If after sequencing pending events the response's
   * {@code eventIndex} is equal to the last completed {@code eventIndex} then the response can be immediately
   * completed. If not enough events are pending to meet the sequence requirement, the sequencing of responses is
   * stopped until events are received.
   *
   * @param sequence The request sequence number.
   * @param context  The response to sequence.
   * @param callback The callback to sequence.
   */
  public void sequenceResponse(long sequence, SessionResponseContext context, Runnable callback) {
    // If the request sequence number is equal to the next response sequence number, attempt to complete the response.
    if (sequence == responseSequence + 1) {
      if (completeResponse(context, callback)) {
        ++responseSequence;
        completeResponses();
      } else {
        responseCallbacks.put(sequence, new ResponseCallback(context, callback));
      }
    }
    // If the response has not yet been sequenced, store it in the response callbacks map.
    // Otherwise, the response for the operation with this sequence number has already been handled.
    else if (sequence > responseSequence) {
      responseCallbacks.put(sequence, new ResponseCallback(context, callback));
    }
  }

  /**
   * Completes all sequenced responses.
   */
  private void completeResponses() {
    // Iterate through queued responses and complete as many as possible.
    ResponseCallback response = responseCallbacks.get(responseSequence + 1);
    while (response != null) {
      // If the response was completed, remove the response callback from the response queue,
      // increment the response sequence number, and check the next response.
      if (completeResponse(response.context, response.callback)) {
        responseCallbacks.remove(++responseSequence);
        response = responseCallbacks.get(responseSequence + 1);
      } else {
        break;
      }
    }

    // Once we've completed as many responses as possible, if no more operations are outstanding
    // and events remain in the event queue, complete the events.
    if (requestSequence == responseSequence) {
      for (StreamSequencer stream : streams.values()) {
        stream.completeStream();
      }
    }
  }

  /**
   * Completes a sequenced response if possible.
   */
  private boolean completeResponse(SessionResponseContext response, Runnable callback) {
    // If the response is null, that indicates an exception occurred. The best we can do is complete
    // the response in sequential order.
    if (response == null) {
      log.trace("Completing failed request");
      callback.run();
      return true;
    }

    // Iterate through all streams in the response context and complete streams up to the stream sequence number.
    boolean complete = true;
    for (SessionStreamContext stream : response.getStreamsList()) {
      complete = complete && completeStream(stream);
    }

    // If after completing pending events the eventIndex is greater than or equal to the response's eventIndex, complete the response.
    // Note that the event protocol initializes the eventIndex to the session ID.
    if (complete) {
      log.trace("Completing response {}", response);
      callback.run();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Completes sequenced values in the given stream.
   */
  private boolean completeStream(SessionStreamContext stream) {
    StreamSequencer sequencer = streams.get(stream.getStreamId());
    if (sequencer == null) {
      return stream.getSequence() == 0;
    }
    return sequencer.completeStream(stream);
  }

  /**
   * Returns the collection of stream sequencers.
   *
   * @return the collection of stream sequencer
   */
  public Collection<StreamSequencer> streams() {
    return streams.values();
  }

  /**
   * Closes the given stream.
   *
   * @param streamId the stream ID
   * @param callback the callback to run once the stream has been closed
   */
  public void closeStream(long streamId, Runnable callback) {
    StreamSequencer stream = streams.get(streamId);
    if (stream != null) {
      stream.close(callback);
    }
  }

  /**
   * Response callback holder.
   */
  private static final class ResponseCallback implements Runnable {
    private final SessionResponseContext context;
    private final Runnable callback;

    private ResponseCallback(SessionResponseContext context, Runnable callback) {
      this.context = context;
      this.callback = callback;
    }

    @Override
    public void run() {
      callback.run();
    }
  }

  /**
   * Stream sequencer.
   */
  public class StreamSequencer {
    private final long streamId;
    private long streamIndex;
    private long streamSequence;
    private long completeSequence;
    private final Queue<EventCallback> eventCallbacks = new LinkedList<>();
    private Runnable closeCallback;

    StreamSequencer(long streamId) {
      this.streamId = streamId;
    }

    /**
     * Returns the stream ID.
     *
     * @return the stream ID
     */
    public long streamId() {
      return streamId;
    }

    /**
     * Returns the highest sequence number received in order on the stream.
     *
     * @return the highest sequence number received in order on the stream
     */
    public long getStreamSequence() {
      return streamSequence;
    }

    /**
     * Sequences the given event.
     *
     * @param context  the stream context
     * @param callback the callback to execute
     */
    void sequenceEvent(SessionStreamContext context, Runnable callback) {
      // If the sequence number is equal to the next stream sequence number, accept the event.
      if (context.getSequence() == streamSequence + 1) {
        streamSequence = context.getSequence();
        if (requestSequence == responseSequence) {
          log.trace("Completing {}", context);
          callback.run();
          completeSequence = context.getSequence();
        } else {
          eventCallbacks.add(new EventCallback(context, callback));
          completeResponses();
        }
      }
    }

    /**
     * Completes the stream up to the given context.
     *
     * @param context the stream context
     * @return indicates whether the stream was completed up to the given context
     */
    boolean completeStream(SessionStreamContext context) {
      // For each pending event with an eventIndex less than or equal to the response eventIndex, complete the event.
      // This is safe since we know that sequenced responses should see sequential order of events.
      EventCallback eventCallback = eventCallbacks.peek();
      while (eventCallback != null && eventCallback.event.getSequence() <= context.getSequence()) {
        eventCallbacks.remove();
        log.trace("Completing event {}", eventCallback.event);
        eventCallback.run();
        completeSequence = eventCallback.event.getSequence();
        if (eventCallback.event.getSequence() == context.getSequence()) {
          if (closeCallback != null) {
            closeCallback.run();
          }
          return true;
        }
        eventCallback = eventCallbacks.peek();
      }
      return completeSequence == context.getSequence();
    }

    /**
     * Completes all pending events in the stream.
     */
    void completeStream() {
      EventCallback eventCallback = eventCallbacks.poll();
      while (eventCallback != null) {
        log.trace("Completing {}", eventCallback.event);
        eventCallback.run();
        completeSequence = eventCallback.event.getSequence();
        eventCallback = eventCallbacks.poll();
      }
      if (closeCallback != null) {
        closeCallback.run();
      }
    }

    /**
     * Closes the stream.
     *
     * @param callback the callback to run once the stream is closed
     */
    void close(Runnable callback) {
      closeCallback = callback;
    }
  }

  /**
   * Event callback holder.
   */
  private static final class EventCallback implements Runnable {
    private final SessionStreamContext event;
    private final Runnable callback;

    private EventCallback(SessionStreamContext event, Runnable callback) {
      this.event = event;
      this.callback = callback;
    }

    @Override
    public void run() {
      callback.run();
    }
  }

}
