package io.atomix.primitive.session.impl;

import java.util.LinkedList;
import java.util.Queue;

import com.google.protobuf.ByteString;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.SessionStreamHandler;
import io.atomix.primitive.session.StreamId;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.utils.stream.StreamHandler;

import static com.google.common.base.Preconditions.checkState;

/**
 * Primitive session stream.
 */
public class PrimitiveSessionStream<T> implements SessionStreamHandler<T> {
  private final StreamId streamId;
  private final OperationId<?, T> operationId;
  private final PrimitiveStreamRegistry registry;
  private final DefaultServiceExecutor executor;
  private final ByteArrayEncoder<T> encoder;
  private StreamHandler<SessionStreamResponse> handler;
  private long currentSequence;
  private long completeSequence;
  private long lastIndex;
  private final Queue<EventHolder> events = new LinkedList<>();
  private boolean open = true;

  public PrimitiveSessionStream(
      StreamId streamId,
      OperationId<?, T> operationId,
      PrimitiveStreamRegistry registry,
      DefaultServiceExecutor executor) {
    this.streamId = streamId;
    this.operationId = operationId;
    this.registry = registry;
    this.executor = executor;
    this.encoder = executor.encoder(operationId);
    registry.register(this);
  }

  /**
   * Returns the stream ID.
   *
   * @return the stream ID
   */
  public StreamId id() {
    return streamId;
  }

  /**
   * Returns the stream operation ID.
   *
   * @return the stream operation ID
   */
  public OperationId<?, T> operationId() {
    return operationId;
  }

  /**
   * Returns the stream sequence number.
   *
   * @return the stream sequence number
   */
  public long getCurrentSequence() {
    return currentSequence;
  }

  /**
   * Sets the stream sequence number.
   *
   * @param currentSequence the stream sequence number
   */
  public void setCurrentSequence(long currentSequence) {
    this.currentSequence = currentSequence;
  }

  /**
   * Returns the highest completed sequence number.
   *
   * @return the highest completed sequence number
   */
  public long getCompleteSequence() {
    return completeSequence;
  }

  /**
   * Sets the highest completed sequence number.
   *
   * @param completeSequence the highest completed sequence number
   */
  public void setCompleteSequence(long completeSequence) {
    this.completeSequence = completeSequence;
  }

  /**
   * Returns the last index enqueued in the stream.
   *
   * @return the last index enqueued in the stream
   */
  public long getLastIndex() {
    return !events.isEmpty() ? lastIndex : executor.getIndex();
  }

  /**
   * Sets the stream handler.
   *
   * @param handler the stream handler
   */
  public void handler(StreamHandler<SessionStreamResponse> handler) {
    this.handler = handler;
  }

  @Override
  public void next(T value) {
    if (!open) {
      return;
    }

    // If the event is being published during a read operation, throw an exception.
    checkState(executor.getOperationType() == OperationType.COMMAND, "session events can only be published during command execution");

    // If the client acked a sequence number greater than the current event sequence number since we know the
    // client must have received it from another server.
    long sequence = ++currentSequence;
    if (completeSequence > sequence) {
      return;
    }

    lastIndex = executor.getIndex();
    EventHolder event = new EventHolder(sequence, lastIndex, ByteArrayEncoder.encode(value, encoder));
    events.add(event);
    sendEvent(event);
  }

  @Override
  public void complete() {
    if (open) {
      open = false;
      registry.unregister(this);
      if (handler != null) {
        handler.complete();
      }
    }
  }

  @Override
  public void error(Throwable error) {
    if (open) {
      open = false;
      registry.unregister(this);
      if (handler != null) {
        handler.error(error);
      }
    }
  }

  /**
   * Returns the index of the highest event acked for the session.
   *
   * @return The index of the highest event acked for the session.
   */
  public long getCompleteIndex() {
    EventHolder event = events.peek();
    if (event != null) {
      return event.index - 1;
    }
    return executor.getIndex();
  }

  /**
   * Clears events up to the given sequence.
   *
   * @param sequence The sequence number to clear.
   */
  private void clearEvents(long sequence) {
    if (sequence > completeSequence) {
      EventHolder event = events.peek();
      while (event != null && event.sequence <= sequence) {
        events.remove();
        completeSequence = event.sequence;
        event = events.peek();
      }
      completeSequence = sequence;
    }
  }

  /**
   * Resends events from the given sequence.
   *
   * @param sequence The sequence from which to resend events.
   */
  public void resendEvents(long sequence) {
    clearEvents(sequence);
    for (EventHolder event : events) {
      sendEvent(event);
    }
  }

  /**
   * Sends an event through the handler.
   */
  private void sendEvent(EventHolder event) {
    if (handler != null) {
      handler.next(SessionStreamResponse.newBuilder()
          .setContext(SessionStreamContext.newBuilder()
              .setStreamId(streamId.streamId())
              .setIndex(event.index)
              .setSequence(event.sequence)
              .build())
          .setValue(ByteString.copyFrom(event.event))
          .build());
    }
  }

  /**
   * Event holder.
   */
  private static class EventHolder {
    private final long sequence;
    private final long index;
    private final byte[] event;

    EventHolder(long sequence, long index, byte[] event) {
      this.sequence = sequence;
      this.index = index;
      this.event = event;
    }
  }
}
