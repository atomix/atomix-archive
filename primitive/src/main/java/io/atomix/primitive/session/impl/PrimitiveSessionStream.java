package io.atomix.primitive.session.impl;

import java.util.LinkedList;
import java.util.Queue;

import com.google.protobuf.ByteString;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.StreamHandler;

import static com.google.common.base.Preconditions.checkState;

/**
 * Primitive session stream.
 */
public class PrimitiveSessionStream implements StreamHandler<byte[]> {
  private final long streamId;
  private final PrimitiveStreamRegistry registry;
  private final StateMachine.Context context;
  private StreamHandler<SessionStreamResponse> handler;
  private long currentSequence;
  private long completeSequence;
  private long lastIndex;
  private final Queue<EventHolder> events = new LinkedList<>();
  private boolean open = true;

  public PrimitiveSessionStream(
      long streamId,
      PrimitiveStreamRegistry registry,
      StateMachine.Context context) {
    this.streamId = streamId;
    this.registry = registry;
    this.context = context;
    registry.register(this);
  }

  /**
   * Returns the stream ID.
   *
   * @return the stream ID
   */
  public long id() {
    return streamId;
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
    return !events.isEmpty() ? lastIndex : context.getIndex();
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
  public void next(byte[] value) {
    if (!open) {
      return;
    }

    // If the event is being published during a read operation, throw an exception.
    checkState(context.getOperationType() == OperationType.COMMAND, "session events can only be published during command execution");

    // If the client acked a sequence number greater than the current event sequence number since we know the
    // client must have received it from another server.
    long sequence = ++currentSequence;
    if (completeSequence > sequence) {
      return;
    }

    lastIndex = context.getIndex();
    EventHolder event = new EventHolder(sequence, lastIndex, value);
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
    return context.getIndex();
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
              .setStreamId(streamId)
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
