package io.atomix.client.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;

/**
 * Primitive partition.
 */
public class PrimitivePartition {
  private final int partitionId;
  private long sessionId;
  private long index;
  private long commandRequest;
  private long commandResponse;
  private final Map<Long, PrimitiveStream> streams = new ConcurrentHashMap<>();

  public PrimitivePartition(int partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Initializes the partition.
   *
   * @param header the header with which to initialize the partition
   */
  public void init(ResponseHeader header) {
    index = header.getIndex();
  }

  /**
   * Returns the next request header.
   *
   * @return the next request header
   */
  public RequestHeader getRequestHeader() {
    return RequestHeader.newBuilder()
        .setPartitionId(partitionId)
        .setIndex(index)
        .build();
  }

  /**
   * Returns the next session header.
   *
   * @return the next session header
   */
  public SessionHeader getSessionHeader() {
    return SessionHeader.newBuilder()
        .setPartitionId(partitionId)
        .setSessionId(sessionId)
        .build();
  }

  /**
   * Returns the next command header.
   *
   * @return the next command header
   */
  public SessionCommandHeader getCommandHeader() {
    return SessionCommandHeader.newBuilder()
        .setPartitionId(partitionId)
        .setSessionId(sessionId)
        .setSequenceNumber(++commandRequest)
        .build();
  }

  /**
   * Returns the next query header.
   *
   * @return the next query header
   */
  public SessionQueryHeader getQueryHeader() {
    return SessionQueryHeader.newBuilder()
        .setPartitionId(partitionId)
        .setSessionId(sessionId)
        .setLastIndex(index)
        .setLastSequenceNumber(commandRequest)
        .build();
  }

  /**
   * Returns a stream by ID.
   *
   * @param streamId the stream ID
   * @return the primitive stream context
   */
  public PrimitiveStream getStream(long streamId) {
    PrimitiveStream stream = streams.get(streamId);
    if (stream == null) {
      stream = streams.computeIfAbsent(streamId, id -> new PrimitiveStream(streamId));
    }
    return stream;
  }

  private final Map<Long, ResponseCallback> responseCallbacks = new ConcurrentHashMap<>();

  /**
   * Returns a response future that will be completed in the order specified by the given response header.
   *
   * @param response the response value
   * @param header   the response header
   * @param <T>      the response type
   * @return a future to be completed in response order
   */
  public <T> CompletableFuture<T> order(T response, SessionResponseHeader header) {
    index = Math.max(index, header.getIndex());
    commandResponse = header.getSequenceNumber();
    return CompletableFuture.completedFuture(response);
  }

  /**
   * Returns a response future that will be completed in the order specified by the given response header.
   *
   * @param response the response value
   * @param header   the response header
   * @param <T>      the response type
   * @return a future to be completed in response order
   */
  public <T> CompletableFuture<T> complete(T response, ResponseHeader header) {
    update(header);
    return CompletableFuture.completedFuture(response);
  }

  /**
   * Updates the partition state with the given response header.
   *
   * @param header the response header
   */
  void update(ResponseHeader header) {
    index = Math.max(index, header.getIndex());
  }

  /**
   * Updates the partition state with the given response header.
   *
   * @param header the response header
   */
  void update(SessionHeader header) {
    sessionId = header.getSessionId();
  }

  /**
   * Updates the partition state with the given response header.
   *
   * @param header the response header
   */
  void update(SessionResponseHeader header) {
    sessionId = header.getSessionId();
    index = Math.max(index, header.getIndex());
    commandResponse = header.getSequenceNumber();
    for (SessionStreamHeader stream : header.getStreamsList()) {
      getStream(stream.getStreamId()).update(stream);
    }
  }

  /**
   * Response callback holder.
   */
  private static final class ResponseCallback {
    private final Object response;
    private final SessionResponseHeader header;

    public ResponseCallback(Object response, SessionResponseHeader header) {
      this.response = response;
      this.header = header;
    }

    /**
     * Completes the callback.
     */
    public void complete() {

    }
  }
}
