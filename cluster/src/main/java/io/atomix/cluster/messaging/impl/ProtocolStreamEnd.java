package io.atomix.cluster.messaging.impl;

/**
 * Protocol stream end.
 */
public class ProtocolStreamEnd extends ProtocolMessage {
  private final ProtocolStatus status;

  public ProtocolStreamEnd(long id, ProtocolStatus status) {
    super(id);
    this.status = status;
  }

  @Override
  public Type type() {
    return Type.STREAM_END;
  }

  public ProtocolStatus status() {
    return status;
  }
}
