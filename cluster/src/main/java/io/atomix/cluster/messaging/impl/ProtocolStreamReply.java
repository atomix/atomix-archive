package io.atomix.cluster.messaging.impl;

/**
 * Protocol stream reply.
 */
public class ProtocolStreamReply extends ProtocolMessage {
  public ProtocolStreamReply(long id) {
    super(id);
  }

  @Override
  public Type type() {
    return Type.STREAM_REPLY;
  }
}
