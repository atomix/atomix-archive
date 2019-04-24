package io.atomix.cluster.messaging.impl;

import io.atomix.utils.net.Address;

/**
 * Protocol stream request.
 */
public class ProtocolStreamRequest extends ProtocolMessage {
  private final Address sender;
  private final String subject;

  public ProtocolStreamRequest(long id, Address sender, String subject) {
    super(id);
    this.sender = sender;
    this.subject = subject;
  }

  @Override
  public Type type() {
    return Type.STREAM_REQUEST;
  }

  public String subject() {
    return subject;
  }

  public Address sender() {
    return sender;
  }

}
