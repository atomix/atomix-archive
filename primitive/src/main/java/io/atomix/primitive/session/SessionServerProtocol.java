package io.atomix.primitive.session;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.session.impl.EventRequest;

/**
 * Primitive server protocol.
 */
public interface SessionServerProtocol {

  /**
   * Sends an event request to the given member.
   *
   * @param memberId  the member ID
   * @param request   the request to send
   */
  void event(MemberId memberId, EventRequest request);

}
