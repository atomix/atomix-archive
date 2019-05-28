package io.atomix.cluster.grpc;

import io.atomix.cluster.MemberId;

/**
 * Service factory.
 */
public interface ServiceFactory<T> {

  /**
   * Returns a service for the given member.
   *
   * @param memberId the member for which to return the service
   * @return the service for the given member
   */
  T getService(MemberId memberId);

}
