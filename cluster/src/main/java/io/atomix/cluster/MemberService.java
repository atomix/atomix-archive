package io.atomix.cluster;

/**
 * Local member service.
 */
public interface MemberService {

  /**
   * Returns the local member.
   *
   * @return the local member
   */
  Member getLocalMember();

}
