package io.atomix.protocols.log;

/**
 * Distributed log exception.
 */
public abstract class DistributedLogException extends RuntimeException {

  /**
   * Exception indicating the distributed log is unavailable.
   */
  public static class Unavailable extends DistributedLogException {
  }
}
