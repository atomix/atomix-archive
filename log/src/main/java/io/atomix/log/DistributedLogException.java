package io.atomix.log;

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
