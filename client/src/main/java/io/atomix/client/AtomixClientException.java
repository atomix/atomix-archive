package io.atomix.client;

/**
 * Client exception.
 */
public class AtomixClientException extends RuntimeException {
  public AtomixClientException() {
  }

  public AtomixClientException(String message) {
    super(message);
  }

  public AtomixClientException(String message, Throwable cause) {
    super(message, cause);
  }

  public AtomixClientException(Throwable cause) {
    super(cause);
  }
}
