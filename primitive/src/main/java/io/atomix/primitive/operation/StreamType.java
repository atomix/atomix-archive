package io.atomix.primitive.operation;

import java.util.Objects;

import io.atomix.utils.Identifier;

/**
 * Stream type.
 */
public class StreamType<T> implements Identifier<String> {
  private final String name;

  public StreamType(String name) {
    this.name = name;
  }

  @Override
  public String id() {
    return name;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof StreamType) {
      StreamType that = (StreamType) object;
      return this.id().equals(that.id());
    }
    return false;
  }
}
