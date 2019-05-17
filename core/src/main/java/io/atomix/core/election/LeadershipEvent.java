/*
 * Copyright 2014-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import io.atomix.utils.event.AbstractEvent;

/**
 * Describes leadership election event.
 */
public class LeadershipEvent<T> extends AbstractEvent<LeadershipEvent.Type, Leadership> {

  /**
   * Type of leadership events.
   */
  public enum Type {
    /**
     * Leader changed event.
     */
    CHANGED,
  }

  private final Leadership<T> leadership;

  /**
   * Creates an event of a given type and for the specified instance and the
   * current time.
   *
   * @param type       leadership event type
   * @param leadership new leadership
   */
  public LeadershipEvent(Type type, Leadership<T> leadership) {
    this(type, leadership, System.currentTimeMillis());
  }

  /**
   * Creates an event of a given type and for the specified subject and time.
   *
   * @param type       leadership event type
   * @param leadership new leadership
   * @param time       occurrence time
   */
  public LeadershipEvent(Type type, Leadership<T> leadership, long time) {
    super(type, leadership, time);
    this.leadership = leadership;
  }

  /**
   * Returns the new leadership for the topic.
   *
   * @return the new leadership for the topic
   */
  public Leadership<T> leadership() {
    return subject();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type(), subject(), time());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof LeadershipEvent) {
      final LeadershipEvent other = (LeadershipEvent) obj;
      return Objects.equals(this.type(), other.type())
          && Objects.equals(this.subject(), other.subject())
          && Objects.equals(this.time(), other.time());
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("type", type())
        .add("leadership", leadership())
        .add("time", time())
        .toString();
  }
}