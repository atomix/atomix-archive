/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.service.operation;

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
