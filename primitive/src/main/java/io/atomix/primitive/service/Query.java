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
package io.atomix.primitive.service;

import java.util.function.Function;

/**
 * State machine query.
 */
public class Query<T> extends Operation<T> {
  public Query(long index, long timestamp, T value) {
    super(index, timestamp, value);
  }

  @Override
  public Type type() {
    return Type.COMMAND;
  }

  @Override
  public <U> Query<U> map(Function<T, U> mapper) {
    return new Query<>(index(), timestamp(), mapper.apply(value()));
  }
}
