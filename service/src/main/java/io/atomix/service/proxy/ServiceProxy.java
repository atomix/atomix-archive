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
package io.atomix.service.proxy;

import java.util.concurrent.CompletableFuture;

/**
 * Primitive proxy.
 */
public interface ServiceProxy {

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the service type.
   *
   * @return the service type
   */
  String type();

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  CompletableFuture<Void> delete();

}
