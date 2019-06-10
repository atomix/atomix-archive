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
package io.atomix.service;

import io.atomix.utils.NamedType;

/**
 * Base class for user-provided services.
 */
public interface PrimitiveService extends StateMachine {

  /**
   * Service type.
   */
  interface Type extends NamedType {

    /**
     * Creates a new service instance from the given configuration.
     *
     * @return the service instance
     */
    PrimitiveService newService();
  }

  /**
   * Primitive service context.
   */
  interface Context extends StateMachine.Context {
  }

}
