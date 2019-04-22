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
package io.atomix.primitive.proxy;

import io.atomix.primitive.client.PrimitiveClient;
import io.atomix.primitive.client.impl.DefaultPrimitiveClient;

/**
 * Base class for primitive proxies.
 */
public abstract class SimplePrimitiveProxy extends AbstractPrimitiveProxy<PrimitiveClient> {
  protected SimplePrimitiveProxy(Context context) {
    super(context);
  }

  @Override
  protected PrimitiveClient createClient() {
    return new DefaultPrimitiveClient(serviceId(), getPartitionClient(), context());
  }
}