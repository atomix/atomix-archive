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
package io.atomix.core.counter.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.component.Component;

/**
 * Counter service.
 */
public class CounterService extends AbstractCounterService {
  public static final Type TYPE = new Type();

  /**
   * Counter service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "counter";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new CounterService();
    }
  }

  private final AtomicLong counter = new AtomicLong();

  @Override
  public SetResponse set(SetRequest request) {
    return SetResponse.newBuilder()
        .setPreviousValue(counter.getAndSet(request.getValue()))
        .build();
  }

  @Override
  public GetResponse get(GetRequest request) {
    return GetResponse.newBuilder()
        .setValue(counter.get())
        .build();
  }

  @Override
  public CheckAndSetResponse checkAndSet(CheckAndSetRequest request) {
    return CheckAndSetResponse.newBuilder()
        .setSucceeded(counter.compareAndSet(request.getExpect(), request.getUpdate()))
        .build();
  }

  @Override
  public IncrementResponse increment(IncrementRequest request) {
    long previousValue;
    if (request.getDelta() == 0) {
      previousValue = counter.getAndIncrement();
    } else {
      previousValue = counter.getAndAdd(request.getDelta());
    }
    return IncrementResponse.newBuilder()
        .setPreviousValue(previousValue)
        .setNextValue(counter.get())
        .build();
  }

  @Override
  public DecrementResponse decrement(DecrementRequest request) {
    long previousValue;
    if (request.getDelta() == 0) {
      previousValue = counter.getAndDecrement();
    } else {
      previousValue = counter.getAndAdd(-request.getDelta());
    }
    return DecrementResponse.newBuilder()
        .setPreviousValue(previousValue)
        .setNextValue(counter.get())
        .build();
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicCounterSnapshot.newBuilder()
        .setCounter(counter.get())
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    AtomicCounterSnapshot snapshot = AtomicCounterSnapshot.parseFrom(input);
    counter.set(snapshot.getCounter());
  }
}