/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.grpc.impl;

import com.google.common.util.concurrent.ListenableFuture;
import io.atomix.core.Atomix;
import io.atomix.grpc.lock.IsLockedRequest;
import io.atomix.grpc.lock.LockId;
import io.atomix.grpc.lock.LockRequest;
import io.atomix.grpc.lock.LockResponse;
import io.atomix.grpc.lock.LockServiceGrpc;
import io.atomix.grpc.lock.UnlockRequest;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * gRPC lock service test.
 */
public class LockServiceImplTest extends GrpcServiceTest<LockServiceGrpc.LockServiceFutureStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new LockServiceImpl(atomix);
  }

  @Override
  protected LockServiceGrpc.LockServiceFutureStub getStub(Channel channel) {
    return LockServiceGrpc.newFutureStub(channel);
  }

  @Test
  public void testGrpcMap() throws Exception {
    LockServiceGrpc.LockServiceFutureStub lock1 = getStub(1);
    LockServiceGrpc.LockServiceFutureStub lock2 = getStub(2);

    LockId lockId = LockId.newBuilder()
        .setName("test-lock")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    assertFalse(lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getIsLocked());

    long version = lock1.lock(LockRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getVersion();
    assertTrue(version > 0);

    assertTrue(lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getIsLocked());
    assertTrue(lock2.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getIsLocked());
    assertTrue(lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setVersion(version)
        .build())
        .get()
        .getIsLocked());
    assertTrue(lock2.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setVersion(version)
        .build())
        .get()
        .getIsLocked());

    ListenableFuture<LockResponse> future = lock2.lock(LockRequest.newBuilder()
        .setId(lockId)
        .build());

    assertTrue(lock1.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .setVersion(version)
        .build())
        .get()
        .getUnlocked());

    assertTrue(future.get().getVersion() > 0);
    assertTrue(lock2.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getUnlocked());

    assertFalse(lock1.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .build())
        .get()
        .getUnlocked());
  }
}
