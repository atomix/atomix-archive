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
import com.google.protobuf.Duration;
import io.atomix.core.Atomix;
import io.atomix.grpc.headers.SessionCommandHeaders;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionQueryHeaders;
import io.atomix.grpc.lock.CreateRequest;
import io.atomix.grpc.lock.IsLockedRequest;
import io.atomix.grpc.lock.IsLockedResponse;
import io.atomix.grpc.lock.LockId;
import io.atomix.grpc.lock.LockRequest;
import io.atomix.grpc.lock.LockResponse;
import io.atomix.grpc.lock.LockServiceGrpc;
import io.atomix.grpc.lock.UnlockRequest;
import io.atomix.grpc.lock.UnlockResponse;
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
  public void testGrpcLock() throws Exception {
    LockServiceGrpc.LockServiceFutureStub lock1 = getStub(1);
    LockServiceGrpc.LockServiceFutureStub lock2 = getStub(2);

    LockId lockId = LockId.newBuilder()
        .setName("test-lock")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    long sessionId1 = lock1.create(CreateRequest.newBuilder()
        .setId(lockId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(5)
            .build())
        .build())
        .get()
        .getHeaders()
        .getSessionId();

    long sessionId2 = lock2.create(CreateRequest.newBuilder()
        .setId(lockId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(5)
            .build())
        .build())
        .get()
        .getHeaders()
        .getSessionId();

    IsLockedResponse isLockedResponse = lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setHeaders(SessionQueryHeaders.newBuilder()
            .setSessionId(sessionId1)
            .build())
        .build())
        .get();
    assertFalse(isLockedResponse.getIsLocked());

    LockResponse lockResponse = lock1.lock(LockRequest.newBuilder()
        .setId(lockId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(-1)
            .build())
        .setHeaders(SessionCommandHeaders.newBuilder()
            .setSessionId(sessionId1)
            .build())
        .build())
        .get();
    assertTrue(lockResponse.getVersion() > 0);

    isLockedResponse = lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setHeaders(SessionQueryHeaders.newBuilder()
            .setSessionId(sessionId1)
            .addHeaders(SessionQueryHeader.newBuilder()
                .setLastIndex(lockResponse.getHeaders().getHeaders(0).getIndex())
                .build())
            .build())
        .build())
        .get();
    assertTrue(isLockedResponse.getIsLocked());

    isLockedResponse = lock2.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setHeaders(SessionQueryHeaders.newBuilder()
            .setSessionId(sessionId2)
            .build())
        .build())
        .get();
    assertTrue(isLockedResponse.getIsLocked());

    isLockedResponse = lock1.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setVersion(lockResponse.getVersion())
        .setHeaders(SessionQueryHeaders.newBuilder()
            .setSessionId(sessionId1)
            .build())
        .build())
        .get();
    assertTrue(isLockedResponse.getIsLocked());

    isLockedResponse = lock2.isLocked(IsLockedRequest.newBuilder()
        .setId(lockId)
        .setVersion(lockResponse.getVersion())
        .setHeaders(SessionQueryHeaders.newBuilder()
            .setSessionId(sessionId2)
            .build())
        .build())
        .get();
    assertTrue(isLockedResponse.getIsLocked());

    ListenableFuture<LockResponse> lockFuture = lock2.lock(LockRequest.newBuilder()
        .setId(lockId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(-1)
            .build())
        .setHeaders(SessionCommandHeaders.newBuilder()
            .setSessionId(sessionId2)
            .build())
        .build());

    UnlockResponse unlockResponse = lock1.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .setVersion(lockResponse.getVersion())
        .setHeaders(SessionCommandHeaders.newBuilder()
            .setSessionId(sessionId1)
            .build())
        .build())
        .get();
    assertTrue(unlockResponse.getUnlocked());

    lockResponse = lockFuture.get();
    assertTrue(lockResponse.getVersion() > 0);

    unlockResponse = lock2.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .setHeaders(SessionCommandHeaders.newBuilder()
            .setSessionId(sessionId2)
            .build())
        .build())
        .get();
    assertTrue(unlockResponse.getUnlocked());

    unlockResponse = lock1.unlock(UnlockRequest.newBuilder()
        .setId(lockId)
        .setHeaders(SessionCommandHeaders.newBuilder()
            .setSessionId(sessionId1)
            .build())
        .build())
        .get();
    assertFalse(unlockResponse.getUnlocked());
  }
}
