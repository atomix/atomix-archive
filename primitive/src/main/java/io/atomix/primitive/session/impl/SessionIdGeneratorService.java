/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitive.session.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.atomix.primitive.service.ServiceOperationRegistry;
import io.atomix.primitive.service.SimplePrimitiveService;
import io.atomix.primitive.session.impl.proto.NextRequest;
import io.atomix.primitive.session.impl.proto.NextResponse;
import io.atomix.primitive.session.impl.proto.SessionIdGeneratorSnapshot;

/**
 * ID generator service.
 */
public class SessionIdGeneratorService extends SimplePrimitiveService {
  private long id;

  @Override
  public void backup(OutputStream output) throws IOException {
    SessionIdGeneratorSnapshot.newBuilder()
        .setId(id)
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    SessionIdGeneratorSnapshot snapshot = SessionIdGeneratorSnapshot.parseFrom(input);
    id = snapshot.getId();
  }

  @Override
  protected void configure(ServiceOperationRegistry registry) {
    registry.register(
        SessionIdGeneratorOperations.NEXT,
        this::next,
        NextRequest::parseFrom,
        NextResponse::toByteArray);
  }

  protected NextResponse next(NextRequest request) {
    return NextResponse.newBuilder()
        .setSessionId(++id)
        .build();
  }
}
