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

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.grpc.map.GetRequest;
import io.atomix.grpc.map.MapId;
import io.atomix.grpc.map.MapServiceGrpc;
import io.atomix.grpc.map.PutRequest;
import io.atomix.grpc.map.SizeRequest;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * gRPC map service test.
 */
public class MapServiceImplTest extends GrpcServiceTest<MapServiceGrpc.MapServiceBlockingStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new MapServiceImpl(atomix);
  }

  @Override
  protected MapServiceGrpc.MapServiceBlockingStub getStub(Channel channel) {
    return MapServiceGrpc.newBlockingStub(channel);
  }

  @Test
  public void testGrpcMap() throws Exception {
    MapServiceGrpc.MapServiceBlockingStub map1 = getStub(1);
    MapServiceGrpc.MapServiceBlockingStub map2 = getStub(2);

    MapId mapId = MapId.newBuilder()
        .setName("test-map")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    assertEquals(0, map1.size(SizeRequest.newBuilder()
        .setId(mapId)
        .build())
        .getSize());
    assertEquals(0, map1.get(GetRequest.newBuilder()
        .setId(mapId)
        .setKey("foo")
        .build())
        .getValue()
        .toByteArray()
        .length);
    assertEquals(0, map1.get(GetRequest.newBuilder()
        .setId(mapId)
        .setKey("foo")
        .build())
        .getVersion());

    assertTrue(map1.put(PutRequest.newBuilder()
        .setId(mapId)
        .setKey("foo")
        .setValue(ByteString.copyFrom("bar".getBytes()))
        .build()).getHeaders().getHeaders(0).getIndex() > 0);
    assertArrayEquals("bar".getBytes(), map1.get(GetRequest.newBuilder()
        .setId(mapId)
        .setKey("foo")
        .build()).getValue().toByteArray());
    assertTrue(map1.get(GetRequest.newBuilder()
        .setId(mapId)
        .setKey("foo")
        .build())
        .getVersion() > 0);
  }
}
