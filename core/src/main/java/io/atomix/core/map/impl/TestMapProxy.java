package io.atomix.core.map.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.impl.EventContext;
import io.atomix.primitive.session.impl.SessionContext;
import io.atomix.primitive.session.impl.SessionMetadata;
import io.atomix.utils.concurrent.ThreadContext;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Test map proxy.
 */
public class TestMapProxy extends SessionEnabledPrimitiveProxy {
  public TestMapProxy(ServiceId serviceId, PartitionId partitionId, PrimitiveManagementService managementService, ThreadContext context) {
    super(serviceId, partitionId, managementService, context);
  }

  public CompletableFuture<Pair<SessionContext, PutResponse>> put(SessionMetadata session, PutRequest request) {
    return getClient().execute(MapOperations.PUT, session, request, PutRequest::toByteString, PutResponse::parseFrom);
  }

  public CompletableFuture<Pair<SessionContext, GetResponse>> get(SessionMetadata session, GetRequest request) {
    return getClient().execute(MapOperations.GET, session, request, GetRequest::toByteString, GetResponse::parseFrom);
  }

  public void onEvent(SessionMetadata session, BiConsumer<EventContext, MapEvent> listener) {
    getClient().addEventListener(MapEvents.ON_EVENT, session, listener, MapEvent::parseFrom);
  }
}
