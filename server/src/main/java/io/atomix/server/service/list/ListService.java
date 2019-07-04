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
package io.atomix.server.service.list;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import io.atomix.service.PrimitiveService;
import io.atomix.utils.component.Component;
import io.atomix.utils.stream.StreamHandler;

/**
 * List service implementation.
 */
public class ListService extends AbstractListService {
    public static final Type TYPE = new Type();

    /**
     * Set service type.
     */
    @Component
    public static class Type implements PrimitiveService.Type {
        private static final String NAME = "list";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public PrimitiveService newService() {
            return new ListService();
        }
    }

    private List<String> list = new LinkedList<>();

    @Override
    public SizeResponse size(SizeRequest request) {
        return SizeResponse.newBuilder()
            .setSize(list.size())
            .build();
    }

    @Override
    public ContainsResponse contains(ContainsRequest request) {
        return ContainsResponse.newBuilder()
            .setContains(list.contains(request.getValue()))
            .build();
    }

    @Override
    public AppendResponse append(AppendRequest request) {
        list.add(request.getValue());
        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.ADDED)
            .setValue(request.getValue())
            .build());
        return AppendResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .build();
    }

    @Override
    public InsertResponse insert(InsertRequest request) {
        int index = request.getIndex();
        if (index < 0 || index >= list.size()) {
            return InsertResponse.newBuilder()
                .setStatus(ResponseStatus.OUT_OF_BOUNDS)
                .build();
        }

        String oldValue = list.set(index, request.getValue());
        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.REMOVED)
            .setValue(oldValue)
            .build());
        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.ADDED)
            .setValue(request.getValue())
            .build());
        return InsertResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .build();
    }

    @Override
    public RemoveResponse remove(RemoveRequest request) {
        int index = request.getIndex();
        if (index < 0 || index >= list.size()) {
            return RemoveResponse.newBuilder()
                .setStatus(ResponseStatus.OUT_OF_BOUNDS)
                .build();
        }

        String value = list.remove(index);
        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.REMOVED)
            .setValue(value)
            .build());
        return RemoveResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .setValue(value)
            .build();
    }

    @Override
    public ClearResponse clear(ClearRequest request) {
        for (String value : list) {
            onEvent(ListenResponse.newBuilder()
                .setType(ListenResponse.Type.REMOVED)
                .setValue(value)
                .build());
        }
        list.clear();
        return ClearResponse.newBuilder().build();
    }

    @Override
    public void listen(ListenRequest request, StreamHandler<ListenResponse> handler) {
        // Keep the stream open.
    }

    @Override
    public UnlistenResponse unlisten(UnlistenRequest request) {
        // Complete the stream.
        StreamHandler<ListenResponse> stream = getCurrentSession().getStream(request.getStreamId());
        if (stream != null) {
            stream.complete();
        }
        return UnlistenResponse.newBuilder().build();
    }

    @Override
    public void iterate(IterateRequest request, StreamHandler<IterateResponse> handler) {
        for (String value : list) {
            handler.next(IterateResponse.newBuilder()
                .setValue(value)
                .build());
        }
        handler.complete();
    }

    private void onEvent(ListenResponse event) {
        getSessions()
            .forEach(session -> session.getStreams(ListOperations.LISTEN_STREAM)
                .forEach(stream -> stream.next(event)));
    }

    @Override
    public void backup(OutputStream output) throws IOException {
        DistributedListSnapshot.newBuilder()
            .addAllValues(list)
            .build()
            .writeTo(output);
    }

    @Override
    public void restore(InputStream input) throws IOException {
        DistributedListSnapshot snapshot = DistributedListSnapshot.parseFrom(input);
        list = Lists.newLinkedList(snapshot.getValuesList());
    }
}
