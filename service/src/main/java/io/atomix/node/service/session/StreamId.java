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
package io.atomix.node.service.session;

import java.util.Objects;

/**
 * Stream ID.
 */
public class StreamId {
    private final SessionId sessionId;
    private final long streamId;

    public StreamId(SessionId sessionId, long streamId) {
        this.sessionId = sessionId;
        this.streamId = streamId;
    }

    /**
     * Returns the stream session ID.
     *
     * @return the stream session ID
     */
    public SessionId sessionId() {
        return sessionId;
    }

    /**
     * Returns the stream ID.
     *
     * @return the stream ID
     */
    public long streamId() {
        return streamId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, streamId);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof StreamId) {
            StreamId that = (StreamId) object;
            return this.sessionId.equals(that.sessionId) && this.streamId == that.streamId;
        }
        return false;
    }
}
