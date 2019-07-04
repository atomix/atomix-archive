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

package io.atomix.node.service;

import io.atomix.utils.AtomixRuntimeException;

/**
 * Top level exception for Store failures.
 */
@SuppressWarnings("serial")
public class ServiceException extends AtomixRuntimeException {
    public ServiceException() {
    }

    public ServiceException(String message) {
        super(message);
    }

    public ServiceException(Throwable t) {
        super(t);
    }

    /**
     * Store is temporarily unavailable.
     */
    public static class Unavailable extends ServiceException {
        public Unavailable() {
        }

        public Unavailable(String message) {
            super(message);
        }
    }

    /**
     * Store operation timeout.
     */
    public static class Timeout extends ServiceException {
    }

    /**
     * Store update conflicts with an in flight transaction.
     */
    public static class ConcurrentModification extends ServiceException {
    }

    /**
     * Store operation interrupted.
     */
    public static class Interrupted extends ServiceException {
    }

    /**
     * Primitive service exception.
     */
    public static class ApplicationException extends ServiceException {
        public ApplicationException() {
        }

        public ApplicationException(String message) {
            super(message);
        }

        public ApplicationException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Command failure exception.
     */
    public static class CommandFailure extends ServiceException {
        public CommandFailure() {
        }

        public CommandFailure(String message) {
            super(message);
        }
    }

    /**
     * Query failure exception.
     */
    public static class QueryFailure extends ServiceException {
        public QueryFailure() {
        }

        public QueryFailure(String message) {
            super(message);
        }
    }

    /**
     * Unknown client exception.
     */
    public static class UnknownClient extends ServiceException {
        public UnknownClient() {
        }

        public UnknownClient(String message) {
            super(message);
        }
    }

    /**
     * Unknown session exception.
     */
    public static class UnknownSession extends ServiceException {
        public UnknownSession() {
        }

        public UnknownSession(String message) {
            super(message);
        }
    }

    /**
     * Unknown service exception.
     */
    public static class UnknownService extends ServiceException {
        public UnknownService() {
        }

        public UnknownService(String message) {
            super(message);
        }
    }

    /**
     * Closed session exception.
     */
    public static class ClosedSession extends ServiceException {
        public ClosedSession() {
        }

        public ClosedSession(String message) {
            super(message);
        }
    }
}