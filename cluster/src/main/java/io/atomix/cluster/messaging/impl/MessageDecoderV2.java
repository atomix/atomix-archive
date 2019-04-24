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
package io.atomix.cluster.messaging.impl;

import java.util.List;

import io.atomix.utils.net.Address;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import static com.google.common.base.Preconditions.checkState;

/**
 * Protocol version 2 message decoder.
 */
class MessageDecoderV2 extends AbstractMessageDecoder {

  /**
   * V2 decoder state.
   */
  enum TypeState {
    READ_TYPE,
    READ_MESSAGE_ID,
    READ_SENDER_HOST_LENGTH,
    READ_SENDER_HOST,
    READ_SENDER_PORT,
    READ_REQUEST,
    READ_REPLY,
    READ_STREAM_REQUEST,
    READ_STREAM_REPLY,
    READ_STREAM,
    READ_STREAM_END,
  }

  enum MessageState {
    READ_STATUS,
    READ_SUBJECT_LENGTH,
    READ_SUBJECT,
    READ_CONTENT_LENGTH,
    READ_CONTENT,
  }

  private TypeState typeState = TypeState.READ_SENDER_HOST_LENGTH;
  private MessageState messageState;

  private int senderHostLength;
  private String senderHost;
  private int senderPort;
  private Address senderAddress;

  private ProtocolMessage.Type type;
  private long messageId;
  private ProtocolStatus status;
  private int contentLength;
  private byte[] content;
  private int subjectLength;
  private String subject;

  @Override
  @SuppressWarnings("squid:S128") // suppress switch fall through warning
  protected void decode(
      ChannelHandlerContext context,
      ByteBuf buffer,
      List<Object> out) throws Exception {

    switch (typeState) {
      case READ_SENDER_HOST_LENGTH:
        if (buffer.readableBytes() < Short.BYTES) {
          return;
        }
        senderHostLength = buffer.readShort();
        typeState = TypeState.READ_SENDER_HOST;
      case READ_SENDER_HOST:
        if (buffer.readableBytes() < senderHostLength) {
          return;
        }
        senderHost = readString(buffer, senderHostLength);
        typeState = TypeState.READ_SENDER_PORT;
      case READ_SENDER_PORT:
        if (buffer.readableBytes() < Integer.BYTES) {
          return;
        }
        senderPort = buffer.readInt();
        senderAddress = Address.from(senderHost, senderPort);
        typeState = TypeState.READ_TYPE;
      case READ_TYPE:
        if (buffer.readableBytes() < Byte.BYTES) {
          return;
        }
        type = ProtocolMessage.Type.forId(buffer.readByte());
        typeState = TypeState.READ_MESSAGE_ID;
      case READ_MESSAGE_ID:
        try {
          messageId = readLong(buffer);
        } catch (Escape e) {
          return;
        }
        switch (type) {
          case REQUEST:
            typeState = TypeState.READ_REQUEST;
            messageState = MessageState.READ_SUBJECT_LENGTH;
            break;
          case REPLY:
            typeState = TypeState.READ_REPLY;
            messageState = MessageState.READ_STATUS;
            break;
          case STREAM_REQUEST:
            typeState = TypeState.READ_STREAM_REQUEST;
            messageState = MessageState.READ_SUBJECT_LENGTH;
            break;
          case STREAM_REPLY:
            typeState = TypeState.READ_STREAM_REPLY;
            messageState = null;
            break;
          case STREAM:
            typeState = TypeState.READ_STREAM;
            messageState = MessageState.READ_CONTENT_LENGTH;
            break;
          case STREAM_END:
            typeState = TypeState.READ_STREAM_END;
            messageState = MessageState.READ_STATUS;
            break;
        }
        break;
      default:
        break;
    }

    switch (typeState) {
      case READ_REQUEST:
        switch (messageState) {
          case READ_SUBJECT_LENGTH:
            if (buffer.readableBytes() < Short.BYTES) {
              return;
            }
            subjectLength = buffer.readShort();
            messageState = MessageState.READ_SUBJECT;
          case READ_SUBJECT:
            if (buffer.readableBytes() < subjectLength) {
              return;
            }
            subject = readString(buffer, subjectLength);
            messageState = MessageState.READ_CONTENT_LENGTH;
          case READ_CONTENT_LENGTH:
            try {
              contentLength = readInt(buffer);
            } catch (Escape e) {
              return;
            }
            messageState = MessageState.READ_CONTENT;
          case READ_CONTENT:
            if (buffer.readableBytes() < contentLength) {
              return;
            }
            if (contentLength > 0) {
              // TODO: Perform a sanity check on the size before allocating
              content = new byte[contentLength];
              buffer.readBytes(content);
            } else {
              content = EMPTY_PAYLOAD;
            }
            out.add(new ProtocolRequest(messageId, senderAddress, subject, content));
            typeState = TypeState.READ_TYPE;
            messageState = null;
        }
        break;
      case READ_REPLY:
        switch (messageState) {
          case READ_STATUS:
            if (buffer.readableBytes() < Byte.BYTES) {
              return;
            }
            status = ProtocolStatus.forId(buffer.readByte());
            messageState = MessageState.READ_CONTENT_LENGTH;
          case READ_CONTENT_LENGTH:
            try {
              contentLength = readInt(buffer);
            } catch (Escape e) {
              return;
            }
            messageState = MessageState.READ_CONTENT;
          case READ_CONTENT:
            if (buffer.readableBytes() < contentLength) {
              return;
            }
            if (contentLength > 0) {
              // TODO: Perform a sanity check on the size before allocating
              content = new byte[contentLength];
              buffer.readBytes(content);
            } else {
              content = EMPTY_PAYLOAD;
            }
            out.add(new ProtocolReply(messageId, status, content));
            typeState = TypeState.READ_TYPE;
            messageState = null;
        }
        break;
      case READ_STREAM_REQUEST:
        switch (messageState) {
          case READ_SUBJECT_LENGTH:
            if (buffer.readableBytes() < Short.BYTES) {
              return;
            }
            subjectLength = buffer.readShort();
            messageState = MessageState.READ_SUBJECT;
          case READ_SUBJECT:
            if (buffer.readableBytes() < subjectLength) {
              return;
            }
            subject = readString(buffer, subjectLength);
            out.add(new ProtocolStreamRequest(messageId, senderAddress, subject));
            typeState = TypeState.READ_TYPE;
            messageState = null;
        }
        break;
      case READ_STREAM_REPLY:
        out.add(new ProtocolStreamReply(messageId));
        typeState = TypeState.READ_TYPE;
        messageState = null;
        break;
      case READ_STREAM:
        switch (messageState) {
          case READ_CONTENT_LENGTH:
            try {
              contentLength = readInt(buffer);
            } catch (Escape e) {
              return;
            }
            messageState = MessageState.READ_CONTENT;
          case READ_CONTENT:
            if (buffer.readableBytes() < contentLength) {
              return;
            }
            if (contentLength > 0) {
              // TODO: Perform a sanity check on the size before allocating
              content = new byte[contentLength];
              buffer.readBytes(content);
            } else {
              content = EMPTY_PAYLOAD;
            }
            out.add(new ProtocolStream(messageId, content));
            typeState = TypeState.READ_TYPE;
            messageState = null;
        }
        break;
      case READ_STREAM_END:
        switch (messageState) {
          case READ_STATUS:
            if (buffer.readableBytes() < Byte.BYTES) {
              return;
            }
            status = ProtocolStatus.forId(buffer.readByte());
            out.add(new ProtocolStreamEnd(messageId, status));
            typeState = TypeState.READ_TYPE;
            messageState = null;
        }
        break;
      default:
        break;
    }
  }
}
