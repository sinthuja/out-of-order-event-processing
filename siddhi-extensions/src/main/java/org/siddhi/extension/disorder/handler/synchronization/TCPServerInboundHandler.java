/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.siddhi.extension.disorder.handler.synchronization;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.Constants;
import org.siddhi.extension.disorder.handler.multi.source.EventSourceDriftHolder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class TCPServerInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = Logger.getLogger(TCPServerInboundHandler.class);

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        ByteBuf encoded = null;
        try {
            if (in.readableBytes() < 5) {
                return;
            }
            byte protocol = in.readByte();
            if (protocol != Constants.PROTOCOL_VERSION) {
                ReferenceCountUtil.release(msg);
                return;
            }
            long receiveTime = System.currentTimeMillis();
            int messageTypeSize = in.readInt();
            String messageType = getString(in, messageTypeSize);
            int sourceIdSize = in.readInt();
            String sourceId = getString(in, sourceIdSize);
            long requestSendTime = in.readLong();
            if (messageType.equalsIgnoreCase(Constants.TIME_SYNC_INIT)) {
                encoded = ctx.alloc().buffer(30);
                encoded.writeByte(Constants.PROTOCOL_VERSION);
                encoded.writeByte(Constants.SUCCESS_RESPONSE);
                encoded.writeInt(sourceIdSize);
                encoded.writeBytes(sourceId.getBytes(StandardCharsets.UTF_8));
                encoded.writeLong(requestSendTime);
                encoded.writeLong(receiveTime);
                encoded.writeLong(System.currentTimeMillis());
            } else if (messageType.equalsIgnoreCase(Constants.TIME_SYNC_DONE)) {
                long requestReceiveTime = in.readLong();
                long replySendTime = in.readLong();
                long replyReceiveTime = in.readLong();
                double delay = (((replySendTime - requestReceiveTime) -
                        (replyReceiveTime - requestSendTime)) * 0.5);
                // requestSentTime + drift + delay = requestReceiveTime
                double drift = ((requestSendTime - requestReceiveTime - delay) +
                        (replyReceiveTime - replySendTime - delay)) * 0.5;
                System.out.println("################### Current Drift => source : "
                        + sourceId + " , drift: " + drift);
                EventSourceDriftHolder.getInstance().updateTimeDrift(sourceId,
                        new EventSourceDriftHolder.EventSourceDriftInfo(-drift, delay));
                encoded = ctx.alloc().buffer(2);
                encoded.writeByte(Constants.PROTOCOL_VERSION);
                encoded.writeInt(Constants.SUCCESS_RESPONSE);
            } else {
                log.error("Unknown message type : " + messageType + " received, hence dropping the message");
                encoded = ctx.alloc().buffer(2);
                encoded.writeByte(Constants.PROTOCOL_VERSION);
                encoded.writeByte(Constants.FAILURE_RESPONSE);
            }
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
            encoded = ctx.alloc().buffer(2);
            encoded.writeByte(Constants.PROTOCOL_VERSION);
            encoded.writeByte(Constants.FAILURE_RESPONSE);
        } catch (Throwable throwable) {
            log.error(throwable.getMessage(), throwable);
        } finally {
            ReferenceCountUtil.release(msg);
            if (encoded != null) {
                ctx.writeAndFlush(encoded);
            }
        }
    }

    private static String getString(ByteBuf byteBuffer, int size) throws UnsupportedEncodingException {
        byte[] bytes = new byte[size];
        byteBuffer.readBytes(bytes);
        return new String(bytes, Constants.DEFAULT_CHARSET);
    }

}
