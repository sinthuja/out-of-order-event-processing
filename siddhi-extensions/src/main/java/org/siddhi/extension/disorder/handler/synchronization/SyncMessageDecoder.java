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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.Constant;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

public class SyncMessageDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = Logger.getLogger(SyncMessageDecoder.class);
    private FlowController flowController;
    private HashMap<String, Long> eventSourceDrift = new HashMap<>();

    public SyncMessageDecoder(FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext,
                          ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 5) {
            return;
        }
        try {
            byte protocol = in.readByte();
            int messageSize = in.readInt();
            if (protocol == 1 && messageSize > in.readableBytes()) {
                in.resetReaderIndex();
                return;
            }
            long receiveTime = System.currentTimeMillis();
            flowController.barrier();

            int messageTypeSize = in.readInt();
            String messageType = getString(in, messageTypeSize);
            if (messageType.equalsIgnoreCase(Constant.TIME_SYNC_INIT)) {
                out.add(new byte[]{(byte) 0x01});
                out.add(15);
                out.add(receiveTime);
                out.add(System.currentTimeMillis());
            } else if (messageType.equalsIgnoreCase(Constant.TIME_SYNC_DONE)) {
                int sourceIdSize = in.readInt();
                String sourceId = getString(in, sourceIdSize);
                long requestSendTime = in.readLong();
                long requestReceiveTime = in.readLong();
                long replySendTime = in.readLong();
                long replyReceiveTime = in.readLong();
                long delay = Math.round(((replyReceiveTime - requestSendTime)
                        - (replySendTime - replyReceiveTime)) * 0.5);
                long drift = requestReceiveTime - replySendTime - delay;
                eventSourceDrift.put(sourceId, drift);

                out.add(new byte[]{(byte) 0x01});
                out.add(3);
                out.add(new byte[]{(byte) 0x20});
            } else {
                LOG.error("Unknown message type : " + messageType + " received, hence dropping the message");
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            in.markReaderIndex();
        }
    }

    public static String getString(ByteBuf byteBuffer, int size) throws UnsupportedEncodingException {
        byte[] bytes = new byte[size];
        byteBuffer.readBytes(bytes);
        return new String(bytes, Constant.DEFAULT_CHARSET);
    }
}
