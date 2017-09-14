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
package org.siddhi.simulator.client;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.ArrayList;

public class SingleEventSourcePublisher {
    private static final Logger log = Logger.getLogger(SingleEventSourcePublisher.class);

    private static final String SOURCE_ID = "SOURCE1";

    public static void main(String[] args) {
        TCPNettyClient tcpNettyClient = null;
        try {
            tcpNettyClient = new TCPNettyClient();

            tcpNettyClient.connect("localhost", 9892);
            ArrayList<Event> arrayList = new ArrayList<Event>();

            final StreamDefinition streamDefinition = StreamDefinition.id("inputStream").attribute("sourceId", Attribute.Type.STRING)
                    .attribute("seqNum", Attribute.Type.LONG).attribute("volume",
                            Attribute.Type.LONG);
            Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(streamDefinition.getAttributeList());

            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 1L, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 2L, 200L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 3L, 200L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 7L, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 6L, 200L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 5L, 200L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 4L, 100L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 8L, 200L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 10L, 300L}));
            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{SOURCE_ID, 9L, 400L}));
            try {
                log.info("Starting to publish events .. ");
                tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
                        arrayList.toArray(new Event[arrayList.size()]), types).array()).await();
                log.info("Completed Publishing  events .. ");
            } catch (IOException e) {
                log.error(e);
            } catch (InterruptedException e) {
                log.error(e);
            }
        } catch (ConnectionUnavailableException e) {
            log.error(e);
        } finally {
            if (tcpNettyClient != null) {
                tcpNettyClient.disconnect();
                tcpNettyClient.shutdown();
            }
        }
    }
}
