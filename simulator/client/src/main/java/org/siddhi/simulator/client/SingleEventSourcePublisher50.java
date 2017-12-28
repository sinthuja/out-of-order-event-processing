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
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class SingleEventSourcePublisher50 {
    private static final Logger log = Logger.getLogger(SingleEventSourcePublisher50.class);

    private static final String SOURCE_ID = "SOURCE1";
    private static final int EVENT_BATCH_SIZE = 20;
    private static final double OUT_OF_ORDER_PERCENTAGE = 0.5;
    private static final int MAX_OUT_OF_ORDER_GAP = 10;

    public static void main(String[] args) {
        TCPNettyClient tcpNettyClient = null;
        try {
            tcpNettyClient = new TCPNettyClient();
            tcpNettyClient.connect("localhost", 9892);
            ArrayList<Event> arrayList = null;
            final StreamDefinition streamDefinition = StreamDefinition.id("inputStream").attribute("sourceId", Attribute.Type.STRING)
                    .attribute("seqNum", Attribute.Type.LONG).attribute("volume",
                            Attribute.Type.LONG).attribute("eventTime", Attribute.Type.LONG);

            Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(streamDefinition.getAttributeList());
            try {
                log.info("Starting to publish events .. ");
                for (int i = 0; i < 100000; i++) {
                    arrayList = getEvents(i);
                    tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[arrayList.size()]), types).array()).await();
                }
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

    private static ArrayList<Event> getEvents(int iteration) {
        ArrayList<Event> arrayList = new ArrayList<Event>();
        Random random = new Random(EVENT_BATCH_SIZE);
        int startSequence = iteration * EVENT_BATCH_SIZE;
        int endSequence = (iteration + 1) * EVENT_BATCH_SIZE;
        for (int i = startSequence; i < endSequence; i++) {
            long sequenceNum = ((Integer) i).longValue();
            long timestamp = System.currentTimeMillis();
            arrayList.add(new Event(timestamp, new Object[]{SOURCE_ID, sequenceNum + 1,
                    ((Integer) (random.nextInt(9) + 1)).longValue(), timestamp}));
        }
        int numberOfOutofOrder = ((Long) Math.round(EVENT_BATCH_SIZE * OUT_OF_ORDER_PERCENTAGE)).intValue();
        List<Integer> uniqueRandomIndex = getUniqueRandomNumbers(EVENT_BATCH_SIZE, random);
        for (int i = 0; i < numberOfOutofOrder; i++) {
            int currentPos = uniqueRandomIndex.get(i);
            int newPos = currentPos + random.nextInt(MAX_OUT_OF_ORDER_GAP);
            if (newPos > arrayList.size() - 1) {
                Event event = arrayList.remove(currentPos);
                System.out.println(event.getData()[1]);
                arrayList.add(event);
            } else {
                Event event = arrayList.remove(currentPos);
                System.out.println(event.getData()[1]);
                arrayList.add(newPos, event);
            }

        }
        return arrayList;
    }

    private static List<Integer> getUniqueRandomNumbers(int maxNumber, Random random) {
        List<Integer> numbers = new ArrayList<Integer>(maxNumber);
        for (int i = 0; i < maxNumber; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers, random);
        return numbers;
    }
}
