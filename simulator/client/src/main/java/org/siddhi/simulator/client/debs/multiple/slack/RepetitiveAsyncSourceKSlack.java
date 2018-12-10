/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.siddhi.simulator.client.debs.multiple.slack;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class RepetitiveAsyncSourceKSlack implements Runnable {
    private static final Logger log = Logger.getLogger(RepetitiveAsyncSourceKSlack.class);
    private Attribute.Type[] types;
    private List<Event> queue;
    private int bundleSize;
    private TCPNettyClient tcpNettyClient;
    private long minTimestamp;
    private long maxTimestamp;
    private String sourceId;
    private long skewTime;


    public RepetitiveAsyncSourceKSlack(String sourceId, Attribute.Type[] types,
                                       List<Event> queue, int bundleSize,
                                       long minTimestamp, long skewTime) {
        this.types = types;
        this.queue = queue;
        this.bundleSize = bundleSize;
        this.minTimestamp = minTimestamp;
        this.sourceId = sourceId;
        this.skewTime = skewTime;
        try {
            tcpNettyClient = new TCPNettyClient();
            tcpNettyClient.connect("localhost", 9892);
        } catch (ConnectionUnavailableException e) {
            log.error(e);
        }
    }

    @Override
    public void run() {
        while (!RepetitiveMultipleSourceKSlack.START) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }
        try {
            int bundleIndex = 0;
            Event[] eventBundle = new Event[bundleSize];
            int count = 0;
            Event currentEvent = queue.get(0);
            currentEvent.getData()[1] = ((Long) currentEvent.getData()[1]) + skewTime;
            long firstEventTime = (Long) currentEvent.getData()[1];
            long initialWaitTime = (firstEventTime - minTimestamp) / 1000000000L;
            if (initialWaitTime > 0) {
                System.out.println("Source ID :" + sourceId + " , initial sleep : " + initialWaitTime);
                Thread.sleep(initialWaitTime);
            }
            int i = 1;
            while (true) {
                long currentEventTime = 0;
                if (currentEvent != null) {
                    currentEventTime = (Long) currentEvent.getData()[1];
                    if (maxTimestamp == -1) {
                        maxTimestamp = (Long) currentEvent.getData()[1];
                    } else if (maxTimestamp < currentEventTime) {
                        maxTimestamp = currentEventTime;
                    }
                }

                Event nextEvent = null;
                if (i < queue.size()) {
                    nextEvent = queue.get(i);
                    nextEvent.getData()[1] = ((Long) nextEvent.getData()[1]) + skewTime;
                }
                if (currentEvent == null) {
                    if (bundleIndex != 0) {
                        eventBundle = Arrays.copyOf(eventBundle, bundleIndex);
                        tcpNettyClient.send("TestServer/inputStream",
                                BinaryEventConverter.convertToBinaryMessage(eventBundle, types).array()).await();
                        count = count + eventBundle.length;
                        bundleIndex = 0;
                        eventBundle = new Event[bundleSize];
                    } else {
                        break;
                    }
                } else {
//                    long currentEventTime = (Long) currentEvent.getData()[1];
                    currentEvent.setTimestamp(System.currentTimeMillis());
                    eventBundle[bundleIndex] = currentEvent;
                    bundleIndex++;
                    if (nextEvent != null) {
                        long nextEventTime = (Long) nextEvent.getData()[1];
                        if (currentEventTime < nextEventTime) {
                            eventBundle = Arrays.copyOf(eventBundle, bundleIndex);
                            long waitTime = (nextEventTime - currentEventTime) / 1000000000L;
                            long startTime = System.currentTimeMillis();
                            tcpNettyClient.send("TestServer/inputStream",
                                    BinaryEventConverter.convertToBinaryMessage(eventBundle, types).array()).await();
                            long waitingTime = waitTime - (System.currentTimeMillis() - startTime);
                            if (waitingTime > 0) {
                                if (waitingTime > 1000) {
                                    System.out.println("Sleeping for : " + waitingTime + " -  source Id :" + this.sourceId);
                                }
                                try {
                                    Thread.sleep(waitingTime);
                                } catch (Throwable ignored) {
                                }
                            }
                            count = count + eventBundle.length;
                            bundleIndex = 0;
                            eventBundle = new Event[bundleSize];
                        }
                    }
                    currentEvent = nextEvent;
                }
                if (bundleIndex == bundleSize) {
                    tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
                            eventBundle, types).array()).await();
                    count = count + eventBundle.length;
                    bundleIndex = 0;
                    eventBundle = new Event[bundleSize];
                }
                i++;
            }
            log.info("Completed Publishing  events => " + count + " for source: " + sourceId);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
//            try {
//                int i = 1;
//                while (true) {
//                    Thread.sleep(1000);
//                    firstEvent.getData()[1] = maxTimestamp + (i * 10000000000000L);
//                    tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
//                            new Event[]{firstEvent}, types).array()).await();
//                    i++;
//                }
//            } catch (InterruptedException | IOException ignored) {
//            }
            if (tcpNettyClient != null) {
                tcpNettyClient.disconnect();
                tcpNettyClient.shutdown();
            }
        }

    }
}