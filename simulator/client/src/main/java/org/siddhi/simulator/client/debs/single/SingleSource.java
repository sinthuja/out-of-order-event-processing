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
package org.siddhi.simulator.client.debs.single;

import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SingleSource {
    private static final Logger log = Logger.getLogger(SingleSource.class);
    private static Splitter splitter = Splitter.on(',');
    private static final int bundleSize = 100;
    private static LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private static final String SOURCE_ID = "0";

    public static void main(String[] args) {
        TCPNettyClient tcpNettyClient = null;
        String path = "/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/single-source/out-of-order/dataset2";
        DataLoader loader = new DataLoader(path);
        Executors.newSingleThreadExecutor().execute(loader);
        try {
            tcpNettyClient = new TCPNettyClient();
            tcpNettyClient.connect("localhost", 9892, 7452, SOURCE_ID, 10);
            final StreamDefinition streamDefinition = StreamDefinition.id("inputStream")
                    .attribute("sid", Attribute.Type.INT)
                    .attribute("ts", Attribute.Type.LONG)
                    .attribute("x", Attribute.Type.INT)
                    .attribute("y", Attribute.Type.INT)
                    .attribute("z", Attribute.Type.INT)
                    .attribute("v_abs", Attribute.Type.INT)
                    .attribute("a_abs", Attribute.Type.INT)
                    .attribute("vx", Attribute.Type.INT)
                    .attribute("vy", Attribute.Type.INT)
                    .attribute("vz", Attribute.Type.INT)
                    .attribute("ax", Attribute.Type.INT)
                    .attribute("ay", Attribute.Type.INT)
                    .attribute("az", Attribute.Type.INT)
                    .attribute("sourceId", Attribute.Type.STRING)
                    .attribute("seqNum", Attribute.Type.LONG);
            Attribute.Type[] types = EventDefinitionConverterUtil
                    .generateAttributeTypeArray(streamDefinition.getAttributeList());
            try {
                int bundleIndex = 0;
                Event[] eventBundle = new Event[bundleSize];
                int count = 0;
                while (true) {
                    Event event = events.poll();
                    if (event == null) {
                        if (bundleIndex != 0) {
                            eventBundle = Arrays.copyOf(eventBundle, bundleIndex);
                            tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
                                    eventBundle, types).array()).await();
                            count = count + eventBundle.length;
                            bundleIndex = 0;
                            eventBundle = new Event[bundleSize];
                        }
                        if (loader.completed) {
                            break;
                        }
                    } else {
                        event.setTimestamp(System.currentTimeMillis());
                        eventBundle[bundleIndex] = event;
                        bundleIndex++;
                    }
                    if (bundleIndex == bundleSize) {
                        tcpNettyClient.send("TestServer/inputStream", BinaryEventConverter.convertToBinaryMessage(
                                eventBundle, types).array()).await();
                        count = count + eventBundle.length;
                        bundleIndex = 0;
                        eventBundle = new Event[bundleSize];
                    }
                }
                log.info("Completed Publishing  events => " + count);
            } catch (IOException | InterruptedException e) {
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

    public static class DataLoader implements Runnable {
        private String filePath;
        private boolean completed;

        public DataLoader(String filePath) {
            this.filePath = filePath;
            this.completed = false;
        }

        private void loadData() {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(filePath), 10 * 1024 * 1024);
                String line = br.readLine();
                int count = 0;
                while (line != null && !line.isEmpty()) {
                    try {
                        Iterator<String> dataStrIterator = splitter.split(line).iterator();
                        Integer sid = Integer.parseInt(dataStrIterator.next()); //sensor id
                        String ts = dataStrIterator.next(); //Timestamp in pico seconds
                        String x = dataStrIterator.next();
                        String y = dataStrIterator.next();
                        String z = dataStrIterator.next();
                        String v_abs = dataStrIterator.next();
                        String a_abs = dataStrIterator.next();
                        String vx = dataStrIterator.next();
                        String vy = dataStrIterator.next();
                        String vz = dataStrIterator.next();
                        String ax = dataStrIterator.next();
                        String ay = dataStrIterator.next();
                        String az = dataStrIterator.next();
                        String sourceId = dataStrIterator.next();
                        String sequenceNum = dataStrIterator.next();
                        Object[] eventData = new Object[]{
                                sid,
                                Long.parseLong(ts), //Since this value is in pico seconds we
                                // have to multiply by 1000M (10^9) to make milliseconds to picoseconds
                                Integer.parseInt(x), //This can be represented by two bytes
                                Integer.parseInt(y),
                                Integer.parseInt(z),
                                Integer.parseInt(v_abs),
                                Integer.parseInt(a_abs),
                                Integer.parseInt(vx),
                                Integer.parseInt(vy),
                                Integer.parseInt(vz),
                                Integer.parseInt(ax),
                                Integer.parseInt(ay),
                                Integer.parseInt(az),
                                sourceId,
                                Long.parseLong(sequenceNum)
                        };
                        Event event = new Event(System.currentTimeMillis(), eventData);
                        events.put(event);
                        line = br.readLine();
                        count++;
                    } catch (NumberFormatException ignored) {
                        line = br.readLine();
                    }
                }
                this.completed = true;
                System.out.println("Total amount of sensorBasedEvents read : " + count);
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        @Override
        public void run() {
            loadData();
        }
    }
}
