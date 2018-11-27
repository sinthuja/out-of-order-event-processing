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
package org.siddhi.simulator.client.debs.multiple;

import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.siddhi.simulator.client.SingleEventSourcePublisher0;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class MultipleSource {
    private static final Logger log = Logger.getLogger(SingleEventSourcePublisher0.class);
    private static Splitter splitter = Splitter.on(',');
    private static final int bundleSize = 100;
    private static Map<String, LinkedBlockingQueue<Event>> eventsQueue = new HashMap<>();
    public static boolean START = false;

    public static void main(String[] args) {
        String path = "/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/multiple-source/out-of-order/2-source/dataset2";
        loadData(path);
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
        for (Map.Entry<String, LinkedBlockingQueue<Event>> entry : eventsQueue.entrySet()) {
            AsyncSource source = new AsyncSource(entry.getKey(), types, entry.getValue(), bundleSize);
            Executors.newFixedThreadPool(eventsQueue.size()).submit(source);
        }
        START = true;
    }

    private static void loadData(String filePath) {
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
                    LinkedBlockingQueue<Event> queue = eventsQueue.get(sourceId);
                    if (queue == null) {
                        queue = new LinkedBlockingQueue<>();
                        eventsQueue.putIfAbsent(sourceId, queue);
                    }
                    queue.put(event);
                    line = br.readLine();
//                    if (count > 100000) {
//                        break;
//                    }
                    count++;
                } catch (NumberFormatException ignored) {
                    line = br.readLine();
                }
            }
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
}
