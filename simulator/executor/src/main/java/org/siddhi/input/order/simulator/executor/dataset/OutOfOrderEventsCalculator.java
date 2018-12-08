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
package org.siddhi.input.order.simulator.executor.dataset;

import com.google.common.base.Splitter;
import org.siddhi.input.order.simulator.executor.sequence.SensorEvent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OutOfOrderEventsCalculator {
    private static Splitter splitter = Splitter.on(',');
    private static HashMap<Integer, List<SensorEvent>> allEvents = new HashMap<>();
    private static int batchSize = 10000;

    public static void main(String[] args) {
        populateEvents("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/multiple-source/out-of-order/5-source/dataset3");
//        populateEvents("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/multiple-source/in-order/5-source/dataset3");

        for (Map.Entry<Integer, List<SensorEvent>> sensorEvents : allEvents.entrySet()) {
            long lastTimestamp = -1;
            long ooOrderEvents = 0;
            List<SensorEvent> outofOrderEvents = sensorEvents.getValue();
            for (int i = 0; i < outofOrderEvents.size(); i++) {
                Long timestamp = outofOrderEvents.get(i).getTimestamp();
                 if (lastTimestamp > timestamp){
                   ooOrderEvents++;
                }
                lastTimestamp = timestamp;
            }
            System.out.println("Source : "+ sensorEvents.getKey());
            System.out.println("OOrder events: \t"+ ooOrderEvents);
            System.out.println("Total events: \t\t"+ outofOrderEvents.size());
        }
    }

    private static void populateEvents(String fileName) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            int count = 0;
            while (line != null && !line.isEmpty()) {
                try {
                    Iterator<String> dataStrIterator = splitter.split(line).iterator();
                    Integer sid = Integer.parseInt(dataStrIterator.next()); //sensor id
                    String ts = dataStrIterator.next(); //Timestamp in pico seconds
                    SensorEvent sensorEvent = new SensorEvent(Long.parseLong(ts), line);
                    List<SensorEvent> sensorEvents = allEvents.computeIfAbsent(sensorEvent.getSensorId(), k -> new ArrayList<>());
                    sensorEvents.add(sensorEvent);
                    count++;
                    line = br.readLine();
                } catch (NumberFormatException ignored) {
                    line = br.readLine();
                }
            }
            System.out.println("Total amount of sensorBasedEvents read : " + count);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
