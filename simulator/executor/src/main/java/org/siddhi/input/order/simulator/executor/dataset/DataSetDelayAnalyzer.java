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
import java.util.Iterator;
import java.util.List;

public class DataSetDelayAnalyzer{
    private static Splitter splitter = Splitter.on(',');
    private static List<SensorEvent> outofOrderEvents = new ArrayList<>();
    private static int batchSize = 10000;

    public static void main(String[] args) {
        populateEvents("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/single-source/in-order/dataset3", outofOrderEvents);
        StringBuilder output = new StringBuilder();
        int batch = 1;
        int iteration = 1;
        long lastTimestamp = 0;
        long totalDelay = 0;
        for (int i = 0; i < outofOrderEvents.size(); i++) {
            Long timestamp = outofOrderEvents.get(i).getTimestamp();
            if (lastTimestamp == 0) {
                lastTimestamp = timestamp;
            } else {
                long currentDelay = Math.abs(timestamp - lastTimestamp);
                totalDelay += currentDelay;
                lastTimestamp = timestamp;
            }
            if (batch == batchSize) {
//                System.out.println(gap);
                output.append(iteration * batch).append(",");
                double doubleTotalDelay = totalDelay;

                output.append((totalDelay/(double) batch)/1000000).append("\n");
                iteration++;
                batch = 1;
                lastTimestamp = 0;
                totalDelay = 0;
            } else {
                batch++;
            }
        }
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(
                    "/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/data-set-delay-variation/delay.csv"));
            writer.write(output.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void populateEvents(String fileName, List<SensorEvent> eventList) {
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
                    eventList.add(sensorEvent);
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
