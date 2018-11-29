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
package org.siddhi.input.order.simulator.executor;

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
import java.util.Random;

public class SequenceOOSimulator {
    private static Splitter splitter = Splitter.on(',');
    private static List<SensorEvent> ooSensorEvents = new ArrayList<>();
    private static int batchSize = 10000;

    public static void main(String[] args) throws IOException {
        String[] datasets = new String[]{"dataset1", "dataset2", "dataset3", "dataset1", "dataset2", "dataset3"};
//        String[] datasets = new String[]{"dataset1"};
        int[] ooGaps = new int[]{2, 10, 50, 100, 1000, 4999};
        Random random = new Random(10L);
        for (int ooGap : ooGaps) {
            int[] ooPercentage = new int[]{300, 700, 2000, 4000, 7000, 12000};
            int[] dirName = new int[]{10, 20, 40, 60, 80, 90};
            int dirIndex = 0;
            for (int aPercentage : ooPercentage) {
                BufferedReader inorderFileBr = null;
                try {
                    inorderFileBr = new BufferedReader(new FileReader
                            ("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence-2/in-order-events/1-source/"
                                    + datasets[dirIndex]),
                            10 * 1024 * 1024);
                    List<SensorEvent> events = getData(inorderFileBr);

                    while (events != null) {
                        int i = 0;
                        while (i < aPercentage) {
                            int index = random.nextInt(batchSize);
                            int toIndex = getNextIndex(random, index, ooGap);
                            SensorEvent event1 = events.get(index);
                            SensorEvent event2 = events.get(toIndex);
                            events.set(toIndex, event1);
                            events.set(index, event2);
                            i++;
                        }
                        ooSensorEvents.addAll(events);
                        events = getData(inorderFileBr);
                    }
                    writeToFile("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence-2/" +
                            "out-of-order/1-source/" + dirName[dirIndex] + "/" + datasets[dirIndex] +"_"+ ooGap);
                    ooSensorEvents.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (inorderFileBr != null) {
                        inorderFileBr.close();
                    }
                }
                dirIndex++;
            }
        }
    }

    private static int getNextIndex(Random random, int current, int bound) {
        int gap = 0;
        if (bound == 2) {
            gap = 1;
        } else {
            gap = random.nextInt(bound);
        }
        while (gap == 0) {
            gap = random.nextInt(bound);
        }
        if ((gap + current < batchSize - 1) && (current - gap >= 0)) {
            boolean highIndex = random.nextBoolean();
            if (highIndex) {
                return gap + current;
            } else {
                return current - gap;
            }
        } else if ((gap + current < batchSize - 1)) {
            return gap + current;
        } else {
            return current - gap;
        }
    }

    private static List<SensorEvent> getData(BufferedReader br) throws IOException {
        List<SensorEvent> events = new ArrayList<>();
        String line = br.readLine();
        int totalRecords = 0;
        while (line != null) {
            Iterator<String> dataStrIterator = splitter.split(line).iterator();
            String sensor = dataStrIterator.next();
            String timestamp = dataStrIterator.next();
            SensorEvent sensorEvent = new SensorEvent(Long.parseLong(timestamp), line);
            events.add(sensorEvent);
            totalRecords++;
            if (totalRecords == batchSize) {
                return events;
            }
            line = br.readLine();
        }
        return null;
    }

    private static void writeToFile(String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        int count = 0;
        for (SensorEvent sensorEvent : ooSensorEvents) {
            String eventData = sensorEvent.getEventLine();
            if (count != ooSensorEvents.size() - 1) {
                eventData = eventData + "\n";
            }
            writer.write(eventData);
        }
        writer.close();
    }
}
