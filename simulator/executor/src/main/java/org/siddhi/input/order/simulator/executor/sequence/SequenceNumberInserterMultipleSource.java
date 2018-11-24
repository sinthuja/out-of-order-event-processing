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
package org.siddhi.input.order.simulator.executor.sequence;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class SequenceNumberInserterMultipleSource {
    private static Splitter splitter = Splitter.on(',');
    private static TreeMap<Integer, TreeSet<SensorEvent>> sensorBasedEvents = new TreeMap<>();
    private static List<SensorEvent> readOrderEvents = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        String[] datasetNames = new String[]{"dataset1", "dataset2", "dataset3", "dataset4_1", "dataset4_2",
                "dataset5_1", "dataset5_2"};
        int[] numOfSources = new int[]{2, 5, 10, 20, -1};
        for (int numOfSource : numOfSources) {
            for (String datasetName : datasetNames) {
                String sourceFile = "/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/" + datasetName;
                String dirName;
                if (numOfSource != -1) {
                    dirName = numOfSource + "-source";
                } else {
                    dirName = "all-source";
                }
                String destinationFile = "/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence/" +
                        "multiple-source/out-of-order/" + dirName + "/" + datasetName;
                System.out.print("Reading Source file..");
                populateEvents(sourceFile);
                System.out.println("Merge Sources");
                mergeSources(numOfSource);
                System.out.print("Adding sequence number..");
                addSequenceNumber(numOfSource);
                System.out.print("Writing the results to file");
                writeToFile(destinationFile);
                sensorBasedEvents.clear();
                readOrderEvents.clear();
            }
        }
    }

    private static void mergeSources(int numberOfSources) {
        if (numberOfSources != -1) {
            List<Integer> initialSensorIds = new ArrayList<>();
            int index = 0;
            for (Integer sensorId : sensorBasedEvents.keySet()) {
                if (index < numberOfSources) {
                    initialSensorIds.add(sensorId);
                    index++;
                } else {
                    break;
                }
            }
            while (sensorBasedEvents.size() > numberOfSources) {
                int mergeSensorIndex = sensorBasedEvents.size() % numberOfSources;
                TreeSet<SensorEvent> orginalData = sensorBasedEvents.get(initialSensorIds.get(mergeSensorIndex));
                TreeSet<SensorEvent> mergingSensorData = sensorBasedEvents.remove(sensorBasedEvents.lastEntry().getKey());
                orginalData.addAll(mergingSensorData);
            }
        }
        System.out.println("Total sources: " + sensorBasedEvents.size());
        for (Integer sensorIndex : sensorBasedEvents.keySet()) {
            System.out.println("Size of sensor index:  " + sensorIndex + " : "
                    + sensorBasedEvents.get(sensorIndex).size());
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
                    readOrderEvents.add(sensorEvent);
                    int sensorIndex = sid;
                    TreeSet<SensorEvent> events = sensorBasedEvents.computeIfAbsent(sensorIndex, k -> new TreeSet<>());
                    events.add(sensorEvent);
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

    private static void addSequenceNumber(int numOfSources) {
        int sensorId = 0;
        for (Map.Entry<Integer, TreeSet<SensorEvent>> entry : sensorBasedEvents.entrySet()) {
            if (numOfSources == -1) {
                sensorId = entry.getKey();
            }
            long seqNum = 1;
            for (SensorEvent event : entry.getValue()) {
                event.updateEvent(seqNum, sensorId);
                seqNum++;
            }
            if (numOfSources != -1) {
                sensorId++;
            }

        }
    }

    private static void writeToFile(String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        int count = 0;
        for (SensorEvent sensorEvent : readOrderEvents) {
            String eventData = sensorEvent.getEventLine();
            if (count != readOrderEvents.size() - 1) {
                eventData = eventData + "\n";
            }
            writer.write(eventData);
        }
        writer.close();
    }
}
