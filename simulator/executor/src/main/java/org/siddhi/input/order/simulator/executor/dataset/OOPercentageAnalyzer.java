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
import java.io.FileReader;
import java.util.Iterator;

public class OOPercentageAnalyzer {
    private static Splitter splitter = Splitter.on(',');


    public static void main(String[] args) {
        String[] datasets = new String[]{"dataset1", "dataset2", "dataset3", "dataset1", "dataset2", "dataset3", "dataset1", "dataset2", "dataset3"};
        int[] ooGaps = new int[]{2, 10, 50, 100, 1000, 4999};
        int[] ooPercentage = new int[]{10, 20, 40, 60, 80, 90};
        int index = 0;
        for (int ooP : ooPercentage) {
            for (int ooGap : ooGaps) {
                try {
                    String dataset = datasets[index] + "_" + ooGap;
                    BufferedReader br = new BufferedReader(
                            new FileReader("/Users/sinthu/wso2/sources/personal/git/AK-Slack/datasets/sequence-2/out-of-order/1-source/" + ooP + "/" + dataset), 10 * 1024 * 1024);
                    String line = br.readLine();
                    int count = 0;
                    int ooOrder = 0;
                    long lastTimestamp = 0;
                    while (line != null && !line.isEmpty()) {
                        try {
                            Iterator<String> dataStrIterator = splitter.split(line).iterator();
                            Integer sid = Integer.parseInt(dataStrIterator.next()); //sensor id
                            String ts = dataStrIterator.next(); //Timestamp in pico seconds
                            long timestamp = Long.parseLong(ts);
                            if (lastTimestamp == 0) {
                                lastTimestamp = timestamp;
                            } else {
                                if (timestamp < lastTimestamp) {
                                    ooOrder++;
                                }
                                lastTimestamp = timestamp;
                            }
                            count++;
                            line = br.readLine();
                        } catch (NumberFormatException ignored) {
                            line = br.readLine();
                        }
                    }
                    System.out.println("--------------------------------------------");
                    System.out.println("Percentage = " + ooP + ", Dataset = " + dataset);
                    System.out.println("Total amount of sensorBasedEvents read : " + count);
                    System.out.println("Total oo order events : " + ooOrder);
                    double percentage = ((double) ooOrder) / ((double) count);
                    System.out.println("OO percentage: " + percentage * 100);
                    System.out.println("--------------------------------------------");
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            index++;
        }

    }
}
