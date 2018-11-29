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
package org.siddhi.simulator.server.debs;

import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.SequenceBasedReorderExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SiddhiServer {
    private static final Logger log = Logger.getLogger(SiddhiServer.class);
    private static long lastDataReceivedTimestamp = 0;
    private static int totalEvents = 0;
    private static double averageLatency = 0.0;
    private static int totalOOEvents = 0;
    private static String outputPath = "/Users/sinthu/wso2/sources/personal/git/test-results/results1.csv";

    public static void main(String[] args) {
        SiddhiManager siddhiManager = new SiddhiManager();
        if (args.length == 1) {
            outputPath = args[0];
        }
        String inStreamDefinition = "@app:name('TestServer')" +
                "@source(type='tcp', @map(type='binary')) " +
                "define stream inputStream (sid int, ts long, x int, y int, z int, v_abs int, a_abs int, vx int, " +
                "vy int, vz int, ax int, ay int, az int, sourceId string, seqNum long);";

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);

        String query = ("@info(name = 'query1') " +
                "from inputStream#reorder:sequence(sourceId, seqNum, ts, 40L, false) " +
                "select sourceId, seqNum, eventTimestamp() as relativeTimestamp, ts " +
                "insert into outputStream;");

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            private int count = 0;
            private long latency = 0;
            private long lastEventTime = 0;
            private int ooOrdereventsCount = 0;

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    latency = latency + (System.currentTimeMillis() - (Long) event.getData()[2]);
                    count++;
                    long currentEventTime = (Long) event.getData()[3];
                    if (lastEventTime == 0) {
                        lastEventTime = currentEventTime;
                    } else {
                        if (currentEventTime < lastEventTime) {
                            ooOrdereventsCount++;
                        }
                        lastEventTime = currentEventTime;
                    }
                }
//                totalEvents = count;
//                averageLatency = latency / count;
//                totalOOEvents = ooOrdereventsCount;
                System.out.println("------------------------------------");
                System.out.println("Total Events => " + count);
                System.out.println("Average Latency => " + latency / count);
                System.out.println("Out of order total events => " + ooOrdereventsCount);
                System.out.println("------------------------------------");
            }
        });
//        ResultRecorder resultRecorder = new ResultRecorder();
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        executorService.submit(resultRecorder);
        executionPlanRuntime.start();
//        while (!resultRecorder.completed) {
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException ignored) {
//            }
//        }
//        System.exit(0);
    }

    public static class ResultRecorder implements Runnable {
        private boolean completed = false;

        @Override
        public void run() {
            while (true) {
                if (lastDataReceivedTimestamp == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                } else if (lastDataReceivedTimestamp > System.currentTimeMillis() - 10000) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {

                    }
                } else {
                    System.out.println("Writing the results");
                    String output = totalEvents + "," + averageLatency + "," + totalOOEvents + "\n";

                    BufferedWriter writer;
                    try {
                        writer = new BufferedWriter(new FileWriter(outputPath));
                        writer.write(output);
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    completed = true;
                    break;
                }

            }
        }
    }


}
