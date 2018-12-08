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
package org.siddhi.simulator.server.aggregator;

import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.SequenceBasedReorderExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiServer {
    private static final Logger log = Logger.getLogger(SiddhiServer.class);

    public static void main(String[] args) {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@app:name('TestServer')" +
                "@source(type='tcp', @map(type='binary')) " +
                "define stream inputStream (sid int, ts long, x int, y int, z int, v_abs int, a_abs int, vx int, " +
                "vy int, vz int, ax int, ay int, az int, sourceId string, seqNum long, driftedTs long);";

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);

//        String query = ("@info(name = 'query1') " +
//                "from inputStream#reorder:sequence(sourceId, seqNum, driftedTs, 500L, false) " +
//                "select  sourceId, seqNum, eventTimestamp() as relativeTimestamp, ts/1000000000L as occuredTime, v_abs " +
//                "insert into outputStream;");
//
//        String aggregation = "from outputStream#window.externalTimeBatch(occuredTime, 10 sec) " +
//                "select sum(v_abs) as sumVelocity, occuredTime " +
//                "insert into aggregateOutputStream";

                String query = ("@info(name = 'query1') " +
                "from inputStream#reorder:sequence(sourceId, seqNum, driftedTs, 500L, false) " +
                "select  sourceId, seqNum, eventTimestamp() as relativeTimestamp, ts/1000000000L as occuredTime, v_abs " +
                "insert into outputStream;");

        String aggregation = "from outputStream#reorder:externalTimeBatch(occuredTime, 10 sec, true) " +
                "select sum(v_abs) as sumVelocity, occuredTime " +
                "insert into aggregateOutputStream";

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query + aggregation);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            private int count = 0;
            private long latency = 0;
            private long lastEventTime = 0;
            private int ooOrdereventsCount = 0;
            private long maxLatency = -1;
            private long minLatency = -1;
            private int batchSize = 10000;
            private int batch = 1;
            private int iteration = 1;

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    long delay = System.currentTimeMillis() - (Long) event.getData()[2];
                    latency = latency + delay;

                    if (minLatency == -1) {
                        minLatency = delay;
                    } else if (minLatency > delay) {
                        minLatency = delay;
                    }
                    if (maxLatency == -1) {
                        maxLatency = delay;
                    } else if (maxLatency < delay) {
                        maxLatency = delay;
                    }
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
                    if (batch == batchSize) {
//                        System.out.println(batch * iteration + "," + (latency / (double) batch)
//                                + "," + (maxLatency) + "," + ooOrdereventsCount);
                        maxLatency = -1;
                        minLatency = -1;
                        batch = 1;
                        latency = 0;
                        ooOrdereventsCount = 0;
                        iteration++;
                    } else {
                        batch++;
                    }
                }
            }
        });

        executionPlanRuntime.addCallback("aggregateOutputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    System.out.println(event.getData()[0] + "," + event.getData()[1]);
                }
            }
        });
        executionPlanRuntime.start();
    }
}
