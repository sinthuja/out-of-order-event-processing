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
package org.siddhi.simulator.server.pattern.matching;

import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.SequenceBasedReorderExtension;
import org.wso2.extension.siddhi.execution.math.AbsFunctionExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiServer {
    private static final Logger log = Logger.getLogger(org.siddhi.simulator.server.aggregator.SiddhiServer.class);

    public static void main(String[] args) {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@app:name('TestServer')" +
                "@source(type='tcp', @map(type='binary')) " +
                "define stream inputStream (sid int, ts long, x int, y int, z int, v_abs int, a_abs int, vx int, " +
                "vy int, vz int, ax int, ay int, az int, sourceId string, seqNum long, driftedTs long);";

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);

//        String query = ("@info(name = 'query1') " +
//                "from inputStream#reorder:sequence(sourceId, seqNum, driftedTs, 500L, false) " +
//                "select  sourceId, seqNum, eventTimestamp() as relativeTimestamp, ts as occuredTime, v_abs " +
//                "insert into outputStream;");
//
//        String aggregation = "from outputStream#window.externalTimeBatch(occuredTime, 10000000000 sec) " +
//                "select avg(v_abs) as sumVelocity, occuredTime " +
//                "insert into aggregateOutputStream";

        String query = ("@info(name = 'query1') " +
                "from inputStream#reorder:sequence(sourceId, seqNum, driftedTs, 500L, false) " +
                "select  sid, ts, x, y, z, v_abs, a_abs, sourceId, seqNum, eventTimestamp() as eventTime, driftedTs, relativeTimestamp  " +
                "insert into outputStream;");

//        String pattern = "from every e1=outputStream [sourceId == '0' and (x>29880 or x<22560) and y> (-33968) and y <33965 and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
//                " -> e2=outputStream [sourceId == '1' and (x<=29898 and x>22579) and y<= (-33968) and z<2440 and a_abs>=55000 and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
//                "select e2.sourceId, e2.ts, e2.driftedTs, e1.ts as e1Ts, e1.driftedTs as e1drift " +
//                "insert into patternStream;";


        String pattern = "from every e1=outputStream [sourceId == '0' and (x>29880 or x<22560) and y> (-33968) and y <33965 and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
                " -> e2=outputStream [sourceId == '1' and (x<=29898 and x>22579) and y<= (-33968) and z<2440 and a_abs>=55000 and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
                "select ifThenElse(e2.relativeTimestamp - e1.relativeTimestamp<=50000000000L ,'uncertainRange1', 'confirmed') as confidence, e2.sourceId, e2.ts, e2.driftedTs, e1.ts as e1Ts, e1.driftedTs as e1drift " +
                "insert into patternStream;";

        String pattern2 = "from every e2=outputStream [sourceId == '1' and (x<=29898 and x>22579) and y<= (-33968) and z<2440 and a_abs>=55000 and (sid==4 or sid ==12 or sid==10 or sid==8)] " +
                " -> e1=outputStream [sourceId == '0' and (x>29880 or x<22560) and y> (-33968) and y <33965 and (sid==4 or sid ==12 or sid==10 or sid==8)]" +
                "select ifThenElse(e1.relativeTimestamp - e2.relativeTimestamp<=50000000000L ,'uncertainRange2', 'notMatch') as confidence, e2.sourceId, e2.ts, e2.driftedTs, e1.ts as e1Ts, e1.driftedTs as e1drift " +
                "having confidence == 'uncertainRange2' " +
                "insert into patternStream;";


        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query + pattern + pattern2);

        executionPlanRuntime.addCallback("patternStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    Object[] data = event.getData();
                    StringBuilder msg = new StringBuilder();
                    for (Object aData : data) {
                        msg.append(aData).append(",");
                    }
                    System.out.println(msg);
                }
            }
        });
        executionPlanRuntime.start();
    }
}
