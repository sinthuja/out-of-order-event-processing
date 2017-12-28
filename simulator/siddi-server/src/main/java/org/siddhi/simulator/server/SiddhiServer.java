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
package org.siddhi.simulator.server;

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
                "define stream inputStream (sourceId string, seqNum long, volume long, eventTime long);";

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);

        String query = ("@info(name = 'query1') from inputStream#reorder:sequence(sourceId, seqNum, eventTime) select seqNum, volume, eventTime " +
                "insert into outputStream;");

        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            private int count = 0;
            private long latency = 0;

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    latency = latency + (System.currentTimeMillis() - (Long)event.getData()[2]);
               //     System.out.println("results: " + event.getData()[0]);
                    count++;
                }
//                System.out.println("Count => "+ count);
                System.out.println("Average Latency => "+ latency/count);
            }
        });
        executionPlanRuntime.start();
    }


}
