/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.reorder;

import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.SequenceBasedReorderExtension;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SequenceReorderTestCase {
    private static final Logger log = Logger.getLogger(SequenceReorderTestCase.class);
    private int count;
    private static final String SOURCE_ID = "source1";
    long startTime;
    long endTime;

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void orderTest() throws InterruptedException {
        log.info("SequenceReorderTestCase TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (sourceId string, seqNum long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:sequence(sourceId, seqNum) select seqNum, volume " +
                "insert into outputStream;");
        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;
                    System.out.println("results: " + event.getData()[0]);
                }
                endTime = System.currentTimeMillis();
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        //The following implements the out-of-order disorder handling scenario described in the
        //http://dl.acm.org/citation.cfm?doid=2675743.2771828
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 1L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 2L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 3L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 7L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 6L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 5L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 4L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 8L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 10L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 9L, 200L});

        Thread.sleep(2000);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertTrue("Event count is at least 9:", count == 10);
    }


    @Test()
    public void orderTestWithTimeout() throws InterruptedException {
        log.info("SequenceReorderTestCase TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (sourceId string, seqNum long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:sequence(sourceId, seqNum) select seqNum, volume " +
                "insert into outputStream;");
        siddhiManager.setExtension("reorder:sequence", SequenceBasedReorderExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;
                    System.out.println("results: " + event.getData()[0]);
                }
                endTime = System.currentTimeMillis();
//                System.out.println("Latency ==> " + (endTime - startTime));
//                System.out.println("Count ========> " + count);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        //The following implements the out-of-order disorder handling scenario described in the
        //http://dl.acm.org/citation.cfm?doid=2675743.2771828
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 1L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 2L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 3L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 7L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 6L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 5L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 4L, 100L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 8L, 200L});
        startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{SOURCE_ID, 10L, 200L});
//        startTime = System.currentTimeMillis();
//        inputHandler.send(new Object[]{SOURCE_ID, 9L, 500L});

        Thread.sleep(2000);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertTrue("Event count is at least 9:", count == 9);
    }

}
