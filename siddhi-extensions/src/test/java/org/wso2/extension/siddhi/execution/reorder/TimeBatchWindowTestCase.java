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
package org.wso2.extension.siddhi.execution.reorder;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.siddhi.extension.disorder.handler.SequenceBasedReorderExtension;
import org.siddhi.extension.disorder.handler.TimeBatchWindowProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class TimeBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(TimeBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void timeWindowBatchTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#reorder:timeBatch(1 sec, 0, 10 milliseconds) " +
                "select symbol, sum(price) as sumPrice , volume, windowType " +
                "insert all events into outputStream ;";

        siddhiManager.setExtension("reorder:timeBatch", TimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEventCount == 0) {
                    Assert.assertTrue("Remove Events will only arrive after the second time period. ", removeEvents
                            == null);
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                } else if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(10000);
        Assert.assertEquals(1, inEventCount);
        Assert.assertEquals(1, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void timeWindowBatchTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#reorder:timeBatch(1 sec, 0, 10 milliseconds) " +
                "select symbol, sum(ifThenElse(windowType=='LOW',price, 0.0f)) as sumLowPrice , " +
                "sum(ifThenElse(windowType=='MIDDLE',price, 0.0f)) as sumMiddlePrice, " +
                "sum(ifThenElse(windowType=='HIGH',price, 0.0f)) as sumHighPrice, " +
                "volume, windowType " +
                "insert all events into outputStream;";

        siddhiManager.setExtension("reorder:timeBatch", TimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream",new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(10000);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void timeWindowBatchTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#reorder:timeBatch(1 sec, 0, 10 milliseconds, true) " +
                "select symbol, sum(ifThenElse(windowType=='LOW',price, 0.0f)) as sumLowPrice , " +
                "sum(ifThenElse(windowType=='MIDDLE',price, 0.0f)) as sumMiddlePrice, " +
                "sum(ifThenElse(windowType=='HIGH',price, 0.0f)) as sumHighPrice, " +
                "volume, windowType " +
                "insert all events into outputStream;";

        siddhiManager.setExtension("reorder:timeBatch", TimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream",new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(10000);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void timeWindowBatchTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#reorder:timeBatch(1 sec, 0, 10 milliseconds, false, true) " +
                "select symbol, sum(ifThenElse(windowType=='LOW',price, 0.0f)) as sumLowPrice , " +
                "sum(ifThenElse(windowType=='MIDDLE',price, 0.0f)) as sumMiddlePrice, " +
                "sum(ifThenElse(windowType=='HIGH',price, 0.0f)) as sumHighPrice," +
                "sum(ifThenElse(windowType=='FULL',price, 0.0f)) as sumFullPrice, " +
                "volume, windowType " +
                "insert all events into outputStream;";

        siddhiManager.setExtension("reorder:timeBatch", TimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream",new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(10000);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }



}
