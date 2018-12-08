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
import org.junit.Before;
import org.junit.Test;
import org.siddhi.extension.disorder.handler.ExternalTimeBatchWindowProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.junit.Assert.assertEquals;

public class ExternalTimeBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(ExternalTimeBatchWindowTestCase.class);
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
    public void testExternalTimeBatchWindowOrginal() throws InterruptedException {
        log.info("ExternalTimeBatchWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream#window.externalTimeBatch(timestamp, 1 sec) " +
                "select symbol, sum(price) as sumPrice , volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "IBM", 700f, 0});
        inputHandler.send(new Object[]{1366335804342L, "WSO2", 60f, 2});
        inputHandler.send(new Object[]{1366335814341L, "FOO", 400f, 4});
        inputHandler.send(new Object[]{1366335814345L, "BAR", 800f, 5});
        inputHandler.send(new Object[]{1366335824341L, "KKK", 200f, 1});

        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExternalTimeBatchWindow1() throws InterruptedException {
        log.info("ExternalTimeBatchWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream#reorder:externalTimeBatch(1 sec,timestamp, false, false, 10 milliseconds) " +
                "select symbol, sum(price) as sumPrice , volume, windowType " +
                "insert into outputStream ;";

        siddhiManager.setExtension("reorder:externalTimeBatch", ExternalTimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "IBM", 700f, 0});
        inputHandler.send(new Object[]{1366335804342L, "WSO2", 60f, 2});
        inputHandler.send(new Object[]{1366335814341L, "FOO", 400f, 4});
        inputHandler.send(new Object[]{1366335814345L, "BAR", 800f, 5});
        inputHandler.send(new Object[]{1366335824341L, "KKK", 200f, 1});

        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExternalTimeBatchWindow2() throws InterruptedException {
        log.info("ExternalTimeBatchWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream#reorder:externalTimeBatch(1 sec,timestamp, true, false, 10 milliseconds) " +
                "select symbol, sum(ifThenElse(windowType=='LOW',price, 0.0f)) as sumLowPrice , " +
                "sum(ifThenElse(windowType=='MIDDLE',price, 0.0f)) as sumMiddlePrice, " +
                "sum(ifThenElse(windowType=='HIGH',price, 0.0f)) as sumHighPrice, " +
                "volume, windowType " +
                "insert into outputStream;";;

        siddhiManager.setExtension("reorder:externalTimeBatch", ExternalTimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "IBM", 700f, 0});
        inputHandler.send(new Object[]{1366335804342L, "WSO2", 60f, 2});
        inputHandler.send(new Object[]{1366335814341L, "FOO", 400f, 4});
        inputHandler.send(new Object[]{1366335814345L, "BAR", 800f, 5});
        inputHandler.send(new Object[]{1366335824341L, "KKK", 200f, 1});

        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExternalTimeBatchWindow3() throws InterruptedException {
        log.info("ExternalTimeBatchWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream#reorder:externalTimeBatch(1 sec,timestamp, false, false, 10 milliseconds) " +
                "select symbol, (sum(ifThenElse(windowType=='LOW',price, 0.0f))+  " +
                "sum(ifThenElse(windowType=='MIDDLE',price, 0.0f))+ " +
                "sum(ifThenElse(windowType=='HIGH',price, 0.0f)))/3 as sumPrice," +
                "volume, windowType " +
                "insert into outputStream;";

        siddhiManager.setExtension("reorder:externalTimeBatch", ExternalTimeBatchWindowProcessor.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "IBM", 700f, 0});
        inputHandler.send(new Object[]{1366335804342L, "WSO2", 60f, 2});
        inputHandler.send(new Object[]{1366335814341L, "FOO", 400f, 4});
        inputHandler.send(new Object[]{1366335814345L, "BAR", 800f, 5});
        inputHandler.send(new Object[]{1366335824341L, "KKK", 200f, 1});

        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }
}
