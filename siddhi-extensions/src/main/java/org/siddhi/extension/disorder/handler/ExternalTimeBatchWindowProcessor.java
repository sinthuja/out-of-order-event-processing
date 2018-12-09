/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.siddhi.extension.disorder.handler;

import org.siddhi.extension.disorder.handler.multi.source.MultiSourceEventSynchronizer;
import org.siddhi.extension.disorder.handler.multi.source.MultiSourceEventSynchronizerManager;
import org.siddhi.extension.disorder.handler.storage.WindowStateStore;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link WindowProcessor} which represent a Batch Window operating based on time.
 */
@Extension(
        name = "externalTimeBatch",
        namespace = "reorder",
        description = "A batch (tumbling) time window that holds events that arrive during window.time periods, " +
                "and gets updated for each window.time.",
        parameters = {
                @Parameter(name = "window.time",
                        description = "The batch time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "timestamp",
                        description = "This is the field that specifies the timestamp which it needs to be ordered.",
                        type = {DataType.LONG}),
                @Parameter(name = "window.uncertain.time",
                        description = "The uncertain time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "window.immediate.emit",
                        description = "Whether to emit immediately or once all window types are ready",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "timeBatch(20) output all events;\n" +
                                "@info(name = 'query0')\n" +
                                "from cseEventStream\n" +
                                "insert into cseEventWindow;\n" +
                                "@info(name = 'query1')\n" +
                                "from cseEventWindow\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert all events into outputStream ;",
                        description = "This will processing events arrived every 20 milliseconds" +
                                " as a batch and out put all events."
                )
        }
)
public class ExternalTimeBatchWindowProcessor extends StreamProcessor implements SchedulingProcessor {

    private long timeInMilliSeconds;
    private Scheduler scheduler;
    private SiddhiAppContext siddhiAppContext;
    private long uncertainWindowRange = -1;
    private List<ExternalTimeBatchWindow> windows = new ArrayList<>();
    private Map<ExternalTimeBatchWindow.Type, ComplexEventChunk<StreamEvent>> sendToNextReady = new HashMap<>();
    private boolean emitImmediate = false;
    private boolean sendFullWindow = false;
    private WindowStateStore windowStateStore = new WindowStateStore();
    private MultiSourceEventSynchronizer eventSynchronizer;
    private VariableExpressionExecutor timestampExecutor;
    private AtomicBoolean isInit = new AtomicBoolean(false);
    private String streamId;

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.streamId = "inputStream";
        this.eventSynchronizer = MultiSourceEventSynchronizerManager.getInstance()
                .getMultiSourceEventSynchronizer(streamId);
        if (attributeExpressionExecutors.length == 1) {
            checkFirstParameter();
        } else if (attributeExpressionExecutors.length == 2) {
            checkFirstParameter();
            // start time
            checkSecondParameter();
        } else if (attributeExpressionExecutors.length == 3) {
            checkFirstParameter();
            // start time
            checkSecondParameter();
            //uncertainWindowrange
            checkThirdParameter();
        } else if (attributeExpressionExecutors.length == 4) {
            checkFirstParameter();
            // start time
            checkSecondParameter();
            //uncertainWindowrange
            checkThirdParameter();
            //emitImmediateResults
            checkFourthParameter();
        } else if (attributeExpressionExecutors.length == 5) {
            checkFirstParameter();
            // start time
            checkSecondParameter();
            //uncertainWindowrange
            checkThirdParameter();
            //emitImmediateResults
            checkFourthParameter();
            //SendFullWindow
            checkFifthParameter();
        } else {
            throw new SiddhiAppValidationException("Time window should only have one or two parameters. " +
                    "(<int|long|time> windowTime), but found " +
                    attributeExpressionExecutors.length + " input " +
                    "attributes");
        }

        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(Constants.WINDOW_TYPE_ATTRIBUTE, Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            //TODO: load the uncertain time dynamically after each expiry.
            long uncertainWindowBuffer;
            if (uncertainWindowRange == -1) {
                if (this.eventSynchronizer == null){
                    this.eventSynchronizer = MultiSourceEventSynchronizerManager.getInstance()
                            .getMultiSourceEventSynchronizer(this.streamId);
                }
                uncertainWindowBuffer = this.eventSynchronizer.getUncertainTimeRange();
            } else {
                uncertainWindowBuffer = this.uncertainWindowRange;
            }
            if (isInit.compareAndSet(false, true)) {
                StreamEvent firstEvent = streamEventChunk.getFirst();
                if (firstEvent != null && firstEvent.getType() == ComplexEvent.Type.CURRENT) {
                    long currentTime = (Long) timestampExecutor.execute(firstEvent);
                    this.windows.add(new ExternalTimeBatchWindow(timestampExecutor, timeInMilliSeconds,
                            outputExpectsExpiredEvents, currentTime - uncertainWindowBuffer,
                            complexEventPopulater, ExternalTimeBatchWindow.Type.LOW));
                    this.windows.add(new ExternalTimeBatchWindow(timestampExecutor, timeInMilliSeconds,
                            outputExpectsExpiredEvents, currentTime,
                            complexEventPopulater, ExternalTimeBatchWindow.Type.MIDDLE));
                    this.windows.add(new ExternalTimeBatchWindow(timestampExecutor, timeInMilliSeconds,
                            outputExpectsExpiredEvents, currentTime + uncertainWindowBuffer,
                            complexEventPopulater, ExternalTimeBatchWindow.Type.HIGH));
//                    if (this.sendFullWindow) {
//                        this.windows.add(new TimeBatchWindow(nextEmitTime + uncertainWindowBuffer,
//                                2 * uncertainWindowBuffer, timeInMilliSeconds, outputExpectsExpiredEvents,
//                                complexEventPopulater, scheduler, TimeBatchWindow.Type.FULL));
//                    }
                }
            }
            for (ExternalTimeBatchWindow window : windows) {
                streamEventChunk.reset();
                ComplexEventChunk<StreamEvent> streamEventAfterWindow = window.process(streamEventChunk, streamEventCloner);
                if (streamEventAfterWindow.getFirst() != null) {
                    if (emitImmediate) {
                        this.sendToNextProcessor(streamEventAfterWindow);
                    } else {
                        this.sendToNextReady.put(window.getType(), streamEventAfterWindow);
                        if (this.sendToNextReady.size() > 1) {
                            streamEventAfterWindow.reset();
                            streamEventAfterWindow.next();
                            streamEventAfterWindow.remove();
                        }
                    }
                }
            }
            if (!this.emitImmediate && this.sendToNextReady.size() != 0 &&
                    this.sendToNextReady.size() == this.windows.size()) {
                this.sendToNextReady.get(ExternalTimeBatchWindow.Type.LOW).
                        add(this.sendToNextReady.get(ExternalTimeBatchWindow.Type.MIDDLE).getFirst());
                this.sendToNextReady.get(ExternalTimeBatchWindow.Type.LOW).
                        add(this.sendToNextReady.get(ExternalTimeBatchWindow.Type.HIGH).getFirst());
                if (sendFullWindow) {
                    this.sendToNextReady.get(ExternalTimeBatchWindow.Type.LOW).
                            add(this.sendToNextReady.get(ExternalTimeBatchWindow.Type.FULL).getFirst());
                }
                this.sendToNextProcessor(this.sendToNextReady.get(ExternalTimeBatchWindow.Type.LOW));
                this.sendToNextReady.clear();
            }
            streamEventChunk.clear();
        }
    }

    private void sendToNextProcessor(ComplexEventChunk<StreamEvent> complexEventChunk) {
        if (complexEventChunk != null && complexEventChunk.getFirst() != null) {
            complexEventChunk.setBatch(true);
            nextProcessor.process(complexEventChunk);
//            if (eventSynchronizer != null &&
//                    eventSynchronizer.getMissingEventStartTime() <= currentTime &&
//                    eventSynchronizer.getMissingEventEndTime() >= currentTime) {
//                windowStateStore.storeState(currentTime, currentState());
//            }
        }
    }

    private void checkFirstParameter() {
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                        .getValue();

            } else if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                        .getValue();
            } else {
                throw new SiddhiAppValidationException("Time window's parameter attribute should be either " +
                        "int or long, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("Time window should have constant parameter attribute but " +
                    "found a dynamic attribute " +
                    attributeExpressionExecutors[0].getClass().
                            getCanonicalName());
        }
    }

    private void checkSecondParameter() {
        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppValidationException("ExternalTime window's 2nd parameter timestamp should be a" +
                    " variable, but found " + attributeExpressionExecutors[1].getClass());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.LONG) {
            throw new SiddhiAppValidationException("ExternalTime window's 2nd parameter timestamp should be " +
                    "type long, but found " + attributeExpressionExecutors[1].getReturnType());
        }
        this.timestampExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[1];
    }

    private void checkFifthParameter() {
        if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                uncertainWindowRange = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[4])
                        .getValue();

            } else if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                uncertainWindowRange = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[4])
                        .getValue();
            } else {
                throw new SiddhiAppValidationException("Time window's uncertain window parameter attribute should be either " +
                        "int or long, but found " +
                        attributeExpressionExecutors[4].getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("Time window's uncertain window time should have constant " +
                    "parameter attribute but " +
                    "found a dynamic attribute " + attributeExpressionExecutors[4].getClass()
                    .getCanonicalName());
        }
    }

    private void checkThirdParameter() {
        if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
            emitImmediate = (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
        }
    }

    private void checkFourthParameter() {
        if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.BOOL) {
            sendFullWindow = (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();
        }
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> state) {

    }
}
