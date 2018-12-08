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
package org.siddhi.extension.disorder.handler;

import org.apache.log4j.Logger;
import org.siddhi.extension.disorder.handler.exception.SequenceNumberAlreadyPassedException;
import org.siddhi.extension.disorder.handler.exception.UnsupportedParameterException;
import org.siddhi.extension.disorder.handler.multi.source.MultiSourceEventSynchronizer;
import org.siddhi.extension.disorder.handler.multi.source.MultiSourceEventSynchronizerManager;
import org.siddhi.extension.disorder.handler.synchronization.TCPNettySyncServer;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Extension(
        name = "sequence",
        namespace = "reorder",
        description = "This alogortithm orders the events based on the sequence number provided in " +
                "the particular field of event",
        examples = @Example(description = "TBD"
                , syntax = "from inputStream#reorder:sequence(sourceId, sequenceNum, timestamp, 100L) \" +\n" +
                "                \"select eventtt, price, \"\n" +
                "                + \"volume \" +\n" +
                "                \"insert into outputStream;")
)
public class SequenceBasedReorderExtension extends StreamProcessor implements SchedulingProcessor {
    private static Logger log = Logger.getLogger(SequenceBasedReorderExtension.class);

    private static final Long DEFAULT_TIMEOUT_MILLI_SEC = 20L;

    private ExpressionExecutor sourceIdExecutor;
    private ExpressionExecutor sequenceNumberExecutor;
    private ExpressionExecutor timestampExecutor;
    private long userDefinedTimeout = DEFAULT_TIMEOUT_MILLI_SEC;
    private Scheduler scheduler;
    private MultiSourceEventSynchronizer synchronizer;
    private boolean dropIfSeqNumAlreadyPassed = true;
    private String streamId;

    private ConcurrentHashMap<String, EventSource> sourceHashMap = new ConcurrentHashMap<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            if (this.synchronizer == null) {
                this.synchronizer = MultiSourceEventSynchronizerManager.getInstance().
                        getMultiSourceEventSynchronizer(this.streamId, processor, userDefinedTimeout);
            }
            StreamEvent event;
            String sourceId;
            Long sequenceNumber;
            while (complexEventChunk.hasNext()) {
                try {
                    event = complexEventChunk.next();
                    complexEventChunk.remove();
                    if (event.getType().equals(ComplexEvent.Type.CURRENT)) {
                        event.setTimestamp(System.currentTimeMillis());
                        sourceId = (String) sourceIdExecutor.execute(event);
                        sequenceNumber = (Long) sequenceNumberExecutor.execute(event);
                        EventSource source = sourceHashMap.get(sourceId);
                        if (source == null) {
                            source = new EventSource(sourceId, timestampExecutor);
                            sourceHashMap.put(sourceId, source);
                            this.synchronizer.putEventSource(sourceId, source);
                        }
                        long expiryTimestamp = System.currentTimeMillis() +
                                Utils.getTimeout(userDefinedTimeout, source);
                        try {
                            boolean[] response = source.isInOrder(event, sequenceNumber, expiryTimestamp);
                            if (response[0]) {
                                this.synchronizer.putEvent(sourceId, event, getEventTime(event),
                                        sequenceNumber, complexEventPopulater);
                                if (response[1]) {
                                    this.synchronizer.putEvent(sourceId, source.checkAndReleaseBufferedEvents(),
                                            complexEventPopulater);
                                }
                            } else {
                                scheduler.notifyAt(expiryTimestamp);
                            }
                        } catch (SequenceNumberAlreadyPassedException ex) {
                            if (!this.dropIfSeqNumAlreadyPassed) {
                                this.synchronizer.putEvent(sourceId, event, getEventTime(event), sequenceNumber,
                                        complexEventPopulater);
                            }
                        }
                    } else {
                        long currentTimestamp = System.currentTimeMillis();
                        for (String s : sourceHashMap.keySet()) {
                            EventSource source = sourceHashMap.get(s);
                            List<StreamEventWrapper> eventWrapper = source.releaseTimeoutBufferedEvents(currentTimestamp);
                            if (eventWrapper != null && !eventWrapper.isEmpty()) {
                                this.synchronizer.putEvent(source.getName(), eventWrapper, complexEventPopulater);
                            }
                        }
                        //Events released from timeout, without meeting the next expected sequence number,
                        // therefore enabled MissingEvent property therefore the windows will be stored.
//                        if (!this.dropIfSeqNumAlreadyPassed) {
//                        }
                    }
                } catch (UnsupportedParameterException e) {
                    log.error("The event is not unsupported value. ", e);
                }
            }
        }
    }


    private long getEventTime(StreamEvent event) {
        long currentEventTimestamp = event.getTimestamp();
        if (timestampExecutor != null) {
            currentEventTimestamp = (Long) timestampExecutor.execute(event);
        }
        return currentEventTimestamp;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionLength > 5) {
            throw new SiddhiAppCreationException("Maximum allowed expressions to sequence based reorder extension is 5!" +
                    " But found - " + attributeExpressionLength);
        } else if (attributeExpressionLength <= 1) {
            throw new SiddhiAppCreationException("Minimum number of attributes is 2, those are Source ID and Sequence " +
                    "number fields of the stream. " +
                    "But only found 1 attribute.");
        } else if (attributeExpressionLength == 2) {
            checkFirstParameter(expressionExecutors);
            checkSecondParameter(expressionExecutors);
        } else if (attributeExpressionLength == 3) {
            checkFirstParameter(expressionExecutors);
            checkSecondParameter(expressionExecutors);
            checkThirdParameter(expressionExecutors);
        } else if (attributeExpressionLength == 4) {
            checkFirstParameter(expressionExecutors);
            checkSecondParameter(expressionExecutors);
            checkThirdParameter(expressionExecutors);
            checkFourthParameter(expressionExecutors);
        } else {
            checkFirstParameter(expressionExecutors);
            checkSecondParameter(expressionExecutors);
            checkThirdParameter(expressionExecutors);
            checkFourthParameter(expressionExecutors);
            checkFifthParameter(expressionExecutors);
        }
        this.streamId = abstractDefinition.getId();
        List<Attribute> eventAttribute = new ArrayList<>();
        eventAttribute.add(new Attribute(Constants.RELATIVE_TIMETSAMP_ATTRIBUTE, Attribute.Type.LONG));
        return eventAttribute;
    }

    private void checkFirstParameter(ExpressionExecutor[] expressionExecutors) {
        if (expressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
            sourceIdExecutor = expressionExecutors[0];
        } else {
            throw new SiddhiAppCreationException("Expected a field with String return type for the Source ID field," +
                    "but found a field with return type - " + expressionExecutors[0].getReturnType());
        }
    }

    private void checkSecondParameter(ExpressionExecutor[] expressionExecutors) {
        if (expressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
            sequenceNumberExecutor = expressionExecutors[1];
        } else {
            throw new SiddhiAppCreationException("Expected a field with Long return type for the Sequence Number field," +
                    "but found a field with return type - " + expressionExecutors[1].getReturnType());
        }
    }

    private void checkThirdParameter(ExpressionExecutor[] expressionExecutors) {
        if (expressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
            if (expressionExecutors[2] instanceof ConstantExpressionExecutor) {
                userDefinedTimeout = (Long) expressionExecutors[2].execute(null);
            } else {
                timestampExecutor = expressionExecutors[2];
            }
        } else {
            throw new SiddhiAppCreationException("Expected a field with Long return type as timestamp field " +
                    "or Long constant for the userDefinedTimeout field, "
                    + "but found a field with return type - " + expressionExecutors[2].getReturnType());
        }
    }

    private void checkFourthParameter(ExpressionExecutor[] expressionExecutors) {
        if (expressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
            if (expressionExecutors[3] instanceof ConstantExpressionExecutor) {
                userDefinedTimeout = (Long) expressionExecutors[3].execute(null);
            } else {
                throw new SiddhiAppCreationException("Expected Long constant for the userDefinedTimeout field, but non constant field found");
            }
        } else {
            throw new SiddhiAppCreationException("Expected a field with Long return type but found a field with - "
                    + expressionExecutors[3].getReturnType());
        }
    }

    private void checkFifthParameter(ExpressionExecutor[] expressionExecutors) {
        if (expressionExecutors[4].getReturnType() == Attribute.Type.BOOL) {
            if (expressionExecutors[4] instanceof ConstantExpressionExecutor) {
                this.dropIfSeqNumAlreadyPassed = (Boolean) expressionExecutors[4].execute(null);
            } else {
                throw new SiddhiAppCreationException("Expected boolean constant for the " +
                        "field - DropIfSequenceNumberAlreadyPassed, but non constant field found");
            }
        } else {
            throw new SiddhiAppCreationException("Expected a field with Boolean return type for the " +
                    "DropIfSequenceNumberAlreadyPassed field," +
                    "but found a field with return type - " + expressionExecutors[1].getReturnType());
        }
    }


    @Override
    public void start() {
        TCPNettySyncServer.getInstance().startIfNotAlready();
    }

    @Override
    public void stop() {
        TCPNettySyncServer.getInstance().shutdownGracefullyIfNotUsed();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
}
