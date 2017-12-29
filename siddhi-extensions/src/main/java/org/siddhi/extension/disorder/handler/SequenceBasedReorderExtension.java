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

@Extension(
        name = "reorder",
        namespace = "sequenceBased",
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

    private HashMap<String, EventSource> sourceHashMap = new HashMap<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> orderedEventChunk = new ComplexEventChunk<>(null, null, complexEventChunk.isBatch());
        synchronized (this) {
            StreamEvent event;
            String sourceId;
            Long sequenceNumber;
            while (complexEventChunk.hasNext()) {
                try {
                    event = complexEventChunk.next();
                    complexEventChunk.remove();
                    if (event.getType().equals(ComplexEvent.Type.CURRENT)) {
                        sourceId = (String) sourceIdExecutor.execute(event);
                        sequenceNumber = (Long) sequenceNumberExecutor.execute(event);
                        EventSource source = sourceHashMap.get(sourceId);
                        if (source == null) {
                            source = new EventSource(sourceId, timestampExecutor);
                            sourceHashMap.put(sourceId, source);
                        }
                        long expiryTimestamp = System.currentTimeMillis() +
                                Math.min(userDefinedTimeout, Math.max(source.getAverageInoderEventArrivalInterval(),
                                source.getBufferedEventsDelay()));
                        boolean[] response = source.isInOrder(event, sequenceNumber, expiryTimestamp);
                        if (response[0]) {
                            orderedEventChunk.add(event);
                            if (response[1]) {
                                addToEventChunk(orderedEventChunk, source.checkAndReleaseBufferedEvents());
                            }
                        } else {
                            scheduler.notifyAt(expiryTimestamp);
                        }
                    } else {
                        long currentTimestamp = System.currentTimeMillis();
                        Iterator<String> sources = sourceHashMap.keySet().iterator();
                        while (sources.hasNext()) {
                            EventSource source = sourceHashMap.get(sources.next());
                            addToEventChunk(orderedEventChunk, source.releaseTimeoutBufferedEvents(currentTimestamp));
                        }
                    }
                } catch (UnsupportedParameterException e) {
                    log.error("The event is not unsupported value. ", e);
                }

            }
        }
        processor.process(orderedEventChunk);
    }

    private void addToEventChunk(ComplexEventChunk<StreamEvent> streamEventComplexEventChunk, List<StreamEventWrapper> events) {
        for (StreamEventWrapper event : events) {
            streamEventComplexEventChunk.add(event.getStreamEvent());
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
//        List<Attribute> attributes = new ArrayList<>();
//        attributes.add(new Attribute(CONFIDENT_LEVEL, Attribute.Type.DOUBLE));
        if (attributeExpressionLength > 4) {
            throw new SiddhiAppCreationException("Maximum allowed expressions to sequence based reorder extension is 4! But found - " + attributeExpressionLength);
        }

        if (attributeExpressionLength == 1) {
            throw new SiddhiAppCreationException("Minimum number of attributes is 2, those are Source ID and Sequence number fields of the stream. " +
                    "But only found 1 attribute.");
        } else if (attributeExpressionLength == 2) {
            if (expressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                sourceIdExecutor = expressionExecutors[0];
            } else {
                throw new SiddhiAppCreationException("Expected a field with String return type for the Source ID field," +
                        "but found a field with return type - " + expressionExecutors[0].getReturnType());
            }
            if (expressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                sequenceNumberExecutor = expressionExecutors[1];
            } else {
                throw new SiddhiAppCreationException("Expected a field with Long return type for the Sequence Number field," +
                        "but found a field with return type - " + expressionExecutors[1].getReturnType());
            }
        } else if (attributeExpressionLength == 3) {
            if (expressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                sourceIdExecutor = expressionExecutors[0];
            } else {
                throw new SiddhiAppCreationException("Expected a field with String return type for the Source ID field," +
                        "but found a field with return type - " + expressionExecutors[0].getReturnType());
            }
            if (expressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                sequenceNumberExecutor = expressionExecutors[1];
            } else {
                throw new SiddhiAppCreationException("Expected a field with Long return type for the Sequence Number field," +
                        "but found a field with return type - " + expressionExecutors[1].getReturnType());
            }
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
        } else {
            if (expressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                sourceIdExecutor = expressionExecutors[0];
            } else {
                throw new SiddhiAppCreationException("Expected a field with String return type for the Source ID field, " +
                        "but found a field with return type - " + expressionExecutors[0].getReturnType());
            }

            if (expressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                sequenceNumberExecutor = expressionExecutors[1];
            } else {
                throw new SiddhiAppCreationException("Expected a field with Long return type for the Sequence Number field," +
                        "but found a field with return type - " + expressionExecutors[1].getReturnType());
            }

            if (expressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                if (!(expressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                    timestampExecutor = expressionExecutors[2];
                } else {
                    throw new SiddhiAppCreationException("Expected a field with Long return type as timestamp field, but found a constant");
                }
            } else {
                throw new SiddhiAppCreationException("Expected a field with Long return type but found a field with - "
                        + expressionExecutors[2].getReturnType());
            }
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
        return new ArrayList<>();
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
