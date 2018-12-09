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
package org.siddhi.extension.disorder.handler;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;


public class ExternalTimeBatchWindow {

    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>(false);
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private StreamEvent resetEvent = null;
    private VariableExpressionExecutor timestampExpressionExecutor;
    private long timeToKeep;
    private long endTime = -1;
    private long startTime = 0;
    private long lastCurrentEventTime;
    private boolean outputExpectsExpiredEvents;
    private ComplexEventPopulater complexEventPopulater;
    private Type type;

    public ExternalTimeBatchWindow(VariableExpressionExecutor timestampExpressionExecutor, long windowTime,
                                   boolean outputExpectsExpiredEvents, long startTime,
                                   ComplexEventPopulater complexEventPopulater, Type type) {
        this.timestampExpressionExecutor = timestampExpressionExecutor;
        this.timeToKeep = windowTime;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.startTime = startTime;
        this.endTime = startTime + timeToKeep;
        if (this.outputExpectsExpiredEvents) {
            expiredEventChunk = new ComplexEventChunk<>(false);
        }
        this.complexEventPopulater = complexEventPopulater;
        this.type = type;
    }

    public ComplexEventChunk<StreamEvent> process(ComplexEventChunk<StreamEvent> streamEventChunk,
                                                  StreamEventCloner streamEventCloner) {
        // event incoming trigger process. No events means no action
        if (streamEventChunk.getFirst() == null) {
            return null;
        }
        ComplexEventChunk<StreamEvent> newEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            StreamEvent nextStreamEvent = streamEventChunk.getFirst();
            while (nextStreamEvent != null) {
                StreamEvent currStreamEvent = nextStreamEvent;
                nextStreamEvent = nextStreamEvent.getNext();

                if (currStreamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }

                long currentEventTime = (Long) timestampExpressionExecutor.execute(currStreamEvent);
                if (currentEventTime < startTime){
                    continue;
                }
                if (lastCurrentEventTime < currentEventTime) {
                    lastCurrentEventTime = currentEventTime;
                }

                if (currentEventTime < endTime) {
                    cloneAppend(streamEventCloner, currStreamEvent);
                } else {
                    flushToOutputChunk(streamEventCloner, newEventChunk, lastCurrentEventTime);
                    // update timestamp, call next processor
                    endTime = findEndTime(lastCurrentEventTime, startTime, timeToKeep);
                    cloneAppend(streamEventCloner, currStreamEvent);
                }
            }
        }
        return newEventChunk;
    }

    private void flushToOutputChunk(StreamEventCloner streamEventCloner, ComplexEventChunk<StreamEvent> newEventChunk,
                                    long currentTime) {
        if (outputExpectsExpiredEvents) {
            if (expiredEventChunk.getFirst() != null) {
                // mark the timestamp for the expiredType event
                expiredEventChunk.reset();
                while (expiredEventChunk.hasNext()) {
                    StreamEvent expiredEvent = expiredEventChunk.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                // add expired event to newEventChunk.
                newEventChunk.add(expiredEventChunk.getFirst());
            }
        }
        if (expiredEventChunk != null) {
            expiredEventChunk.clear();
        }

        if (currentEventChunk.getFirst() != null) {

            // add reset event in front of current events
            resetEvent.setTimestamp(currentTime);
            //TODO: commented reset
            newEventChunk.add(resetEvent);
            resetEvent = null;

            // move to expired events
            if (outputExpectsExpiredEvents) {
                currentEventChunk.reset();
                while (currentEventChunk.hasNext()) {
                    StreamEvent currentEvent = currentEventChunk.next();
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    expiredEventChunk.add(toExpireEvent);
                }
            }

            // add current event chunk to next processor
            newEventChunk.add(currentEventChunk.getFirst());
        }
        currentEventChunk.clear();
    }

    private long findEndTime(long currentTime, long startTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }

    private void cloneAppend(StreamEventCloner streamEventCloner, StreamEvent currStreamEvent) {
        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
        this.complexEventPopulater.populateComplexEvent(clonedStreamEvent, new Object[]{this.type.toString()});
        currentEventChunk.add(clonedStreamEvent);
        if (resetEvent == null) {
            resetEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
            resetEvent.setType(ComplexEvent.Type.RESET);
        }
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        LOW, MIDDLE, HIGH, FULL
    }
}
