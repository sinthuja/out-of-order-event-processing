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
import org.wso2.siddhi.core.util.Scheduler;

import java.util.Map;

public class TimeBatchWindow {

    private long timeInMilliSeconds;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>(false);
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ComplexEventChunk<StreamEvent> nextExpiredEventChunk = null;
    private Scheduler scheduler;
    private boolean outputExpectsExpiredEvents;
    private StreamEvent resetEvent = null;
    private long nextEmitTime;
    private Type type;
    private ComplexEventPopulater complexEventPopulater;
    private int lastIndexOfCurrentEventToExpire = -1;
    private long overLappingTime;


    TimeBatchWindow(long nextEmitTime, long overLappingTime, long timeInMilliSeconds,
                    boolean outputExpectsExpiredEvents,
                    ComplexEventPopulater complexEventPopulater,
                    Scheduler scheduler, Type type) {
        this.nextEmitTime = nextEmitTime;
        this.timeInMilliSeconds = timeInMilliSeconds;
        this.complexEventPopulater = complexEventPopulater;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.overLappingTime = overLappingTime;
        if (this.outputExpectsExpiredEvents) {
            this.expiredEventChunk = new ComplexEventChunk<>(false);
            if (this.overLappingTime > 0) {
                this.nextExpiredEventChunk = new ComplexEventChunk<>(false);
            }
        }
        this.scheduler = scheduler;
        this.scheduler.notifyAt(this.nextEmitTime);
        this.type = type;
    }

    public ComplexEventChunk<StreamEvent> process(ComplexEventChunk<StreamEvent> streamEventChunk,
                                                  StreamEventCloner streamEventCloner, long currentTime) {
        synchronized (this) {
            boolean isAddToExpire = true;
            if (this.nextExpiredEventChunk != null && currentTime > this.nextEmitTime - this.overLappingTime) {
                isAddToExpire = false;
            }

            boolean sendEvents;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }

            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                complexEventPopulater.populateComplexEvent(clonedStreamEvent, new Object[]{this.type.toString()});
                currentEventChunk.add(clonedStreamEvent);
                if (this.nextExpiredEventChunk != null && isAddToExpire) {
                    lastIndexOfCurrentEventToExpire++;
                }
            }
            if (sendEvents) {
                ComplexEventChunk<StreamEvent> sendingStreamEventChunk
                        = new ComplexEventChunk<>(false);
                if (outputExpectsExpiredEvents) {
                    if (expiredEventChunk.getFirst() != null) {
                        while (expiredEventChunk.hasNext()) {
                            StreamEvent expiredEvent = expiredEventChunk.next();
                            expiredEvent.setTimestamp(currentTime);
                        }
                        sendingStreamEventChunk.add(expiredEventChunk.getFirst());
                    }
                }
                if (expiredEventChunk != null) {
                    expiredEventChunk.clear();
                }

                if (currentEventChunk.getFirst() != null) {

                    // add reset event in front of current events
                    sendingStreamEventChunk.add(resetEvent);
                    resetEvent = null;

                    if (expiredEventChunk != null) {
                        currentEventChunk.reset();
                        if (this.nextExpiredEventChunk != null && this.nextExpiredEventChunk.getFirst() != null) {
                            this.expiredEventChunk.add(this.nextExpiredEventChunk.getFirst());
                            this.nextExpiredEventChunk.clear();
                        }
                        int currentChunkIndex = -1;
                        while (currentEventChunk.hasNext()) {
                            StreamEvent currentEvent = currentEventChunk.next();
                            StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                            toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                            if (this.nextExpiredEventChunk != null) {
                                currentChunkIndex++;
                                if (currentChunkIndex <= lastIndexOfCurrentEventToExpire) {
                                    expiredEventChunk.add(toExpireEvent);
                                } else {
                                    nextExpiredEventChunk.add(toExpireEvent);
                                }
                            } else {
                                this.expiredEventChunk.add(toExpireEvent);
                            }
                        }
                    }

                    resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                    resetEvent.setType(ComplexEvent.Type.RESET);
                    sendingStreamEventChunk.add(currentEventChunk.getFirst());
                }
                currentEventChunk.clear();
                lastIndexOfCurrentEventToExpire = -1;
                return sendingStreamEventChunk;
            }
            return null;
        }
    }

    public void storeCurrentState(Map<String, Object> state) {
        state.put("CurrentEventChunk:" + this.type, currentEventChunk.getFirst());
        state.put("ExpiredEventChunk:" + this.type, expiredEventChunk != null ? expiredEventChunk.getFirst() : null);
        state.put("NextExpiredEventChunk:" + this.type, nextExpiredEventChunk != null ? nextExpiredEventChunk.getFirst() : null);
        state.put("ResetEvent:" + this.type, resetEvent);
    }

    public void retrieveState(Map<String, Object> state) {
        if (expiredEventChunk != null) {
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state.get("ExpiredEventChunk:" + this.type));
        }
        if (nextExpiredEventChunk != null) {
            nextExpiredEventChunk.clear();
            nextExpiredEventChunk.add((StreamEvent) state.get("NextExpiredEventChunk:" + this.type));
        }
        currentEventChunk.clear();
        currentEventChunk.add((StreamEvent) state.get("CurrentEventChunk:" + this.type));
        resetEvent = (StreamEvent) state.get("ResetEvent:" + this.type);
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        LOW, MIDDLE, HIGH, FULL
    }
}
