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
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class EventSource {

    private static Logger log = Logger.getLogger(EventSource.class);

    private String name;
    private AtomicLong lastSequenceNumber;
    private long averageInoderEventArrivalInterval;
    private long lastEventTimeStamp;
    private TreeMap<Long, Long> timeoutReleasedEvents;
    private TreeMap<Long, StreamEventWrapper> buffer;
    private TreeMap<Long, List<Long>> timeoutBuffer;
    private ExpressionExecutor timestampExecutor;

    //This provides the delay that it have been or could have been in the buffer, so we can calculate the time that it
    // can be held in the buffer for future adjustment.
    private long bufferedEventsDelay;

    private static final double DELAY_SMOOTHING_FACTOR = 0.5;
    private static final double EVENT_ARRIVAL_INTERVAL_SMOOTHING_FACTOR = 0.5;

    EventSource(String name, ExpressionExecutor timestampExecutor) throws UnsupportedParameterException {
        if (name == null || name.isEmpty()) {
            throw new UnsupportedParameterException("Name of the event source is empty!");
        }
        this.name = name;
        this.lastSequenceNumber = new AtomicLong(0);
        this.timestampExecutor = timestampExecutor;
        this.averageInoderEventArrivalInterval = -1;
        this.bufferedEventsDelay = -1;
        this.buffer = new TreeMap<>();
        this.timeoutBuffer = new TreeMap<>();
        this.timeoutReleasedEvents = new TreeMap<>();
    }

    boolean[] isInOrder(StreamEvent event, long sequenceNumber, long expiryTimestamp) throws SequenceNumberAlreadyPassedException {
        boolean[] inOrderResponse = new boolean[2]; // 1st element - inOrderOrNot, 2nd Element - whether can flush bufferedEevnts
        long expectedNextSeqNumber = lastSequenceNumber.get() + 1;
        if (sequenceNumber == expectedNextSeqNumber) {
            lastSequenceNumber.incrementAndGet();
            inOrderResponse[0] = true;
            inOrderResponse[1] = buffer.size() > 0;
            calculateEventArrivalInterval(event);
        } else if (sequenceNumber > expectedNextSeqNumber) {
            buffer.put(sequenceNumber, new StreamEventWrapper(event, expiryTimestamp, System.currentTimeMillis(), sequenceNumber));
            List<Long> seqNumList = timeoutBuffer.get(expiryTimestamp);
            if (seqNumList == null) {
                seqNumList = new ArrayList<>();
                seqNumList.add(sequenceNumber);
                timeoutBuffer.put(expiryTimestamp, seqNumList);
            } else {
                seqNumList.add(sequenceNumber);
            }
        } else {
            String msg = "Expected Sequence number " + expectedNextSeqNumber + " is greater than received sequence number "
                    + sequenceNumber;
//            log.error("Expected Sequence number " + expectedNextSeqNumber + " is greater than received sequence number "
//                    + sequenceNumber);
            long currentTime = System.currentTimeMillis();
            if (this.timeoutReleasedEvents.size() > 0) {
                Iterator<Long> timeoutSequenceNumIterator = this.timeoutReleasedEvents.keySet().iterator();
                while (timeoutSequenceNumIterator.hasNext()) {
                    Long key = timeoutSequenceNumIterator.next();
                    if (key < sequenceNumber) {
                        calculateBufferedEventsDelay(currentTime - timeoutReleasedEvents.get(key));
                    } else {
                        break;
                    }
                }
            }
            throw new SequenceNumberAlreadyPassedException(msg);
        }
        return inOrderResponse;
    }

    List<StreamEventWrapper> checkAndReleaseBufferedEvents() {
        if (this.buffer.size() > 0) {
            List<StreamEventWrapper> streamEvents = new ArrayList<>();
            List<Long> releasedSeqNumbers = new ArrayList<>();
            long expectedSeqNum = lastSequenceNumber.get() + 1;
            Iterator<Long> iterator = this.buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                if (expectedSeqNum == key) {
                    StreamEventWrapper streamEventWrapper = this.buffer.get(key);
                    long delay = System.currentTimeMillis() - streamEventWrapper.getArrivalTimestamp();
                    calculateBufferedEventsDelay(delay); // finding the time that was required to be buffered in due to the late arrival
                    releasedSeqNumbers.add(key);
                    removeFromTimeoutBuffer(streamEventWrapper.getExpiryTimestamp(), key);
                    setEventTime(streamEventWrapper);
                    streamEvents.add(streamEventWrapper);
                    expectedSeqNum = lastSequenceNumber.incrementAndGet() + 1;
                } else if (expectedSeqNum < key) {
                    break;
                }
            }
            for (Long seqNum : releasedSeqNumbers) {
                calculateEventArrivalInterval(this.buffer.remove(seqNum).getStreamEvent());
            }
            return streamEvents;
        }
        return null;
    }

    private void setEventTime(StreamEventWrapper streamEventWrapper) {
        long currentEventTimestamp = streamEventWrapper.getStreamEvent().getTimestamp();
        if (timestampExecutor != null) {
            currentEventTimestamp = (Long) timestampExecutor.execute(streamEventWrapper.getStreamEvent());
        }
        streamEventWrapper.setEventTime(currentEventTimestamp);
    }

    private void removeFromTimeoutBuffer(long expiryTimestamp, long sequenceNum) {
        List<Long> seqNumList = this.timeoutBuffer.get(expiryTimestamp);
        if (seqNumList != null) {
            seqNumList.remove(sequenceNum);
            if (seqNumList.isEmpty()) {
                this.timeoutBuffer.remove(expiryTimestamp);
            }
        }
    }


    private void calculateBufferedEventsDelay(long delay) {
        if (this.bufferedEventsDelay == -1) {
            this.bufferedEventsDelay = delay;
        } else {
            this.bufferedEventsDelay = Math.round((DELAY_SMOOTHING_FACTOR * this.bufferedEventsDelay)
                    + ((1 - DELAY_SMOOTHING_FACTOR) * delay));
        }
//        log.info("Disorder event delay : " + this.bufferedEventsDelay+ ", delay is - "+ delay);
    }

    private void calculateEventArrivalInterval(StreamEvent event) {
        long currentEventTimestamp = event.getTimestamp();
        if (timestampExecutor != null) {
            currentEventTimestamp = (Long) timestampExecutor.execute(event);
        }
        if (lastEventTimeStamp == 0) {
            this.lastEventTimeStamp = currentEventTimestamp;
        } else {
            if (currentEventTimestamp < this.lastEventTimeStamp) {
                this.lastEventTimeStamp = currentEventTimestamp;
            } else {
                if (averageInoderEventArrivalInterval == -1) {
                    this.averageInoderEventArrivalInterval = currentEventTimestamp - this.lastEventTimeStamp;
                    this.lastEventTimeStamp = currentEventTimestamp;
                } else {
                    this.averageInoderEventArrivalInterval =
                            Math.round(((this.averageInoderEventArrivalInterval * EVENT_ARRIVAL_INTERVAL_SMOOTHING_FACTOR)
                                    + (currentEventTimestamp - this.lastEventTimeStamp) * (1 - EVENT_ARRIVAL_INTERVAL_SMOOTHING_FACTOR)));
                    this.lastEventTimeStamp = currentEventTimestamp;
                }
            }
        }
//        log.info("In order event arrival interval : " + this.averageInoderEventArrivalInterval);
    }

    private List<StreamEventWrapper> releaseBufferedEvents(long sequenceNumber) {
        if (this.buffer.size() > 0) {
            List<StreamEventWrapper> streamEvents = new ArrayList<>();
            Iterator<Long> iterator = this.buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                if (key <= sequenceNumber) {
                    StreamEventWrapper streamEventWrapper = this.buffer.get(key);
                    removeFromTimeoutBuffer(streamEventWrapper.getExpiryTimestamp(), key);
                    streamEvents.add(streamEventWrapper);
                } else {
                    break;
                }
            }
            long lastReleasedSequenceNum = lastSequenceNumber.getAndSet(sequenceNumber);
            for (StreamEventWrapper eventWrapper : streamEvents) {
                StreamEventWrapper streamEventWrapper = this.buffer.remove(eventWrapper.getSequenceNum());
                if (eventWrapper.getSequenceNum() == lastReleasedSequenceNum + 1) {
                    calculateEventArrivalInterval(streamEventWrapper.getStreamEvent());
                } else {
                    this.lastEventTimeStamp = 0;
                    lastReleasedSequenceNum = eventWrapper.getSequenceNum();
                }
            }
            return streamEvents;
        }
        return null;
    }

    List<StreamEventWrapper> releaseTimeoutBufferedEvents(long currentTimestamp) {
        Iterator<Long> timestamp = this.timeoutBuffer.keySet().iterator();
        List<Long> timeStampToRemove = new ArrayList<>();
        List<StreamEventWrapper> releasedEvents = new ArrayList<>();
        while (timestamp.hasNext()) {
            long eventTimestamp = timestamp.next();
            if (eventTimestamp <= currentTimestamp) {
                timeStampToRemove.add(eventTimestamp);
            } else {
                break;
            }
        }
        for (Long timeStamp : timeStampToRemove) {
            List<Long> sequenceNumList = this.timeoutBuffer.remove(timeStamp);
            if (sequenceNumList != null) {
                if (sequenceNumList.size() > 1) {
                    Collections.sort(sequenceNumList);
                }
                for (Long sequenceNum : sequenceNumList) {
                    List<StreamEventWrapper> releaseBufferedEvents = releaseBufferedEvents(sequenceNum);
                    if (releaseBufferedEvents != null) {
                        releasedEvents.addAll(releaseBufferedEvents);
                    }
                }
            }
        }
        this.addTimeoutReleasedEventsCache(releasedEvents);
        return releasedEvents;
    }

    private void addTimeoutReleasedEventsCache(List<StreamEventWrapper> eventList) {
        for (StreamEventWrapper streamEventWrapper : eventList) {
            setEventTime(streamEventWrapper); // To order in the multiple sources.
            this.timeoutReleasedEvents.put(streamEventWrapper.getSequenceNum(), streamEventWrapper.getArrivalTimestamp());
        }
    }

    public boolean equals(Object object) {
        if (object instanceof EventSource) {
            EventSource eventSource = (EventSource) object;
            if (eventSource.name.contentEquals(name)) {
                return true;
            }
        }
        return false;
    }

    long getAverageInoderEventArrivalInterval() {
        if (this.averageInoderEventArrivalInterval == -1 || this.averageInoderEventArrivalInterval == 0) {
            return Long.MAX_VALUE;
        }
        return averageInoderEventArrivalInterval;
    }

    long getBufferedEventsDelay() {
        if (this.bufferedEventsDelay == -1 || this.bufferedEventsDelay == 0) {
            return Long.MAX_VALUE;
        }
        return bufferedEventsDelay;
    }

    String getName() {
        return this.name;
    }
}
