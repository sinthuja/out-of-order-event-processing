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
    private TreeMap<Long, StreamEventWrapper> buffer;
    private TreeMap<Long, Long> timeoutBuffer;
    private ExpressionExecutor timestampExecutor;
    private long maxDelay;

    EventSource(String name, ExpressionExecutor timestampExecutor) throws UnsupportedParameterException {
        if (name == null || name.isEmpty()) {
            throw new UnsupportedParameterException("Name of the event source is empty!");
        }
        this.name = name;
        this.lastSequenceNumber = new AtomicLong(0);
        this.timestampExecutor = timestampExecutor;
        this.averageInoderEventArrivalInterval = -1;
        this.maxDelay = -1;
        this.buffer = new TreeMap<>();
        this.timeoutBuffer = new TreeMap<>();
    }

    boolean[] isInOrder(StreamEvent event, long sequenceNumber, long currentTime) {
        boolean[] inOrderResponse = new boolean[2]; // 1st element - inOrderOrNot, 2nd Element - whether can flush bufferedEevnts
        long expectedNextSeqNumber = lastSequenceNumber.get() + 1;
        if (sequenceNumber == expectedNextSeqNumber) {
            lastSequenceNumber.incrementAndGet();
            inOrderResponse[0] = true;
            inOrderResponse[1] = buffer.size() > 0;
            calculateEventArrivalInterval(event);
        } else if (sequenceNumber > expectedNextSeqNumber) {
            buffer.put(sequenceNumber, new StreamEventWrapper(event, currentTime));
            timeoutBuffer.put(currentTime, sequenceNumber);
        } else {
            log.error("Expected Sequence number " + expectedNextSeqNumber + " is greater than received sequence number "
                    + sequenceNumber);
        }
        return inOrderResponse;
    }

    List<StreamEvent> checkAndReleaseBufferedEvents() {
        if (this.buffer.size() > 0) {
            List<StreamEvent> streamEvents = new ArrayList<>();
            List<Long> releasedSeqNumbers = new ArrayList<>();
            long expectedSeqNum = lastSequenceNumber.get() + 1;
            Iterator<Long> iterator = this.buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                if (expectedSeqNum == key) {
                    StreamEventWrapper streamEventWrapper = this.buffer.get(key);
                    releasedSeqNumbers.add(key);
                    this.timeoutBuffer.remove(streamEventWrapper.getTimestamp());
                    streamEvents.add(streamEventWrapper.getStreamEvent());
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

    private void calculateEventArrivalInterval(StreamEvent event) {
        long currentEventTimestamp = event.getTimestamp();
        if (timestampExecutor != null) {
            currentEventTimestamp = (Long) timestampExecutor.execute(event);
        }
        if (lastEventTimeStamp == 0) {
            this.lastEventTimeStamp = currentEventTimestamp;
        } else {
            if (currentEventTimestamp < this.lastEventTimeStamp) {
                log.error("Current event timestamp is greater than last known event timestamp! Current timestamp - " + currentEventTimestamp + " , lastEventTimestamp: " + lastEventTimeStamp);
                this.lastEventTimeStamp = currentEventTimestamp;
            } else {
                if (averageInoderEventArrivalInterval == -1) {
                    this.averageInoderEventArrivalInterval = currentEventTimestamp - this.lastEventTimeStamp;
                    this.lastEventTimeStamp = currentEventTimestamp;
                } else {
                    this.averageInoderEventArrivalInterval = Math.round(((currentEventTimestamp - this.lastEventTimeStamp)
                            + this.averageInoderEventArrivalInterval) * 0.5);
                    this.lastEventTimeStamp = currentEventTimestamp;
                }
            }
        }
    }

    private List<StreamEvent> releaseBufferedEvents(long sequenceNumber) {
        if (this.buffer.size() > 0) {
            List<StreamEvent> streamEvents = new ArrayList<>();
            List<Long> releasedSeqNumbers = new ArrayList<>();
            Iterator<Long> iterator = this.buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                if (key <= sequenceNumber) {
                    StreamEventWrapper streamEventWrapper = this.buffer.get(key);
                    releasedSeqNumbers.add(key);
                    this.timeoutBuffer.remove(streamEventWrapper.getTimestamp());
                    streamEvents.add(streamEventWrapper.getStreamEvent());
                } else {
                    break;
                }
            }
            long lastReleasedSequenceNum = lastSequenceNumber.getAndSet(sequenceNumber);
            for (Long seqNum : releasedSeqNumbers) {
                StreamEventWrapper streamEventWrapper = this.buffer.remove(seqNum);
                if (seqNum == lastReleasedSequenceNum + 1) {
                    calculateEventArrivalInterval(streamEventWrapper.getStreamEvent());
                } else {
                    this.lastEventTimeStamp = 0;
                    lastReleasedSequenceNum = seqNum;
                }
            }
            return streamEvents;
        }
        return null;
    }

    List<StreamEvent> releaseTimeoutBufferedEvents(long currentTimestamp) {
        Iterator<Long> timestamp = this.timeoutBuffer.keySet().iterator();
        List<Long> timeStampToRemove = new ArrayList<>();
        List<StreamEvent> releasedEvents = new ArrayList<>();
        while (timestamp.hasNext()) {
            long eventTimestamp = timestamp.next();
            if (eventTimestamp <= currentTimestamp) {
                timeStampToRemove.add(eventTimestamp);
            } else {
                break;
            }
        }
        for (Long timeStamp : timeStampToRemove) {
            Long sequenceNum = this.timeoutBuffer.remove(timeStamp);
            if (sequenceNum != null) {
                List<StreamEvent> releaseBufferedEvents = releaseBufferedEvents(sequenceNum);
                if (releaseBufferedEvents != null) {
                    releasedEvents.addAll(releaseBufferedEvents);
                }
            }
        }
        return releasedEvents;
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
        log.info("Inorder event arrival interval : " + averageInoderEventArrivalInterval);
        if (this.averageInoderEventArrivalInterval == -1 || this.averageInoderEventArrivalInterval == 0) {
            return Long.MAX_VALUE;
        }
        return averageInoderEventArrivalInterval;
    }

    long getMaxDelay(){
        if (this.maxDelay == -1 || this.maxDelay == 0){
            return Long.MAX_VALUE;
        }
        return maxDelay;
    }
}
