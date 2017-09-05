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
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class EventSource {

    private static Logger log = Logger.getLogger(EventSource.class);

    private String name;
    private AtomicLong lastSequenceNumber;
    private TreeMap<Long, StreamEvent> buffer;

    public EventSource(String name) throws UnsupportedParameterException {
        if (name == null || name.isEmpty()) {
            throw new UnsupportedParameterException("Name of the event source is empty!");
        }
        this.name = name;
        this.lastSequenceNumber = new AtomicLong(0);
        this.buffer = new TreeMap<>();
    }

    public String getName() {
        return name;
    }

    public boolean[] isInOrder(StreamEvent event, long sequenceNumber) {
        boolean[] inOrderResponse = new boolean[2]; // 1st element - inOrderOrNot, 2nd Element - whether can flush bufferedEevnts
        long expectedNextSeqNumber = lastSequenceNumber.get() + 1;
        if (sequenceNumber == expectedNextSeqNumber) {
            lastSequenceNumber.incrementAndGet();
            inOrderResponse[0] = true;
            inOrderResponse[1] = buffer.size() > 0;
        } else if (sequenceNumber > expectedNextSeqNumber) {
            buffer.put(sequenceNumber, event);
        } else {
            log.error("Expected Sequence number " + expectedNextSeqNumber + " is greater than received sequence number "
                    + sequenceNumber);
        }
        return inOrderResponse;
    }

    public List<StreamEvent> checkAndReleaseBufferedEvents() {
        if (this.buffer.size() > 0) {
            List<StreamEvent> streamEvents = new ArrayList<>();
            List<Long> releasedSeqNumbers = new ArrayList<>();
            long expectedSeqNum = lastSequenceNumber.get() + 1;
            Iterator<Long> iterator = this.buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                if (expectedSeqNum == key) {
                    StreamEvent streamEvent = this.buffer.get(key);
                    releasedSeqNumbers.add(key);
                    streamEvents.add(streamEvent);
                    log.info("release: " + streamEvent);
                    expectedSeqNum = lastSequenceNumber.incrementAndGet() + 1;
                } else if (expectedSeqNum < key) {
                    break;
                }
            }
            for (Long seqNum: releasedSeqNumbers){
                this.buffer.remove(seqNum);
            }
            return streamEvents;
        }
        return null;
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
}
