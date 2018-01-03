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
package org.siddhi.extension.disorder.handler.multi.source;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.siddhi.extension.disorder.handler.*;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MultiSourceEventSynchronizer {
    private HashMap<String, ConcurrentLinkedQueue<MultiSourceEventWrapper>> sourceBasedStreams = new HashMap<>();

    public void putEvent(String sourceId, StreamEvent streamEvent, long eventTime) {
        ConcurrentLinkedQueue<MultiSourceEventWrapper> streamPipe = getStreamPipe(sourceId);
        long drift = EventSourceDriftHolder.getInstance().getDrift(sourceId);
        streamPipe.add(new MultiSourceEventWrapper(streamEvent, eventTime + drift));
    }

    private ConcurrentLinkedQueue<MultiSourceEventWrapper> getStreamPipe(String sourceId) {
        ConcurrentLinkedQueue<MultiSourceEventWrapper> streamPipe = sourceBasedStreams.get(sourceId);
        if (streamPipe == null) {
            synchronized (sourceId.intern()) {
                streamPipe = sourceBasedStreams.get(sourceId);
                if (streamPipe == null) {
                    streamPipe = new ConcurrentLinkedQueue<>();
                    sourceBasedStreams.put(sourceId, streamPipe);
                }
            }
        }
        return streamPipe;
    }

    public void putEvent(String sourceId, List<StreamEventWrapper> eventList) {
        ConcurrentLinkedQueue<MultiSourceEventWrapper> streamPipe = getStreamPipe(sourceId);
        for (StreamEventWrapper eventWrapper : eventList) {
            long drift = EventSourceDriftHolder.getInstance().getDrift(sourceId);
            streamPipe.add(new MultiSourceEventWrapper(eventWrapper.getStreamEvent(), eventWrapper.getEventTime()
                    + drift));
        }
    }

}
