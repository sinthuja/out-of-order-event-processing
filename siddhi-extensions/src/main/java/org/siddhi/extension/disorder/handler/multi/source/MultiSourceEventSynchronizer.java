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

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.siddhi.extension.disorder.handler.*;
import org.wso2.siddhi.core.query.processor.Processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

public class MultiSourceEventSynchronizer {
    private Map<String, ConcurrentLinkedQueue<MultiSourceEventWrapper>> sourceBasedStreams = new HashMap<>();
    private Processor nextProcessor;
    private Map<String, Long> eventStreamTimeout = new HashMap<>();

    MultiSourceEventSynchronizer(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
        Executors.newSingleThreadExecutor().submit(new MultiSourceEventSynchronizingWorker());
    }

    public void putEvent(String sourceId, StreamEvent streamEvent, long eventTime) {
        ConcurrentLinkedQueue<MultiSourceEventWrapper> streamPipe = getStreamPipe(sourceId);
        long drift = EventSourceDriftHolder.getInstance().getDrift(sourceId);
        streamPipe.add(new MultiSourceEventWrapper(sourceId, streamEvent, eventTime + drift));
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
            streamPipe.add(new MultiSourceEventWrapper(sourceId, eventWrapper.getStreamEvent(),
                    eventWrapper.getEventTime() + drift));
        }
    }

    public class MultiSourceEventSynchronizingWorker extends Thread {
        private TreeSet<MultiSourceEventWrapper> events = new TreeSet<>();
        private ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(null, null, true);

        public void run() {
            // TODO: get from the original event chunk
            while (true) {
                if (events.isEmpty()) {
                    for (Map.Entry<String, ConcurrentLinkedQueue<MultiSourceEventWrapper>> queue
                            : sourceBasedStreams.entrySet()) {
                        populateEvent(queue.getKey(), queue.getValue());
                    }
                    checkAndPublishToNextProcess();
                } else {
                    MultiSourceEventWrapper flushedEvent = events.first();
                    ConcurrentLinkedQueue<MultiSourceEventWrapper> queue =
                            sourceBasedStreams.get(flushedEvent.getSourceId());
                    events.remove(flushedEvent);
                    populateEvent(flushedEvent.getSourceId(), queue);
                    checkAndPublishToNextProcess();
                }
            }
        }

        private void checkAndPublishToNextProcess() {
            if (!events.isEmpty()) {
                complexEventChunk.add(events.first().getEvent());
            } else {
                if (complexEventChunk.hasNext()) {
                    nextProcessor.process(complexEventChunk);
                    complexEventChunk = new ComplexEventChunk<>(null, null, true);
                }
            }
        }

        private void populateEvent(String sourceId, ConcurrentLinkedQueue<MultiSourceEventWrapper> queue) {
            MultiSourceEventWrapper eventWrapper = queue.poll();
            if (eventWrapper != null) {
                events.add(eventWrapper);
            } else {
                //TODO: should wait until the timeout, and hence flush all on hand events
                nextProcessor.process(complexEventChunk);
                complexEventChunk = new ComplexEventChunk<>(null, null, true);
                try {
                    Thread.sleep(eventStreamTimeout.get(sourceId));
                } catch (InterruptedException ignored) {
                }
                eventWrapper = queue.poll();
                if (eventWrapper != null) {
                    events.add(eventWrapper);
                }
            }
        }
    }

    public void putTimeout(String sourceId, long timeout){
        this.eventStreamTimeout.put(sourceId, timeout);
    }
}
