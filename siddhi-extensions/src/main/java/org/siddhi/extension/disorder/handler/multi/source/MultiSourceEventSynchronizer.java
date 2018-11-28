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
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.query.processor.Processor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class MultiSourceEventSynchronizer {
    private Map<String, MultiSourceEventWrapperQueue> sourceBasedStreams = new ConcurrentHashMap<>();
    private Processor nextProcessor;
    private Map<String, EventSource> eventStreamTimeout = new HashMap<>();
    private long userDefinedTimeout;
    private final Object linkedListLock = new Object();

    MultiSourceEventSynchronizer(Processor nextProcessor, long userDefinedTimeout) {
        this.nextProcessor = nextProcessor;
        Executors.newSingleThreadExecutor().submit(new MultiSourceEventSynchronizingWorker());
        this.userDefinedTimeout = userDefinedTimeout;
    }

    public void putEvent(String sourceId, StreamEvent streamEvent, long eventTime, long sequenceNumber,
                         ComplexEventPopulater complexEventPopulater) {
        LinkedList<MultiSourceEventWrapper> streamPipe = getStreamPipe(sourceId);
        long drift = EventSourceDriftHolder.getInstance().getDrift(sourceId);
        MultiSourceEventWrapper eventWrapper = new MultiSourceEventWrapper(sourceId,
                streamEvent, eventTime + drift, sequenceNumber, complexEventPopulater);
        if (!streamPipe.isEmpty()) {
            if (streamPipe.getLast().getSequenceNumber() < sequenceNumber) {
                synchronized (linkedListLock) {
                    streamPipe.add(eventWrapper);
                }
            } else {
                //Add the event in the most corrrect position.
                synchronized (linkedListLock) {
                    int index = 0;
                    for (MultiSourceEventWrapper element : streamPipe) {
                        if (element.getSequenceNumber() > sequenceNumber) {
                            streamPipe.add(index, eventWrapper);
                            break;
                        }
                        index++;
                    }
                }
            }
        } else {
            synchronized (linkedListLock) {
                streamPipe.add(eventWrapper);
            }
        }
    }

    private LinkedList<MultiSourceEventWrapper> getStreamPipe(String sourceId) {
        MultiSourceEventWrapperQueue streamPipe = sourceBasedStreams.get(sourceId);
        if (streamPipe == null) {
            synchronized (sourceId.intern()) {
                streamPipe = sourceBasedStreams.computeIfAbsent(sourceId, k ->
                        new MultiSourceEventWrapperQueue(new LinkedList<>()));
            }
        }
        return streamPipe.queue;
    }

    public void putEvent(String sourceId, List<StreamEventWrapper> eventList, ComplexEventPopulater complexEventPopulater) {
        LinkedList<MultiSourceEventWrapper> streamPipe = getStreamPipe(sourceId);
        long drift = EventSourceDriftHolder.getInstance().getDrift(sourceId);
        for (StreamEventWrapper eventWrapper : eventList) {
            synchronized (linkedListLock) {
                streamPipe.add(new MultiSourceEventWrapper(sourceId, eventWrapper.getStreamEvent(),
                        eventWrapper.getEventTime() + drift,
                        eventWrapper.getSequenceNum(), complexEventPopulater));
            }
        }
    }

    public long getUncertainTimeRange() {
        long delay = 0;
        for (String sourceId : this.sourceBasedStreams.keySet()) {
            long eventSourceDelay = EventSourceDriftHolder.getInstance().getTransportDelay(sourceId);
            if (eventSourceDelay > delay) {
                delay = eventSourceDelay;
            }
        }
        return delay;
    }


    public class MultiSourceEventSynchronizingWorker extends Thread {
        private TreeSet<MultiSourceEventWrapper> events = new TreeSet<>();
        private ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);

        public void run() {
            // TODO: get from the original event chunk
            while (EventSourceDriftHolder.getInstance().getNumberOfSources() != sourceBasedStreams.size()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                }
            }
            while (true) {
                try {
                    //Waiting until all sources have started to send events to synchronize.
                    if (events.isEmpty()) {
                        //Iterate through all the sources, and fetch the first event in the queue (ie., the smallest
                        // timestamp events of the source)
                        for (Map.Entry<String, MultiSourceEventWrapperQueue> queue
                                : sourceBasedStreams.entrySet()) {
                            populateEvent(queue.getKey(), queue.getValue());
                        }
                        //Added all the smallest events from the sources, and as it's the sortedSet, the first element of
                        // the tree set - events, will have the smallest event among all sources.
                        //Now get that event and add to the complexEventChunk.
                        checkAndPublishToNextProcess();
                    } else {
                        //Already the first element of events - treeset is added to the complexEventChunk.
                        //Now we have to find the subsequent events.
                        //Fetch the event from the same source as the flushed, and now compare all the event's timestamp,
                        // and pick the smallest event.
                        MultiSourceEventWrapper flushedEvent = events.pollFirst();
                        MultiSourceEventWrapperQueue queue =
                                sourceBasedStreams.get(flushedEvent.getSourceId());
                        if (events.size() != sourceBasedStreams.size() - 1) {
                            for (Map.Entry<String, MultiSourceEventWrapperQueue> eventQueue
                                    : sourceBasedStreams.entrySet()) {
                                boolean alreadyAdded = false;
                                for (MultiSourceEventWrapper eventWrapper : events) {
                                    if (eventWrapper.getSourceId().equalsIgnoreCase(eventQueue.getKey())) {
                                        alreadyAdded = true;
                                        break;
                                    }
                                }
                                if (!alreadyAdded) {
                                    populateEvent(eventQueue.getKey(), eventQueue.getValue());
                                }
                            }
                        } else {
                            populateEvent(flushedEvent.getSourceId(), queue);
                        }
                        checkAndPublishToNextProcess();
                    }
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }

        private void checkAndPublishToNextProcess() {
            if (!events.isEmpty()) {
                //Based on the comparator implemented the first event is the smallest timestamp
                // event among all sources.
                complexEventChunk.add(events.first().getEvent());
            } else {
                // if all events are processed, then it's time to flush all events.
                if (complexEventChunk.hasNext()) {
                    nextProcessor.process(complexEventChunk);
                    complexEventChunk.clear();
                }
            }
        }

        private void populateEvent(String sourceId, MultiSourceEventWrapperQueue wrapperQueue) {
            LinkedList<MultiSourceEventWrapper> queue = wrapperQueue.queue;
            if (!queue.isEmpty()) {
                synchronized (linkedListLock) {
                    try {
                        if (!queue.isEmpty()) {
                            events.add(queue.removeFirst());
                            wrapperQueue.alreadyCheckedWithoutEvent = false;
                        }
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            } else {
                if (complexEventChunk.hasNext()) {
                    nextProcessor.process(complexEventChunk);
                    complexEventChunk = new ComplexEventChunk<>(null, null, true);
                }
                if (!wrapperQueue.alreadyCheckedWithoutEvent) {
                    try {
                        Thread.sleep(Utils.getTimeout(userDefinedTimeout, eventStreamTimeout.get(sourceId)));
                    } catch (InterruptedException ignored) {
                    }
                    if (!queue.isEmpty()) {
                        synchronized (linkedListLock) {
                            if (!queue.isEmpty()) {
                                events.add(queue.removeFirst());
                            }
                        }
                    } else {
                        wrapperQueue.alreadyCheckedWithoutEvent = true;
                    }
                }
            }
        }
    }

    public class MultiSourceEventWrapperQueue {
        private LinkedList<MultiSourceEventWrapper> queue;
        private boolean alreadyCheckedWithoutEvent;

        MultiSourceEventWrapperQueue(LinkedList<MultiSourceEventWrapper> queue) {
            this.queue = queue;
            this.alreadyCheckedWithoutEvent = false;
        }
    }

    public void putEventSource(String sourceId, EventSource eventSource) {
        this.eventStreamTimeout.put(sourceId, eventSource);
    }
}
