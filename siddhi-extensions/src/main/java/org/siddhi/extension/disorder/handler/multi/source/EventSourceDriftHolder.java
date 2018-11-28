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

import org.siddhi.extension.disorder.handler.Constants;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EventSourceDriftHolder {
    private static EventSourceDriftHolder instance = new EventSourceDriftHolder();
    private HashMap<String, EventSourceDriftInfo> eventSourceDrift = new HashMap<>();
    private HashMap<String, AtomicInteger> timeSyncAttempts = new HashMap<>();

    private EventSourceDriftHolder() {

    }

    public static EventSourceDriftHolder getInstance() {
        return instance;
    }

    public int getNumberOfSources() {
        return eventSourceDrift.size();
    }

    public void updateTimeDrift(String sourceId, EventSourceDriftInfo timeDrift) {
        String key = getKey(sourceId);
        synchronized (key.intern()) {
            AtomicInteger attempts = this.timeSyncAttempts.get(key);
            if (attempts != null) {
                if (attempts.incrementAndGet() > Constants.WARM_UP_TIME_SYNC_ATTEMPTS) {
                    EventSourceDriftInfo sourceDriftInfo = this.eventSourceDrift.get(key);
                    if (sourceDriftInfo == null) {
                        this.eventSourceDrift.put(key, timeDrift);
                    } else {
                        sourceDriftInfo.drift = Math.round((sourceDriftInfo.drift + timeDrift.drift) * 0.5);
                        sourceDriftInfo.transportDelay = Math.round((sourceDriftInfo.transportDelay
                                + timeDrift.transportDelay) * 0.5);
                        this.eventSourceDrift.put(key, sourceDriftInfo);
                        System.out.println("################### final Drift => source : "
                                + sourceId + " , drift: " + sourceDriftInfo.drift);
                    }
                } else {
                    attempts.incrementAndGet();
                }
            } else {
                this.timeSyncAttempts.put(key, new AtomicInteger(1));
            }
        }
    }

    private String getKey(String sourceId) {
        return sourceId.toLowerCase();
    }

    public long getDrift(String sourceId) {
        return this.eventSourceDrift.get(getKey(sourceId)).drift;
    }

    public long getTransportDelay(String sourceId) {
        return this.eventSourceDrift.get(getKey(sourceId)).transportDelay;
    }

    public static class EventSourceDriftInfo {
        private long drift;
        private long transportDelay;

        public EventSourceDriftInfo(long drift, long transportDelay) {
            this.drift = drift;
            this.transportDelay = transportDelay;
        }
    }

}
