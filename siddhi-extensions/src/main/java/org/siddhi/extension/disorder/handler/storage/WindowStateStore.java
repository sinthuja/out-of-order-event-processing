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
package org.siddhi.extension.disorder.handler.storage;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class WindowStateStore {
    private TreeMap<Long, Map<String, Object>> stateStore;
    private int storageLimit;

    public WindowStateStore(int storageLimit) {
        this.stateStore = new TreeMap<>();
        this.storageLimit = storageLimit;
    }

    public WindowStateStore() {
        this(10);
    }

    public void storeState(long windowExpireTime, Map<String, Object> state) {
        if (this.stateStore.size() == storageLimit) {
            this.stateStore.remove(this.stateStore.firstKey());
        }
        this.stateStore.put(windowExpireTime, state);
    }

    public Map<Long, Map<String, Object>> retrieveStates(long retrieveTimeFrom) {
        Set<Map.Entry<Long, Map<String, Object>>> states = this.stateStore.entrySet();
        Map<Long, Map<String, Object>> returnState = new TreeMap<>();
        for (Map.Entry<Long, Map<String, Object>> state : states) {
            if (state.getKey() > retrieveTimeFrom) {
                returnState.put(state.getKey(), state.getValue());
            }
        }
        return returnState;
    }

}
