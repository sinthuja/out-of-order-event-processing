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

import org.wso2.siddhi.core.query.processor.Processor;

import java.util.HashMap;

public class MultiSourceEventSynchronizerManager {
    private static MultiSourceEventSynchronizerManager instance = new MultiSourceEventSynchronizerManager();
    private HashMap<String, MultiSourceEventSynchronizer> synchronizers = new HashMap<>();

    private MultiSourceEventSynchronizerManager() {

    }

    public static MultiSourceEventSynchronizerManager getInstance() {
        return instance;
    }

    private synchronized MultiSourceEventSynchronizer addMultiSourceEventSynchronizer(String streamId,
                                                                                      Processor processor,
                                                                                      long userDefinedTimout) {
        MultiSourceEventSynchronizer synchronizer = this.synchronizers.get(streamId);
        if (synchronizer == null) {
            synchronizer = new MultiSourceEventSynchronizer(processor, userDefinedTimout);
            this.synchronizers.put(streamId, synchronizer);
        }
        return synchronizer;
    }

    public MultiSourceEventSynchronizer getMultiSourceEventSynchronizer(String streamId,
                                                                        Processor nextProcessor,
                                                                        long userDefinedTimeout) {
        MultiSourceEventSynchronizer synchronizer = this.synchronizers.get(streamId);
        if (synchronizer == null) {
            synchronizer = addMultiSourceEventSynchronizer(streamId, nextProcessor, userDefinedTimeout);
        }
        return synchronizer;
    }
}
