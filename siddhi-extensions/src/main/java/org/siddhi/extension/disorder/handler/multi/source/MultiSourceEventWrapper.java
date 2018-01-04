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

public class MultiSourceEventWrapper implements Comparable<MultiSourceEventWrapper> {
    private StreamEvent event;
    private long relativeTime;
    private String sourceId;

    public MultiSourceEventWrapper(String sourceId, StreamEvent event, long relativeTime) {
        this.event = event;
        this.relativeTime = relativeTime;
        this.sourceId = sourceId;
    }

    public StreamEvent getEvent() {
        return event;
    }

    public String getSourceId() {
        return sourceId;
    }

    @Override
    public int compareTo(MultiSourceEventWrapper sourceEventWrapper) {
        if (this.relativeTime > sourceEventWrapper.relativeTime) {
            return 1;
        } else if (this.relativeTime < sourceEventWrapper.relativeTime) {
            return -1;
        } else {
            return 0;
        }
    }
}
