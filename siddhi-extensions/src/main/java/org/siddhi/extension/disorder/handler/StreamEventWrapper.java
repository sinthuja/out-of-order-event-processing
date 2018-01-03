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

import org.wso2.siddhi.core.event.stream.StreamEvent;

public class StreamEventWrapper {
    private StreamEvent streamEvent;
    private long expiryTimestamp;
    private long arrivalTimestamp;
    private long eventTime;
    private long sequenceNum;

    StreamEventWrapper(StreamEvent streamEvent, long expiryTimestamp, long arrivalTimestamp, long sequenceNum) {
        this.streamEvent = streamEvent;
        this.expiryTimestamp = expiryTimestamp;
        this.arrivalTimestamp = arrivalTimestamp;
        this.sequenceNum = sequenceNum;
    }

    public StreamEvent getStreamEvent() {
        return streamEvent;
    }

    public long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    public long getArrivalTimestamp(){
        return arrivalTimestamp;
    }

    public long getSequenceNum() {
        return sequenceNum;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }
}
