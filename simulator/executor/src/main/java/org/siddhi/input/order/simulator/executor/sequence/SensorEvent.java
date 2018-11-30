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
package org.siddhi.input.order.simulator.executor.sequence;

public class SensorEvent implements Comparable<SensorEvent> {
    private long timestamp;
    private String eventLine;
    private int sensorId = -1;

    public SensorEvent(long timestamp, String eventLine) {
        this.timestamp = timestamp;
        this.eventLine = eventLine;
    }

    public void updateEvent(long sequenceNumber, int sensorId) {
        eventLine = eventLine + "," + sensorId + "," + sequenceNumber;
    }

    public String getEventLine() {
        return eventLine;
    }

    public Integer getSensorId() {
        if (sensorId == -1) {
            String[] elements = this.eventLine.split(",");
            this.sensorId = Integer.parseInt(elements[elements.length - 2]);
        }
        return sensorId;
    }

    public long getTimestamp() {
        return timestamp;
    }


    @Override
    public int compareTo(SensorEvent o) {
        long val = this.timestamp - o.timestamp;
        if (val < 0) {
            return -1;
        } else if (val > 0) {
            return 1;
        } else {
            return 1;
        }
    }
}
