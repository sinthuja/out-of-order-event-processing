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
package org.siddhi.input.order.simulator.executor;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SequenceOOSimulator {
    private static Splitter splitter = Splitter.on(',');

    public static void main(String[] args) {
        try {
            Map<String, String> ooSimulated = getAllData("/Users/sinthu/Downloads/dataset1.csv");
            Map<String, String> sequenced = getAllData("/Users/sinthu/Downloads/dataset1_copy.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, String> getAllData(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path), 10 * 1024 * 1024);
        String line = br.readLine();
        Map<String, String> map = new HashMap<String, String>();
        int totalRecords = 0;
        while (line != null) {
            Iterator<String> dataStrIterator = splitter.split(line).iterator();
            String sensor = dataStrIterator.next();
            String timestamp = dataStrIterator.next();
            String key = getKey(sensor, timestamp);
            map.put(key, line);
            totalRecords++;
            line = br.readLine();
        }
        br.close();
        if (totalRecords != map.size()) {
            throw new RuntimeException("Total record count = " + totalRecords + ", but hashmap size is = " + map.size()
                    + " for file : " + path);
        }
        return map;
    }

    private static String getKey(String sensor, String timestamp) {
        return sensor + "__" + timestamp;
    }
}
