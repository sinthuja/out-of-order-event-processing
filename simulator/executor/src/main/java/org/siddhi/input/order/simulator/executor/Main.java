/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.siddhi.input.order.simulator.executor;

import org.siddhi.input.order.simulator.executor.threads.DEBS2013DataLoderThread;
import org.siddhi.input.order.simulator.executor.threads.DataLoaderThread;
import org.siddhi.input.order.simulator.executor.threads.DataPersistor;
import org.siddhi.input.order.simulator.executor.threads.GeneralDataPersistor;

public class Main {

    public static void main(String[] args) {
        int numRecords = 1000;
        //String inputFile = "/home/miyurud/DEBS2015/dataset/20M-dataset.csv";
        String inputFile = "/home/miyurud/Projects/OutofOrderEvents/datasets/DEBS2013-Football-games/full-game";
        //DataLoaderThread thrd = new DEBS2015DataLoderThread(inputFile, 1000);
        DataLoaderThread thrd = new DEBS2013DataLoderThread(inputFile, numRecords, 0);
        OOSimulator simulator = new OOSimulator(thrd, 5);
        DataPersistor persistor = new GeneralDataPersistor("/home/miyurud/Projects/OutofOrderEvents/datasets/synthesized_data/test1.csv");
        simulator.init();

        simulator.setRunOO();

        for(int i = 0; i < numRecords; i++){
            persistor.persistEvent(simulator.getNextEvent());
        }

        persistor.close();

//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
//        System.out.println(simulator.getNextEvent()[0]);
    }
}
