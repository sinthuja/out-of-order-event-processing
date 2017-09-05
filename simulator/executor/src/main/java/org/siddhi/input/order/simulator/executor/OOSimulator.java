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

import org.siddhi.input.order.simulator.executor.threads.DataLoaderThread;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by miyurud on 7/27/15.
 * This application is a simulator for out of order events. Given a timely ordered event sequence this application
 * introduces disorder if required. This will be useful for debugging ans testing the out-of-order execution of
 * CEP systems.
 */
public class OOSimulator {
    private Object[] event;
    private LinkedBlockingQueue<Object> eventBuffer;
    private DataLoaderThread dataLoader;
    private LinkedList<Object> holdOnItems;
    private boolean flag;
    private int outOfOrderInterval;
    private int skipCount; //Initialize to the specified skip count.
    private Object skippedEvent;
    private int timeStampField = 0;

    /**
     * The constructor accepts a data loader thread and the outOfOrderInterval. If the interval is a positive integer
     * value, then for each event, the OOS introduces similar amount of delay for the event. Otherwise the event sequence
     * will be replayed as it is.
     * @param dataLoader The data loader thread.
     * @param outOfOrderInterval On-zero positive integer.
     */
    public OOSimulator(DataLoaderThread dataLoader, int outOfOrderInterval){
        this.dataLoader = dataLoader;
        this.outOfOrderInterval = outOfOrderInterval;
        skipCount = outOfOrderInterval;
        eventBuffer = new LinkedBlockingQueue<Object>();
        holdOnItems = new LinkedList<Object>();
    }

    public void init() {
        dataLoader.run();
        eventBuffer = dataLoader.getEventBuffer();
    }

    public void setRunOO(){
        flag = true;
    }

    /**
     * This method returns an event with a timestamp value as the first field which determines the time in which the
     * event was injected.
     * @return
     */
    public Object[] getNextEvent(){
        if(flag){
            if(skipCount == outOfOrderInterval) {
                //here we immediately buffer the current event and return the next one. Also we set the skip count to
                //the pre specified event count.

                try {
                    skippedEvent = (Object[]) eventBuffer.take();
                } catch (InterruptedException e1) {
                    //break;
                    e1.printStackTrace();
                }
                skipCount--;

                try {
                    event = (Object[]) eventBuffer.take();
                } catch (InterruptedException e1) {
                    //break;
                    e1.printStackTrace();
                }
            }else if(skipCount == 0){
                event = (Object[]) skippedEvent;
                skipCount = outOfOrderInterval; //We are still operating in the mode of out of order operation. Therefore,
                                                 //we need to set the skip count to the pre-specified value.
            }else{
                try {
                    event = (Object[]) eventBuffer.take();
                } catch (InterruptedException e1) {
                    //break;
                    e1.printStackTrace();
                }
                skipCount--;
            }
        }else {
            try {
                event = (Object[]) eventBuffer.take();
            } catch (InterruptedException e1) {
                //break;
                e1.printStackTrace();
            }
        }

        return event;
    }
}
