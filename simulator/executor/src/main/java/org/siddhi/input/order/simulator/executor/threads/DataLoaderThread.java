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

package org.siddhi.input.order.simulator.executor.threads;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by miyurud on 7/27/15.
 * This is an interface which should be implemented by all the data loader threads. For each and every data set we will
 * have different data loader threads. All of them should implement this interface so that OOSimulator can use them
 * during its operations.
 */
public interface DataLoaderThread extends Runnable{
    /**
     * Note that the first field listed in the events output from the data loader must correspond to the timestamp used
     * for ordering the events. This is an assumption which must be followed in all use cases.
     */
    public void run();
    public LinkedBlockingQueue<Object> getEventBuffer();
}
