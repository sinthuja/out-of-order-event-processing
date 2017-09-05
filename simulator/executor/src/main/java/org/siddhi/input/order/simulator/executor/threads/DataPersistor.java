package org.siddhi.input.order.simulator.executor.threads;


public interface DataPersistor {
    public void persistEvent(Object[] data);
    public void close();
}
