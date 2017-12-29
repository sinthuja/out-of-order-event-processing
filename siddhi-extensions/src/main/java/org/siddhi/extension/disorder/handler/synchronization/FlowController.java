package org.siddhi.extension.disorder.handler.synchronization;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FlowController {

    private volatile boolean paused = false;
    private ReentrantLock lock;
    private Condition condition;

    public FlowController() {
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void barrier() {
        if (paused) { //spurious wakeup condition is deliberately traded off for performance
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
    }
}
