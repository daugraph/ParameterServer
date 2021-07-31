package cn.daugraph.ps.core;

import cn.daugraph.ps.core.handler.RecvHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Customer {

    private static final Logger LOG = LoggerFactory.getLogger(Customer.class);

    private final ExecutorService es = Executors.newFixedThreadPool(1);

    private final int appId;
    private final int customerId;
    private final RecvHandler handler;

    private final BlockingQueue<Message> queue = new LinkedBlockingDeque<>();

    private final List<int[]> tracker = new ArrayList<>();
    private final Lock trackerLock = new ReentrantLock();
    private final Condition trackerCond = trackerLock.newCondition();

    public Customer(int appId, int customerId, RecvHandler handler) {
        this.appId = appId;
        this.customerId = customerId;
        this.handler = handler;
    }

    public int newRequest(int recver) {
        trackerLock.lock();
        try {
            int groupSize = PostOffice.get().getNodeIds(recver).size();
            tracker.add(new int[]{groupSize, 0});
            return tracker.size() - 1;
        } finally {
            trackerLock.unlock();
        }
    }

    public void waitRequest(int timestamp) {
        trackerLock.lock();
        try {
            while (tracker.get(timestamp)[0] != tracker.get(timestamp)[1]) {
                trackerCond.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            trackerLock.unlock();
        }
    }

    public int numResponse(int timestamp) {
        trackerLock.lock();
        try {
            return tracker.get(timestamp)[1];
        } finally {
            trackerLock.unlock();
        }
    }

    public void addResponse(int timestamp, int num) {
        trackerLock.lock();
        try {
            tracker.get(timestamp)[1]++;
        } finally {
            trackerLock.unlock();
        }
    }

    public void accept(Message message) {
        queue.add(message);
    }

    public int getAppId() {
        return appId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void initialize() {
        // 启动接收线程
        es.execute(new Receiving());
    }

    private class Receiving implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // Get message from queue
                    Message message = queue.take();
                    if (!message.getMeta().getControl().isEmpty()
                            && message.getMeta().getControl().getCommand() == Command.TERMINATE) {
                        break;
                    }
                    // Process message
                    handler.process(message);
                    LOG.info("Current tracker state: {}", tracker);
                    if (!message.getMeta().isRequest()) {
                        trackerLock.lock();
                        try {
                            tracker.get(message.getMeta().getTimestamp())[1]++;
                            trackerCond.signalAll();
                        } finally {
                            trackerLock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }
}
