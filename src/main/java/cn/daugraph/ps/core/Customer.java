package cn.daugraph.ps.core;

import cn.daugraph.ps.core.handler.RecvHandler;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private BlockingQueue<Message> queue;
    private List<int[]> tracker;

    public Customer(int appId, int customerId, RecvHandler handler) {
        this.appId = appId;
        this.customerId = customerId;
        this.handler = handler;
    }

    public int newRequest(int recver) {
        lock.lock();
        try {
            int num = PostOffice.getInstance().getNodeIds(recver).size();
            tracker.add(new int[]{num, 0});
            // 这里返回的newRequest对应的下标
            return tracker.size() - 1;
        } finally {
            lock.unlock();
        }
    }

    public void waitRequest(int timestamp) {
        lock.lock();
        try {
            while (tracker.get(timestamp)[0] != tracker.get(timestamp)[1]) {
                cond.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public int numResponse(int timestamp) {
        lock.lock();
        try {
            return tracker.get(timestamp)[1];
        } finally {
            lock.unlock();
        }
    }

    public void addResponse(int timestamp, int num) {
        lock.lock();
        try {
            tracker.get(timestamp)[1]++;
        } finally {
            lock.unlock();
        }
    }

    public Customer createCustomer(int appId, int customerId, RecvHandler handler) {
        Customer customer = new Customer(appId, customerId, handler);
        PostOffice.getInstance().addCustomer(customer);
        es.execute(new RecvThread());
        return customer;
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

    public BlockingQueue<Message> getQueue() {
        return queue;
    }

    public void setQueue(BlockingQueue<Message> queue) {
        this.queue = queue;
    }

    public List<int[]> getTracker() {
        return tracker;
    }

    public void setTracker(List<int[]> tracker) {
        this.tracker = tracker;
    }

    public RecvHandler getHandler() {
        return handler;
    }

    private class RecvThread implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // Get message from queue
                    Message message = queue.take();
                    if (!message.getMeta().getControl().isEmpty()
                            && message.getMeta().getControl().getCommand() == Control.Command.TERMINATE) {
                        break;
                    }
                    // Process message
                    handler.process(message);
                    // Request + 1
                    if (message.getMeta().isRequest()) {
                        lock.lock();
                        try {
                            tracker.get(message.getMeta().getTimestamp())[1]++;
                            cond.notifyAll();
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }
}
