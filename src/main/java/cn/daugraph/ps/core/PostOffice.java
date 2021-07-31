package cn.daugraph.ps.core;

import cn.daugraph.ps.core.common.Constants;
import cn.daugraph.ps.core.common.Utils;
import cn.daugraph.ps.core.van.Van;
import cn.daugraph.ps.core.van.ZmqVan;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostOffice {

    private static final PostOffice instance = new PostOffice();
    private final Logger LOG = LoggerFactory.getLogger(PostOffice.class);
    private final Lock heartbeatLock = new ReentrantLock();
    private final Lock barrierLock = new ReentrantLock();
    private final Lock customerLock = new ReentrantLock();
    private final Lock startLock = new ReentrantLock();
    private final Condition barrierCond = barrierLock.newCondition();
    private final HashMap<Integer, HashMap<Integer, Customer>> customers = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Boolean>> barrierDone = new HashMap<>();
    private final HashMap<Integer, List<Integer>> nodeIds = new HashMap<>();
    private final HashMap<Integer, Long> heartbeats = new HashMap<>();
    private final List<Range> serverKeyRanges = new ArrayList<>();
    private Van van;
    private boolean isWorker;
    private boolean isServer;
    private boolean isScheduler;

    private int numServers;
    private int numWorkers;
    private int initStage;
    private int verbose;

    private long startTime;

    private PostOffice() {
    }

    public static PostOffice get() {
        return instance;
    }

    public void init() {
        LOG.info("Init PostOffice ...");
        van = new ZmqVan();
        numWorkers = Utils.loadIntegerFromEnvironment(Constants.DMLC_NUM_WORKER,
                Constants.DEFAULT_DMLC_NUM_WORKER);
        numServers = Utils.loadIntegerFromEnvironment(Constants.DMLC_NUM_SERVER,
                Constants.DEFAULT_DMLC_NUM_SERVER);
        String role = Utils.loadStringFromEnvironment(Constants.DMLC_ROLE,
                Constants.DEFAULT_DMLC_ROLE);
        switch (role) {
            case "server":
                isServer = true;
                break;
            case "worker":
                isWorker = true;
                break;
            case "scheduler":
                isScheduler = true;
                break;
        }

        initStage = 1;
        LOG.info("Config: num of worker = {}, num of server = {}, current role = {}", numWorkers, numServers, role);
    }

    public void start(int customerId) {
        this.start(customerId, true);
    }

    private void addNodeToGroup(int nodeId, int groupId) {
        if (!nodeIds.containsKey(groupId)) {
            nodeIds.put(groupId, new ArrayList<>());
        }
        nodeIds.get(groupId).add(nodeId);
    }

    public void start(int customerId, boolean doBarrier) {
        startLock.lock();
        try {
            if (initStage == 0) {
                init();

                for (int i = 0; i < numWorkers; ++i) {
                    int nodeId = workerRankToID(i);
                    for (int groupId : new int[]{
                            nodeId,
                            Constants.GROUP_WORKER,
                            Constants.GROUP_WORKER_SCHEDULER,
                            Constants.GROUP_WORKER_SERVER,
                            Constants.GROUP_ALL}) {
                        addNodeToGroup(nodeId, groupId);
                    }
                }

                for (int i = 0; i < numServers; ++i) {
                    int nodeId = serverRankToID(i);
                    for (int groupId : new int[]{
                            nodeId,
                            Constants.GROUP_SERVER,
                            Constants.GROUP_SERVER_SCHEDULER,
                            Constants.GROUP_WORKER_SERVER,
                            Constants.GROUP_ALL}) {
                        addNodeToGroup(nodeId, groupId);
                    }
                }

                for (int groupId : new int[]{
                        Constants.GROUP_SCHEDULER,
                        Constants.GROUP_SERVER_SCHEDULER,
                        Constants.GROUP_WORKER_SCHEDULER,
                        Constants.GROUP_ALL}) {
                    addNodeToGroup(Constants.SCHEDULER, groupId);
                }
            }
        } finally {
            startLock.unlock();
        }

        van.start(customerId);

        startLock.lock();
        try {
            if (initStage == 1) {
                startTime = new Date().getTime();
                initStage++;
            }
        } finally {
            startLock.unlock();
        }

        if (doBarrier) {
        }
    }

    // 9 11 13 15 ...
    public int workerRankToID(int i) {
        return i * 2 + 9;
    }

    // 8 10 12 14 ...
    public int serverRankToID(int i) {
        return i * 2 + 8;
    }

    public int rankToID(int i, Role role) {
        switch (role) {
            case WORKER:
                return workerRankToID(i);
            case SERVER:
                return serverRankToID(i);
        }
        return Constants.SCHEDULER;
    }

    public int idToRank(int i) {
        return Math.max((i - 8) / 2, 0);
    }

    // app id -> (customer id -> customer object)
    public synchronized void addCustomer(Customer customer) {
        int aid = customer.getAppId();
        if (!customers.containsKey(aid)) {
            customers.put(aid, new HashMap<>());
        }

        HashMap<Integer, Customer> customersInApp = customers.get(aid);
        int cid = customer.getCustomerId();
        if (customersInApp.containsKey(cid)) {
            LOG.error("Customer id " + cid + " already exists");
            return;
        }
        customersInApp.put(cid, customer);

        if (!barrierDone.containsKey(aid)) {
            barrierDone.put(aid, new HashMap<>());
        }
        barrierDone.get(aid).put(cid, false);
    }

    public void updateHeartbeat(int nodeId, long t) {
        heartbeatLock.lock();
        try {
            heartbeats.put(nodeId, t);
        } finally {
            heartbeatLock.unlock();
        }
    }

    public List<Integer> getDeadNodes(int t) {
        List<Integer> deadNodes = new ArrayList<>();
        if (!van.isReady() || t == 0)
            return deadNodes;

        long curTime = System.currentTimeMillis() / 1000;
        List<Integer> nodes = isScheduler ? getNodeIds(Constants.GROUP_WORKER_SERVER)
                : getNodeIds(Constants.GROUP_SCHEDULER);
        heartbeatLock.lock();
        try {
            for (int r : nodes) {
                // never recv heart or heartbeat timeout
                if ((!heartbeats.containsKey(r) || heartbeats.get(r) + t < curTime) && startTime + t < curTime) {
                    deadNodes.add(r);
                }
            }
        } finally {
            heartbeatLock.unlock();
        }
        return deadNodes;
    }

    public Van getVan() {
        return van;
    }

    public Customer getCustomer(int appId, int customerId, int timeout) {
        for (int i = 0; i < timeout * 1000 + 1; i++) {
            customerLock.lock();
            try {
                if (customers.containsKey(appId))
                    return customers.get(appId).get(customerId);
            } finally {
                customerLock.unlock();
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Got exception when sleep, {}", e.getMessage());
            }
        }
        return null;
    }

    public List<Integer> getNodeIds(int nodeId) {
        return nodeIds.get(nodeId);
    }

    public boolean isWorker() {
        return isWorker;
    }

    public boolean isServer() {
        return isServer;
    }

    public void setServer(boolean server) {
        isServer = server;
    }

    public boolean isScheduler() {
        return isScheduler;
    }

    public int getNumServers() {
        return numServers;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public HashMap<Integer, HashMap<Integer, Boolean>> getBarrierDone() {
        return barrierDone;
    }

    public HashMap<Integer, Long> getHeartbeats() {
        return heartbeats;
    }

    public void manage(Message msg) {
        barrierLock.lock();
        try {
            int appId = msg.getMeta().getAppId();
            int customerIds = barrierDone.get(appId).size();
            for (int customerId = 0; customerId < customerIds; customerId++)
                barrierDone.get(appId).put(customerId, true);
            barrierCond.signalAll();
        } finally {
            barrierLock.unlock();
        }
    }

    // 向 SCHEDULER 节点发送 BARRIER COMMAND
    public void barrier(int customerId, int nodeGroup) {
        if (getNodeIds(nodeGroup).size() <= 1)
            return;
        // 判断当前节点是否在 Group 中
        Role role = van.getMyNode().getRole();
        if (!isRoleInGroup(van.getMyNode().getRole(), nodeGroup)) {
            LOG.error("Node role {} is not in group {}", role, nodeGroup);
            return;
        }

        barrierLock.lock();
        try {
            if (!barrierDone.containsKey(0)) {
                barrierDone.put(0, new HashMap<>());
            }
            barrierDone.get(0).put(customerId, false);
            Control control = new Control(Command.BARRIER, nodeGroup, new ArrayList<>(), 0);
            Meta meta = new Meta.Builder()
                    .setRecver(Constants.SCHEDULER)
                    .setRequest(true)
                    .setAppId(0)
                    .setCustomerId(customerId)
                    .setTimestamp(van.getTimestamp())
                    .setControl(control)
                    .build();
            van.send(new Message(meta));
            // 等待 Customer 完成
            while (!barrierDone.get(0).get(customerId)) {
                try {
                    barrierCond.await();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting barrier condition: {}", e.getMessage());
                    break;
                }
            }
        } finally {
            barrierLock.unlock();
        }
    }

    private boolean isRoleInGroup(Role role, int nodeGroup) {
        return (role == Role.SCHEDULER) && (nodeGroup & Constants.GROUP_SCHEDULER) != 0
                || (role == Role.WORKER) && (nodeGroup & Constants.GROUP_WORKER) != 0
                || (role == Role.SERVER) && (nodeGroup & Constants.GROUP_SERVER) != 0;
    }

    public void finalize(int customerId, boolean doBarrier) {
        if (doBarrier) {
            barrier(customerId, Constants.GROUP_ALL);
        }
    }
}
