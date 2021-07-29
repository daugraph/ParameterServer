package cn.daugraph.ps.core;

import cn.daugraph.ps.core.common.Constants;
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
        LOG.info("start init PostOffice");
        van = new ZmqVan();
        numWorkers = Integer.parseInt(System.getenv(Constants.DMLC_NUM_WORKER));
        numServers = Integer.parseInt(System.getenv(Constants.DMLC_NUM_SERVER));
        String role = System.getenv(Constants.DMLC_ROLE);
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

        initStage = 0;
        LOG.info("num workers = {}, num servers = {}, role = {}", numWorkers, numServers, role);
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
                            Constants.WORKER_GROUP,
                            Constants.WORKER_GROUP + Constants.SERVER_GROUP,
                            Constants.WORKER_GROUP + Constants.SCHEDULER,
                            Constants.ALL_GROUP}) {
                        addNodeToGroup(nodeId, groupId);
                    }
                }

                for (int i = 0; i < numServers; ++i) {
                    int nodeId = serverRankToID(i);
                    for (int groupId : new int[]{
                            nodeId,
                            Constants.SERVER_GROUP,
                            Constants.SERVER_GROUP + Constants.WORKER_GROUP,
                            Constants.SERVER_GROUP + Constants.SCHEDULER,
                            Constants.ALL_GROUP}) {
                        addNodeToGroup(nodeId, groupId);
                    }
                }

                for (int groupId : new int[]{
                        Constants.SCHEDULER,
                        Constants.SCHEDULER + Constants.WORKER_GROUP,
                        Constants.SCHEDULER + Constants.SERVER_GROUP,
                        Constants.ALL_GROUP}) {
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

    private int myRank() {
        return idToRank(van.getMyNode().getId());
    }

    public synchronized void addCustomer(Customer customer) {
        int appId = customer.getAppId();
        int customerId = customer.getCustomerId();
        if (customers.containsKey(appId) && customers.get(appId).containsKey(customerId)) {
            LOG.error("Customer id " + customerId + " already exists");
        }
        customers.get(appId).put(customerId, customer);
        barrierDone.get(appId).put(customerId, false);
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
        List<Integer> nodes = isScheduler ? getNodeIds(Constants.WORKER_GROUP + Constants.SERVER_GROUP)
                : getNodeIds(Constants.SCHEDULER);
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

    public void setVan(Van van) {
        this.van = van;
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

    public List<Range> getServerKeyRanges() {
        return serverKeyRanges;
    }

    public boolean isWorker() {
        return isWorker;
    }

    public void setWorker(boolean worker) {
        isWorker = worker;
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

    public void setScheduler(boolean scheduler) {
        isScheduler = scheduler;
    }

    public int getNumServers() {
        return numServers;
    }

    public void setNumServers(int numServers) {
        this.numServers = numServers;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public void setNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public HashMap<Integer, HashMap<Integer, Boolean>> getBarrierDone() {
        return barrierDone;
    }

    public int getVerbose() {
        return verbose;
    }

    public void setVerbose(int verbose) {
        this.verbose = verbose;
    }

    public HashMap<Integer, Long> getHeartbeats() {
        return heartbeats;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void manage(Message msg) {
        Control ctrl = msg.getMeta().getControl();
        if (ctrl.getCommand() == Command.BARRIER && !msg.getMeta().isRequest()) {
            barrierLock.lock();
            try {
                int size = barrierDone.get(msg.getMeta().getAppId()).size();
                for (int customerId = 0; customerId < size; customerId++) {
                    barrierDone.get(msg.getMeta().getAppId()).put(customerId, true);
                }
            } finally {
                barrierLock.unlock();
                barrierCond.notifyAll();
            }
        }
    }

    public void barrier(int customerId, int nodeGroup) {
        if (getNodeIds(nodeGroup).size() <= 1)
            return;
        Role role = van.getMyNode().getRole();
        if (role == Role.SCHEDULER && (nodeGroup & Constants.SCHEDULER) == 0) {
            LOG.error("node group don't match, role: {}, group: {}", role, nodeGroup);
            return;
        } else if (role == Role.WORKER && (nodeGroup & Constants.WORKER_GROUP) == 0) {
            LOG.error("node group don't match, role: {}, group: {}", role, nodeGroup);
            return;
        } else if (role == Role.SERVER && (nodeGroup & Constants.SERVER_GROUP) == 0) {
            LOG.error("node group don't match, role: {}, group: {}", role, nodeGroup);
            return;
        }
        barrierLock.lock();
        try {
            barrierDone.get(0).put(customerId, false);
            Control control = new Control(Command.BARRIER, nodeGroup, new ArrayList<>(), 0);
            Meta meta = new Meta.Builder().setRecver(Constants.SCHEDULER).setRequest(true).setAppId(0)
                    .setCustomerId(customerId).setTimestamp(van.getNextTimestamp()).setControl(control).build();
            van.send(new Message(meta));
            while (!barrierDone.get(0).get(customerId)) {
                try {
                    barrierCond.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting barrier condition: {}", e.getMessage());
                    break;
                }
            }
        } finally {
            barrierLock.unlock();
        }
    }

    public void finalize(int customerId, boolean doBarrier) {
        if (doBarrier) {
            barrier(customerId, Constants.ALL_GROUP);
        }
    }
}
