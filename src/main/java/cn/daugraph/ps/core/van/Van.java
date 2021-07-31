package cn.daugraph.ps.core.van;

import cn.daugraph.ps.core.Command;
import cn.daugraph.ps.core.Control;
import cn.daugraph.ps.core.Customer;
import cn.daugraph.ps.core.DataType;
import cn.daugraph.ps.core.Message;
import cn.daugraph.ps.core.Meta;
import cn.daugraph.ps.core.Node;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.Role;
import cn.daugraph.ps.core.common.Constants;
import cn.daugraph.ps.core.common.Utils;
import cn.daugraph.ps.proto.java.ParameterServerMeta;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public abstract class Van {

    public static final Logger LOG = LoggerFactory.getLogger(Van.class);
    private final AtomicInteger sendBytes = new AtomicInteger(0);
    private final AtomicInteger timestamp = new AtomicInteger(0);
    private final int dropRate = 0;
    private final Node scheduler = new Node(Role.SCHEDULER);
    private final HashMap<String, Integer> connectedNodes = new HashMap<>();
    private final HashMap<Integer, Integer> sharedNodeMapping = new HashMap<>();
    private final ExecutorService es = Executors.newFixedThreadPool(2);
    private final int recvBytes = 0;
    protected Lock startLock = new ReentrantLock();
    protected Node myNode;
    private int heartbeatTimeout;
    private ZContext context;
    private final HashMap<Integer, ZMQ.Socket> senders = new HashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private int numServers;
    private int numWorkers;
    private int[] barrierCount;
    private int initStage = 0;
    private boolean isScheduler;

    public void processTerminateCommand() {
        LOG.info("{} is stopped", myNode);
        ready.set(false);
    }

    public void processAddNodeCommandAtScheduler(Message message, List<Node> registeredNodes, Meta recoveryMeta) {
        LOG.info("Current Message: {}", message);
        LOG.info("Registered Nodes: {}", registeredNodes);
        LOG.info("Recovery Nodes: {}", recoveryMeta);

        long ts = System.currentTimeMillis() / 1000;

        int totalNodes = PostOffice.get().getNumServers() + PostOffice.get().getNumWorkers();
        // 收到全部节点注册后，给节点分配 node id
        if (registeredNodes.size() == totalNodes) {
            Collections.sort(registeredNodes);
            for (Node registeredNode : registeredNodes) {
                String address = registeredNode.getAddress();
                int allocatedNodeId = registeredNode.getRole() == Role.SERVER
                        ? PostOffice.get().serverRankToID(numServers)
                        : PostOffice.get().workerRankToID(numWorkers);
                if (!connectedNodes.containsKey(address)) {
                    registeredNode.setId(allocatedNodeId);
                    connect(registeredNode);
                    PostOffice.get().updateHeartbeat(registeredNode.getId(), ts);
                    // Mapping (address -> node id)
                    LOG.info("Allocated id {} for {}", allocatedNodeId, address);
                    connectedNodes.put(address, allocatedNodeId);
                } else {
                    // 如果相同的 hostname:port 已经有 node id, 则后继 node 使用相同 node id
                    int firstAllocatedId = connectedNodes.get(address);
                    sharedNodeMapping.put(allocatedNodeId, firstAllocatedId);
                    registeredNode.setId(firstAllocatedId);
                }
                if (registeredNode.getRole() == Role.SERVER)
                    numServers++;
                if (registeredNode.getRole() == Role.WORKER)
                    numWorkers++;
            }

            // 把当前节点也就是 SCHEDULER 节点加入已注册节点集合中, 给所有 SERVER + WORKER 节点回复 ADD_NODE
            List<Node> allNodes = new ArrayList<>(registeredNodes);
            allNodes.add(myNode);
            Control control = new Control.Builder()
                    .setCommand(Command.ADD_NODE)
                    .setNodes(allNodes)
                    .build();
            for (int nodeId : PostOffice.get().getNodeIds(Constants.GROUP_WORKER_SERVER)) {
                if (!sharedNodeMapping.containsKey(nodeId)) {
                    Meta meta = new Meta.Builder()
                            .setControl(control)
                            .setRecver(nodeId)
                            .setTimestamp(timestamp.getAndIncrement())
                            .build();
                    send(new Message(meta));
                }
            }
            LOG.info("Scheduler is connected to {} workers and {} servers", numWorkers, numServers);
            // SCHEDULER 节点就绪
            ready.set(true);
            return;
        }

        // 恢复节点
        List<Node> recoveryNodes = recoveryMeta.getControl().getNodes();
        recoveryMeta.getControl().setCommand(Command.ADD_NODE);
        if (recoveryNodes.size() != 1) {
            LOG.error("Bad recovery meta : {}", recoveryMeta);
            return;
        }

        Node recoveryNode = recoveryNodes.get(0);
        Set<Integer> deadNodes = new HashSet<>(PostOffice.get().getDeadNodes(heartbeatTimeout));
        connect(recoveryNode);
        PostOffice.get().updateHeartbeat(recoveryNode.getId(), ts);
        for (int node : PostOffice.get().getNodeIds(Constants.GROUP_WORKER_SERVER)) {
            if (node != recoveryNode.getId() && deadNodes.contains(node)) {
                continue;
            }
            Control control = new Control.Builder()
                    .setNodes(node == recoveryNode.getId() ? registeredNodes : recoveryNodes)
                    .build();
            Meta meta = new Meta.Builder()
                    .setControl(control)
                    .setRecver(node)
                    .setTimestamp(timestamp.getAndIncrement())
                    .build();
            send(new Message(meta));
        }
    }

    abstract void connect(Node node);

    private void processBarrierCommand(Message msg) {
        Control ctrl = msg.getMeta().getControl();
        if (!msg.getMeta().isRequest()) {
            PostOffice.get().manage(msg);
            return;
        }

        // 接受来自其他节点的 BARRIER COMMAND 指令
        if (barrierCount == null) {
            barrierCount = new int[8];
        }
        int group = ctrl.getBarrierGroup();
        ++barrierCount[group];

        LOG.info("Barrier count for {} : {}", group, barrierCount[group]);

        if (barrierCount[group] == PostOffice.get().getNodeIds(group).size()) {
            barrierCount[group] = 0;
            Message res = new Message();
            res.getMeta().setRequest(false);
            res.getMeta().setAppId(msg.getMeta().getAppId());
            res.getMeta().setCustomerId(msg.getMeta().getCustomerId());
            res.getMeta().getControl().setCommand(Command.BARRIER);
            for (int r : PostOffice.get().getNodeIds(group)) {
                if (!sharedNodeMapping.containsKey(r)) {
                    res.getMeta().setRecver(r);
                    res.getMeta().setTimestamp(timestamp.getAndIncrement());
                    send(msg);
                }
            }
        }
    }

    private void processHeartbeat(Message message) {
        long ts = System.currentTimeMillis() / 1000;
        for (Node peerNode : message.getMeta().getControl().getNodes()) {
            PostOffice.get().updateHeartbeat(peerNode.getId(), ts);
            if (isScheduler) {
                Control control = new Control.Builder()
                        .setCommand(Command.HEARTBEAT)
                        .setNodes(Collections.singletonList(myNode))
                        .build();
                Meta meta = new Meta.Builder()
                        .setControl(control)
                        .setRecver(peerNode.getId())
                        .setTimestamp(timestamp.getAndIncrement())
                        .build();
                send(new Message(meta));
            }
        }
    }

    public void start(int customerId) {
        heartbeatTimeout = Utils.loadIntegerFromEnvironment(Constants.PS_HEARTBEAT_TIMEOUT,
                Constants.DEFAULT_PS_HEARTBEAT_TIMEOUT);
        startLock.lock();
        try {
            if (initStage == 0) {
                scheduler.setHostname(Utils.loadStringFromEnvironment(Constants.DMLC_PS_ROOT_URI,
                        Constants.DEFAULT_DMLC_PS_ROOT_URI));
                scheduler.setPort(Utils.loadIntegerFromEnvironment(Constants.DMLC_PS_ROOT_PORT,
                        Constants.DEFAULT_DMLC_PS_ROOT_PORT));
                scheduler.setRole(Role.SCHEDULER);
                scheduler.setId(Constants.SCHEDULER);
                isScheduler = PostOffice.get().isScheduler();

                // scheduler node's ip and port is fixed;
                if (isScheduler) {
                    myNode = new Node(scheduler);
                } else {
                    Role role = PostOffice.get().isWorker() ? Role.WORKER : Role.SERVER;
                    String hostname = Utils.loadStringFromEnvironment(Constants.DMLC_NODE_HOST,
                            Constants.DEFAULT_DMLC_NODE_HOST);
                    int port = Integer.parseInt(System.getenv(Constants.DMLC_NODE_PORT));
                    myNode = new Node(role, hostname, port, -1, customerId, false);
                }

                myNode.setPort(bind(myNode, isScheduler ? 0 : 40));
                if (myNode.getPort() != -1) {
                    LOG.info("Bind to {}", myNode.toString());
                } else {
                    LOG.error("Bind failed {}", myNode.toString());
                }

                connect(scheduler);

                es.execute(new Receiving());

                initStage++;
            }
        } finally {
            startLock.unlock();
        }

        // register to scheduler
        if (!isScheduler) {
            Node self = new Node(myNode);
            self.setCustomerId(customerId);
            Control control = new Control.Builder()
                    .setCommand(Command.ADD_NODE)
                    .setNodes(Collections.singletonList(self))
                    .build();
            Meta meta = new Meta.Builder()
                    .setRecver(Constants.SCHEDULER)
                    .setControl(control)
                    .setTimestamp(timestamp.getAndIncrement())
                    .setSender(-1)
                    .build();
            Message msg = new Message(meta);
            LOG.info("Prepare message: {}", msg);
            send(msg);
            LOG.info("Send message succeed");
        }

        // 等待所有节点注册过来
        while (!ready.get()) {
            try {
                Thread.sleep(5000);
                LOG.info("Waiting to be ready ...");
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for ready, {}", e.getMessage());
            }
        }


        startLock.lock();
        try {
            if (initStage == 1) {
                // TODO resender
                // non scheduler node will start heartbeat
                if (!isScheduler) {
                    es.execute(new Heartbeat());
                }
                initStage++;
            }
        } finally {
            startLock.unlock();
        }

        LOG.info("Van launch successful ...");

    }

    public void stop() {
        Meta meta = new Meta(Command.TERMINATE);
        meta.setRecver(myNode.getId());
        meta.setCustomerId(0);
        Message exit = new Message(meta);
        int ret = sendMsg(exit);
    }

    /**
     * process data type message
     *
     * @param msg message
     */
    public void processDataMsg(Message msg) {
        int appId = msg.getMeta().getAppId();
        int customerId = PostOffice.get().isWorker() ? msg.getMeta().getCustomerId() : appId;
        Customer customer = PostOffice.get().getCustomer(appId, customerId, 5);
        customer.accept(msg);
    }

    public int send(Message message) {
        int numBytes = sendMsg(message);
        if (numBytes == -1) {
            LOG.error("Failed to send message: " + message);
        }
        return numBytes;
    }

    protected abstract int sendMsg(Message message);

    protected abstract Message recvMsg();

    protected abstract int bind(Node node, int maxRetry);

    String packMeta(Meta meta) {
        ParameterServerMeta.PBMeta.Builder mBuilder = ParameterServerMeta.PBMeta.newBuilder()
                .setHead(meta.getHead());
        if (meta.getAppId() != -1)
            mBuilder.setAppId(meta.getAppId());
        if (meta.getTimestamp() != -1)
            mBuilder.setTimestamp(meta.getTimestamp());
        if (meta.getBody() != null)
            mBuilder.setBody(ByteString.copyFromUtf8(meta.getBody()));

        mBuilder.setPull(meta.isPull());
        mBuilder.setPush(meta.isPush());
        mBuilder.setRequest(meta.isRequest());
        mBuilder.setSimpleApp(meta.isSimpleApp());
        mBuilder.setPriority(meta.getPriority());
        mBuilder.setCustomerId(meta.getCustomerId());
        if (meta.getDataTypes() != null) {
            meta.getDataTypes().forEach(dataType -> mBuilder.addDataType(dataType.ordinal()));
        }

        ParameterServerMeta.PBControl.Builder cBuilder = ParameterServerMeta.PBControl.newBuilder();
        if (meta.getControl() != null && meta.getControl().getNodes() != null) {
            cBuilder.setCmd(meta.getControl().getCommand().ordinal());
            if (meta.getControl().getCommand() == Command.BARRIER) {
                cBuilder.setBarrierGroup(meta.getControl().getBarrierGroup());
            } else if (meta.getControl().getCommand() == Command.ACK) {
                cBuilder.setMsgSig(meta.getControl().getMsgSig());
            }
            meta.getControl().getNodes().forEach(node -> {
                ParameterServerMeta.PBNode pbNode = ParameterServerMeta.PBNode.newBuilder()
                        .setId(node.getId())
                        .setRole(node.getRole().ordinal())
                        .setPort(node.getPort())
                        .setHostname(node.getHostname())
                        .setIsRecovery(node.isRecovery())
                        .setCustomerId(node.getCustomerId())
                        .build();
                cBuilder.addNode(pbNode);
            });
        }

        mBuilder.setControl(cBuilder.build());

        return mBuilder.build().toString();
    }

    Meta unpackMeta(byte[] buf) {
        ParameterServerMeta.PBMeta.Builder pb = ParameterServerMeta.PBMeta.newBuilder();
        try {
            TextFormat.getParser().merge(new String(buf), pb);
        } catch (TextFormat.ParseException e) {
            LOG.error("Filed to parse meta info from buf: {}", new String(buf));
        }
        Meta meta = new Meta();
        meta.setHead(pb.getHead());
        meta.setAppId(pb.hasAppId() ? pb.getAppId() : -1);
        meta.setTimestamp(pb.hasTimestamp() ? pb.getTimestamp() : -1);
        meta.setRequest(pb.getRequest());
        meta.setPush(pb.getPush());
        meta.setPull(pb.getPull());
        meta.setSimpleApp(pb.getSimpleApp());
        meta.setPriority(pb.getPriority());
        meta.setBody(String.valueOf(pb.getBody()));
        meta.setCustomerId(pb.getCustomerId());
        for (int i = 0; i < pb.getDataTypeCount(); i++) {
            meta.getDataTypes().add(DataType.fromInteger(pb.getDataType(i)));
        }

        if (pb.hasControl()) {
            ParameterServerMeta.PBControl control = pb.getControl();
            meta.getControl().setCommand(Command.fromInteger(control.getCmd()));
            meta.getControl().setBarrierGroup(control.getBarrierGroup());
            meta.getControl().setMsgSig(control.getMsgSig());
            for (int i = 0; i < control.getNodeCount(); i++) {
                ParameterServerMeta.PBNode pbNode = control.getNode(i);
                Node node = new Node(Role.fromInteger(pbNode.getRole()), pbNode.getHostname(), pbNode.getPort(),
                        pbNode.hasId() ? pbNode.getId() : -1, pbNode.getCustomerId(), pbNode.getIsRecovery());
                meta.getControl().getNodes().add(node);
            }
        } else {
            meta.getControl().setCommand(Command.EMPTY);
        }
        return meta;
    }

    int getNodeID(byte[] buffer) {
        int id = -1;
        if (buffer[0] == 'p' && buffer[1] == 's') {
            try {
                id = Integer.parseInt(new String(buffer).substring(2));
            } catch (NumberFormatException e) {
                LOG.error("Failed to parse node id from buf: ", e);
            }
        }
        return id;
    }

    public ZContext getContext() {
        return context;
    }

    public void setContext(ZContext context) {
        this.context = context;
    }

    public Node getMyNode() {
        return myNode;
    }

    public boolean isReady() {
        return ready.get();
    }

    public void updateLocalID(Message message, Set<Integer> deadNodes, List<Node> registeredNodes, Meta recoveryNodes) {
        List<Node> nodes = message.getMeta().getControl().getNodes();
        int expectedSize = PostOffice.get().getNumServers() + PostOffice.get().getNumWorkers();
        if (message.getMeta().getSender() == -1) {
            if (!isScheduler || nodes.size() != 1) {
                LOG.error("Current node: {}, control nodes: {}", myNode, nodes);
            }
            if (registeredNodes.size() < expectedSize) {
                registeredNodes.add(nodes.get(0));
            }
        }

        // Update my id
        for (Node node : nodes) {
            if (myNode.equals(node) && myNode.getId() == -1) {
                this.myNode = new Node(node);
            }
        }
    }

    public void processAddNodeCommand(Message message, List<Node> registeredNodes, Meta recoveryNodes) {
        Set<Integer> deadNodes = new HashSet<>(PostOffice.get().getDeadNodes(heartbeatTimeout));

        updateLocalID(message, deadNodes, registeredNodes, recoveryNodes);

        if (isScheduler) {
            processAddNodeCommandAtScheduler(message, registeredNodes, recoveryNodes);
            return;
        }

        for (Node node : message.getMeta().getControl().getNodes()) {
            String address = node.getHostname() + ":" + node.getPort();
            if (!connectedNodes.containsKey(address)) {
                connect(node);
                connectedNodes.put(address, node.getId());
            }
            if (!node.isRecovery() && node.getRole() == Role.SERVER)
                numServers++;
            if (!node.isRecovery() && node.getRole() == Role.WORKER)
                numWorkers++;
        }

        LOG.info("{} is ready", myNode.toString());
        // 非 SCHEDULER 节点就绪
        ready.set(true);
    }

    public int getNextTimestamp() {
        return timestamp.getAndIncrement();
    }

    public int getTimestamp() {
        return timestamp.get();
    }

    public class Receiving implements Runnable {

        @Override
        public void run() {
            List<Node> registeredNodes = new ArrayList<>();
            Meta recoveryNodes = new Meta();
            recoveryNodes.getControl().setCommand(Command.ADD_NODE);

            while (true) {
                Message message = recvMsg();
                Control control = message.getMeta().getControl();
                // DATA
                if (control.getNodes().isEmpty()) {
                    LOG.info("Recv data [non-control], message: {}", message);
                    processDataMsg(message);
                    continue;
                }
                // CONTROL
                LOG.info("Recv command: {}, nodes: {}", control.getCommand(), control.getNodes());
                switch (control.getCommand()) {
                    case TERMINATE:
                        processTerminateCommand();
                        break;
                    case ADD_NODE:
                        processAddNodeCommand(message, registeredNodes, recoveryNodes);
                        break;
                    case BARRIER:
                        processBarrierCommand(message);
                        break;
                    case HEARTBEAT:
                        processHeartbeat(message);
                        break;
                    default:
                        LOG.error("Unknown command: {}", message);
                }
                if (control.getCommand() == Command.TERMINATE) break;
            }
        }
    }

    public class Heartbeat implements Runnable {

        @Override
        public void run() {
            int interval = Utils.loadIntegerFromEnvironment(Constants.PS_HEARTBEAT_INTERVAL,
                    Constants.DEFAULT_PS_HEARTBEAT_INTERVAL);
            while (interval > 0 && isReady()) {
                try {
                    LOG.info("Sleep {} seconds for heartbeat", interval);
                    Thread.sleep(interval * 1000L);
                } catch (InterruptedException e) {
                    LOG.info("Interrupted while sleep in heartbeat thread : {}", e.getMessage());
                }
                Control control = new Control.Builder()
                        .setCommand(Command.HEARTBEAT)
                        .setNodes(Collections.singletonList(myNode))
                        .build();
                Meta meta = new Meta.Builder()
                        .setRecver(Constants.SCHEDULER)
                        .setControl(control)
                        .setTimestamp(timestamp.getAndIncrement())
                        .build();
                send(new Message(meta));
            }
        }
    }
}
