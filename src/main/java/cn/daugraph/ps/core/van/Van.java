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
import cn.daugraph.ps.proto.java.ParameterServerMeta;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
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
    protected Lock startLock = new ReentrantLock();
    protected Node myNode;
    private ExecutorService es;
    private int heartbeatTimeout;
    private ZContext context;
    private HashMap<Integer, ZMQ.Socket> senders;
    private AtomicBoolean ready = new AtomicBoolean(false);
    private int numServers;
    private int numWorkers;
    private int[] barrierCount;
    private int initStage = 0;
    private boolean isScheduler;
    private int recvBytes = 0;

    public void processTerminateCommand() {
        LOG.info("{} is stopped", myNode);
        ready.set(false);
    }

    public void processAddNodeCommandAtScheduler(Message msg, Meta nodes, Meta recoveryNodes) {
        LOG.info("Call processAddNodeCommandAtScheduler:");
        LOG.info("msg: {}", msg);
        LOG.info("nodes: {}", nodes);
        LOG.info("recoveryNodes: {}", recoveryNodes);

        recoveryNodes.getControl().setCommand(Command.ADD_NODE);
        long t = System.currentTimeMillis() / 1000;

        int numNodes = PostOffice.get().getNumServers() + PostOffice.get().getNumWorkers();
        List<Node> registeredNodes = nodes.getControl().getNodes();
        // 收到全部节点注册后，给节点分配 node id
        if (registeredNodes.size() == numNodes) {
            Collections.sort(registeredNodes);
            for (Node registeredNode : registeredNodes) {
                String address = registeredNode.getAddress();
                int allocatedNodeId = registeredNode.getRole() == Role.SERVER
                        ? PostOffice.get().serverRankToID(numServers)
                        : PostOffice.get().workerRankToID(numWorkers);
                if (!connectedNodes.containsKey(address)) {
                    registeredNode.setId(allocatedNodeId);
                    connect(registeredNode);
                    PostOffice.get().updateHeartbeat(registeredNode.getId(), t);
                    // Mapping (address -> node id)
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

            // 把当前节点(也就是 scheduler 节点)加入已注册节点集合中
            registeredNodes.add(myNode);
            nodes.getControl().setCommand(Command.ADD_NODE);

            Message response = new Message();
            response.setMeta(nodes);
            // 给所有 worker + server 节点回复
            for (int r : PostOffice.get().getNodeIds(Constants.SERVER_WORKER_GROUP)) {
                if (!sharedNodeMapping.containsKey(r)) {
                    response.getMeta().setRecver(r);
                    response.getMeta().setTimestamp(timestamp.getAndAdd(1));
                    send(response);
                }
            }
            LOG.info("The scheduler is connected to {} workers and {} servers", numWorkers, numServers);
            ready.set(true);
        } else if (!recoveryNodes.getControl().getNodes().isEmpty()) {
            Set<Integer> deadNodes = new HashSet<>(PostOffice.get().getDeadNodes(heartbeatTimeout));
            List<Node> rNodes = recoveryNodes.getControl().getNodes();
            if (rNodes.size() == 1) {
                connect(rNodes.get(0));
                PostOffice.get().updateHeartbeat(rNodes.get(0).getId(), t);
                for (int r : PostOffice.get().getNodeIds(Constants.WORKER_GROUP + Constants.SERVER_GROUP)) {
                    if (r != rNodes.get(0).getId() && deadNodes.contains(r)) {
                        continue;
                    }
                    Message back = new Message();
                    back.setMeta(r == rNodes.get(0).getId() ? nodes : recoveryNodes);
                    back.getMeta().setRecver(r);
                    back.getMeta().setTimestamp(timestamp.getAndIncrement());
                    send(back);
                }
            }
        }
    }

    abstract void connect(Node node);

    private void processBarrierCommand(Message msg) {
        Control ctrl = msg.getMeta().getControl();
        if (msg.getMeta().isRequest()) {
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
        } else {
            PostOffice.get().manage(msg);
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
        heartbeatTimeout = Integer.parseInt(System.getenv(Constants.PS_HEARTBEAT_TIMEOUT));
        ExecutorService es = Executors.newFixedThreadPool(2);
        startLock.lock();
        try {
            if (initStage == 0) {
                scheduler.setHostname(System.getenv(Constants.DMLC_PS_ROOT_URI));
                scheduler.setPort(Integer.parseInt(System.getenv(Constants.DMLC_PS_ROOT_PORT)));
                scheduler.setRole(Role.SCHEDULER);
                scheduler.setId(Constants.SCHEDULER);
                isScheduler = PostOffice.get().isScheduler();

                // scheduler node's ip and port is fixed;
                if (isScheduler) {
                    myNode = new Node(scheduler);
                } else {
                    Role role = PostOffice.get().isWorker() ? Role.WORKER : Role.SERVER;
                    String hostname = System.getenv(Constants.DMLC_NODE_HOST);
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
            LOG.info("Prepare send message: {}", msg);
            send(msg);
            LOG.info("After send message: {}", msg);
        }

        // busing wait ready
        while (!ready.get()) {
            try {
                Thread.sleep(5000);
                LOG.info("{} sleep five seconds", myNode);
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
        LOG.info("Packing meta: {}", meta);
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

    int getNodeID(String buf) {
        LOG.info("buf: {}", buf);
        int id = -1;
        if (buf.startsWith("ps")) {
            try {
                id = Integer.parseInt(buf.substring(2));
            } catch (NumberFormatException e) {
                LOG.error("Failed to parse node id from buf: ", e);
            }
        }
        return id;
    }

    int recv(Message msg) {
        return 0;
    }

    public ZContext getContext() {
        return context;
    }

    public void setContext(ZContext context) {
        this.context = context;
    }

    public Node getScheduler() {
        return scheduler;
    }

    public Node getMyNode() {
        return myNode;
    }

    public void setMyNode(Node myNode) {
        this.myNode = myNode;
    }

    public boolean isReady() {
        return ready.get();
    }

    public void setReady(AtomicBoolean ready) {
        this.ready = ready;
    }

    public HashMap<Integer, ZMQ.Socket> getSenders() {
        return senders;
    }

    public void setSenders(HashMap<Integer, ZMQ.Socket> senders) {
        this.senders = senders;
    }

    public void updateLocalID(Message msg, Set<Integer> deadNodes, Meta nodes, Meta recoveryNodes) {
        Control control = msg.getMeta().getControl();
        int numNodes = PostOffice.get().getNumServers() + PostOffice.get().getNumServers();

        // sender id in message is -1 when recv register message from node, so we need to assign ID to node
        if (msg.getMeta().getSender() == -1) {
            if (!isScheduler || control.getNodes().size() != 1) {
                LOG.error("Current node: {}, control nodes: {}", myNode, control.getNodes());
            }
            if (nodes.getControl().getNodes().size() < numNodes) {
                nodes.getControl().getNodes().add(control.getNodes().get(0));
            } else {
                // handle restart nodes
            }
        }

        // update my id
        for (int i = 0; i < control.getNodes().size(); i++) {
            Node node = control.getNodes().get(i);
            if (myNode.getHostname() == node.getHostname() && myNode.getPort() == myNode.getPort()) {
                if (myNode.getId() == -1) {
                    this.myNode = new Node(node);
                }
            }
        }
    }

    public void processAddNodeCommand(Message msg, Meta nodes, Meta recoveryNodes) {
        Set<Integer> deadNodes = new HashSet<>(PostOffice.get().getDeadNodes(heartbeatTimeout));
        updateLocalID(msg, deadNodes, nodes, recoveryNodes);
        Control ctrl = msg.getMeta().getControl();

        if (isScheduler) {
            processAddNodeCommandAtScheduler(msg, nodes, recoveryNodes);
        } else {
            for (Node node : ctrl.getNodes()) {
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
            LOG.info("{} is connected to others", myNode.toString());
            ready.set(true);
        }
    }

    public int getNextTimestamp() {
        return timestamp.getAndIncrement();
    }

    public class Receiving implements Runnable {

        @Override
        public void run() {
            Meta nodes = new Meta();
            Meta recoveryNodes = new Meta();
            recoveryNodes.getControl().setCommand(Command.ADD_NODE);

            while (true) {
                LOG.info("{} before recv message", myNode);
                Message msg = recvMsg();
                LOG.info("{} after recv message", myNode);
                recvBytes += msg.getData().size();

                if (!msg.getMeta().getControl().getNodes().isEmpty()) {
                    Control ctrl = msg.getMeta().getControl();
                    if (ctrl.getCommand() == Command.TERMINATE) {
                        processTerminateCommand();
                        break;
                    } else if (ctrl.getCommand() == Command.ADD_NODE) {
                        LOG.info("{} recv message ADD_NODE", myNode);
                        processAddNodeCommand(msg, nodes, recoveryNodes);
                    } else if (ctrl.getCommand() == Command.HEARTBEAT) {
                        processHeartbeat(msg);
                    } else if (ctrl.getCommand() == Command.BARRIER) {
                        processBarrierCommand(msg);
                    } else {
                        LOG.error("Unknown type message: {}", msg);
                    }
                } else {
                    processDataMsg(msg);
                }
            }
        }
    }

    public class Heartbeat implements Runnable {

        @Override
        public void run() {
            int interval = Integer.parseInt(System.getenv(Constants.PS_HEARTBEAT_INTERVAL));
            while (interval > 0 && isReady()) {
                try {
                    Thread.sleep(interval);
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
