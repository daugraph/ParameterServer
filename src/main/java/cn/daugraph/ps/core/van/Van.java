package cn.daugraph.ps.core.van;

import cn.daugraph.ps.core.Control;
import cn.daugraph.ps.core.Customer;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
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
import cn.daugraph.ps.core.DataType;
import cn.daugraph.ps.core.Message;
import cn.daugraph.ps.core.Meta;
import cn.daugraph.ps.core.Node;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.common.Consts;
import com.daugraph.proto.java.ParameterServerMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public abstract class Van {

    public static final Logger LOG = LoggerFactory.getLogger(Van.class);

    private final ExecutorService es = Executors.newFixedThreadPool(2);
    private final int heartbeatTimeout = Integer.parseInt(System.getenv(Consts.PS_HEARTBEAT_TIMEOUT));
    private final AtomicInteger sendBytes = new AtomicInteger(0);
    private final AtomicInteger timestamp = new AtomicInteger(0);
    private final int dropRate = 0;
    protected Lock startLock = new ReentrantLock();
    private ZContext context;
    private Node scheduler;
    private Node myNode;
    private HashMap<Integer, ZMQ.Socket> senders;
    private HashMap<String, Integer> connectedNodes;
    private HashMap<Integer, Integer> sharedNodeMapping;
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
        recoveryNodes.getControl().setCommand(Control.Command.ADD_NODE);
        long t = System.currentTimeMillis() / 1000;

        int numNodes = PostOffice.getInstance().getNumServers() + PostOffice.getInstance().getNumWorkers();
        List<Node> cNodes = nodes.getControl().getNodes();
        if (cNodes.size() == numNodes) {
            cNodes.sort((a, b) -> {
                if (a.getHostname().equals(b.getHostname()))
                    return a.getPort() - b.getPort();
                return a.getHostname().compareTo(b.getHostname());
            });

            for (Node node : cNodes) {
                String address = node.getHostname() + ":" + node.getPort();
                int id = node.getRole() == Node.Role.SERVER ? PostOffice.getInstance().serverRankToID(numServers)
                        : PostOffice.getInstance().workerRankToID(numWorkers);
                if (!connectedNodes.containsKey(address)) {
                    node.setId(id);
                    connect(node);
                    PostOffice.getInstance().updateHeartbeat(node.getId(), t);
                    connectedNodes.put(address, node.getId());
                } else {
                    sharedNodeMapping.put(id, connectedNodes.get(address));
                    node.setId(connectedNodes.get(address));
                }
                if (node.getRole() == Node.Role.SERVER)
                    numServers++;
                if (node.getRole() == Node.Role.WORKER)
                    numWorkers++;
            }

            cNodes.add(myNode);
            nodes.getControl().setCommand(Control.Command.ADD_NODE);

            Message back = new Message();
            back.setMeta(nodes);
            for (int r : PostOffice.getInstance().getNodeIds(Consts.WORKER_GROUP + Consts.SERVER_GROUP)) {
                if (!sharedNodeMapping.containsKey(r)) {
                    back.getMeta().setRecver(r);
                    back.getMeta().setTimestamp(timestamp.getAndAdd(1));
                    send(back);
                }
            }
            LOG.info("The scheduler is connected to {} workers and {} servers", numWorkers, numServers);
            ready.set(true);
        } else if (!recoveryNodes.getControl().getNodes().isEmpty()) {
            Set<Integer> deadNodes = new HashSet<>(PostOffice.getInstance().getDeadNodes(heartbeatTimeout));
            List<Node> rNodes = recoveryNodes.getControl().getNodes();
            if (rNodes.size() == 1) {
                connect(rNodes.get(0));
                PostOffice.getInstance().updateHeartbeat(rNodes.get(0).getId(), t);
                for (int r : PostOffice.getInstance().getNodeIds(Consts.WORKER_GROUP + Consts.SERVER_GROUP)) {
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

            if (barrierCount[group] == PostOffice.getInstance().getNodeIds(group).size()) {
                barrierCount[group] = 0;
                Message res = new Message();
                res.getMeta().setRequest(false);
                res.getMeta().setAppId(msg.getMeta().getAppId());
                res.getMeta().setCustomerId(msg.getMeta().getCustomerId());
                res.getMeta().getControl().setCommand(Control.Command.BARRIER);
                for (int r : PostOffice.getInstance().getNodeIds(group)) {
                    if (!sharedNodeMapping.containsKey(r)) {
                        res.getMeta().setRecver(r);
                        res.getMeta().setTimestamp(timestamp.getAndIncrement());
                        send(msg);
                    }
                }
            }
        } else {
            PostOffice.getInstance().manage(msg);
        }
    }

    private void processHeartbeat(Message msg) {
        Control ctrl = msg.getMeta().getControl();
        long t = System.currentTimeMillis() / 1000;
        for (Node node : ctrl.getNodes()) {
            PostOffice.getInstance().updateHeartbeat(node.getId(), t);
            if (isScheduler) {
                Message ack = new Message();
                ack.getMeta().setRecver(node.getId());
                ack.getMeta().getControl().setCommand(Control.Command.HEARTBEAT);
                ack.getMeta().getControl().getNodes().add(myNode);
                ack.getMeta().setTimestamp(timestamp.getAndIncrement());
                send(ack);
            }
        }
    }

    public void start(int customerId) {
        startLock.lock();
        try {
            if (initStage == 0) {
                scheduler.setHostname(System.getenv(Consts.DMLC_PS_ROOT_URI));
                scheduler.setPort(Integer.parseInt(System.getenv(Consts.DMLC_PS_ROOT_PORT)));
                scheduler.setRole(Node.Role.SCHEDULER);
                scheduler.setId(Consts.SCHEDULER);
                isScheduler = PostOffice.getInstance().isScheduler();

                // scheduler node's ip and port is fixed;
                if (isScheduler) {
                    myNode = new Node(scheduler);
                } else {
                    Node.Role role = PostOffice.getInstance().isWorker() ? Node.Role.WORKER : Node.Role.SERVER;
                    String hostname = System.getenv(Consts.DMLC_NODE_HOST);
                    int port = Integer.parseInt(System.getenv(Consts.PORT));
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

        // busing wait ready
        while (!ready.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for ready, {}", e.getMessage());
            }
        }

        startLock.lock();
        try {
            if (initStage == 1) {
                // TODO resender
                // non-scheduler will start heartbeat
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
        Meta meta = new Meta(Control.Command.TERMINATE);
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
        int customerId = PostOffice.getInstance().isWorker() ? msg.getMeta().getCustomerId() : appId;
        Customer customer = PostOffice.getInstance().getCustomer(appId, customerId, 5);
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

    protected abstract int recvMsg(Message message);

    protected abstract int bind(Node node, int maxRetry);

    String packMeta(Meta meta) {
        ParameterServerMeta.PBMeta.Builder mBuilder = ParameterServerMeta.PBMeta.newBuilder();
        mBuilder.setHead(meta.getHead());
        if (meta.getAppId() != -1)
            mBuilder.setAppId(meta.getAppId());
        if (meta.getTimestamp() != -1)
            mBuilder.setTimestamp(meta.getTimestamp());
        if (meta.getBody().length() > 0)
            mBuilder.setBody(ByteString.copyFromUtf8(meta.getBody()));

        mBuilder.setPull(meta.isPull());
        mBuilder.setPush(meta.isPush());
        mBuilder.setRequest(meta.isRequest());
        mBuilder.setSimpleApp(meta.isSimpleApp());
        mBuilder.setPriority(meta.getPriority());
        mBuilder.setCustomerId(meta.getCustomerId());
        meta.getDataTypes().forEach(dataType -> mBuilder.addDataType(dataType.ordinal()));

        ParameterServerMeta.PBControl.Builder cBuilder = ParameterServerMeta.PBControl.newBuilder();
        cBuilder.setCmd(meta.getControl().getCommand().ordinal());
        if (meta.getControl().getCommand() == Control.Command.BARRIER) {
            cBuilder.setBarrierGroup(meta.getControl().getBarrierGroup());
        } else if (meta.getControl().getCommand() == Control.Command.ACK) {
            cBuilder.setMsgSig(meta.getControl().getMsgSig());
        }

        meta.getControl().getNodes().forEach(node -> {
            ParameterServerMeta.PBNode pbNode = ParameterServerMeta.PBNode.newBuilder().setId(node.getId())
                    .setRole(node.getRole().ordinal()).setPort(node.getPort()).setHostname(node.getHostname())
                    .setIsRecovery(node.isRecovery()).setCustomerId(node.getCustomerId()).build();
            cBuilder.addNode(pbNode);
        });

        mBuilder.setControl(cBuilder.build());

        return mBuilder.toString();
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
            meta.getControl().setCommand(Control.Command.fromInteger(control.getCmd()));
            meta.getControl().setBarrierGroup(control.getBarrierGroup());
            meta.getControl().setMsgSig(control.getMsgSig());
            for (int i = 0; i < control.getNodeCount(); i++) {
                ParameterServerMeta.PBNode pbNode = control.getNode(i);
                Node node = new Node(Node.Role.fromInteger(pbNode.getRole()), pbNode.getHostname(), pbNode.getPort(),
                        pbNode.hasId() ? pbNode.getId() : -1, pbNode.getCustomerId(), pbNode.getIsRecovery());
                meta.getControl().getNodes().add(node);
            }
        } else {
            meta.getControl().setCommand(Control.Command.EMPTY);
        }
        return meta;
    }

    Integer getNodeID(String buf) {
        Integer id = null;
        if (buf.startsWith("ps")) {
            try {
                id = Integer.valueOf(buf.substring(2));
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
    }

    public void processAddNodeCommand(Message msg, Meta nodes, Meta recoveryNodes) {
        Set<Integer> deadNodes = new HashSet<>(PostOffice.getInstance().getDeadNodes(heartbeatTimeout));
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
                if (!node.isRecovery() && node.getRole() == Node.Role.SERVER)
                    numServers++;
                if (!node.isRecovery() && node.getRole() == Node.Role.WORKER)
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
            recoveryNodes.getControl().setCommand(Control.Command.ADD_NODE);

            while (true) {
                Message msg = new Message();
                int bytes = recvMsg(msg);
                recvBytes += bytes;

                if (!msg.getMeta().getControl().getNodes().isEmpty()) {
                    Control ctrl = msg.getMeta().getControl();
                    if (ctrl.getCommand() == Control.Command.TERMINATE) {
                        processTerminateCommand();
                        break;
                    } else if (ctrl.getCommand() == Control.Command.ADD_NODE) {
                        processAddNodeCommand(msg, nodes, recoveryNodes);
                    } else if (ctrl.getCommand() == Control.Command.HEARTBEAT) {
                        processHeartbeat(msg);
                    } else if (ctrl.getCommand() == Control.Command.BARRIER) {
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
            int interval = Integer.parseInt(System.getenv(Consts.PS_HEARTBEAT_INTERVAL));
            while (interval > 0 && isReady()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    LOG.info("Interrupted while sleep in heartbeat thread : {}", e.getMessage());
                }
                Message msg = new Message();
                msg.getMeta().setRecver(Consts.SCHEDULER);
                msg.getMeta().getControl().setCommand(Control.Command.HEARTBEAT);
                msg.getMeta().getControl().getNodes().add(myNode);
                msg.getMeta().setTimestamp(timestamp.getAndIncrement());
                send(msg);
            }
        }
    }
}
