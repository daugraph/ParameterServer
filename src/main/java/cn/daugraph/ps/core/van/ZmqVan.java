package cn.daugraph.ps.core.van;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import cn.daugraph.ps.core.Message;
import cn.daugraph.ps.core.Meta;
import cn.daugraph.ps.core.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import zmq.Msg;
import static zmq.ZMQ.ZMQ_SNDMORE;

public class ZmqVan extends Van {

    private final Logger LOG = LoggerFactory.getLogger(ZmqVan.class);
    private final HashMap<Integer, ZMQ.Socket> senders = new HashMap<>();
    private ZMQ.Context context;
    private ZMQ.Socket receiver;

    @Override
    public void start(int customerId) {
        startLock.lock();
        try {
            if (context == null) {
                context = ZMQ.context(1);
                context.setMaxSockets(65536);
            }
        } finally {
            startLock.unlock();
        }
        super.start(customerId);
    }

    @Override
    public void stop() {
        super.stop();

        receiver.setLinger(0);
        receiver.close();

        senders.forEach((id, sender) -> {
            sender.setLinger(0);
            sender.close();
        });

        senders.clear();
        context.close();
    }

    @Override
    public int bind(Node node, int maxRetry) {
        receiver = context.socket(SocketType.ROUTER);
        boolean isLocal = Boolean.parseBoolean(System.getenv("DMLC_LOCAL"));
        String hostname = node.getHostname() == null ? "*" : node.getHostname();
        String addr = isLocal ? "ipc:///tmp/" : "tcp://" + hostname + ":";

        int port = node.getPort();
        Random random = new Random();
        random.setSeed(new Date().getTime());
        for (int i = 0; i < maxRetry + 1; ++i) {
            String address = addr + port;
            if (receiver.bind(address))
                break;
            if (i == maxRetry) {
                port = -1;
            } else {
                port = 10000 + random.nextInt() % 40000;
            }
        }

        return port;
    }

    @Override
    public void connect(Node node) {
        int id = node.getId();
        if (senders.containsKey(id)) {
            senders.get(id).close();
        }
        // worker doesn't need to connect to the other workers. same for server
        if ((node.getRole() == getMyNode().getRole()) && (!node.getId().equals(getMyNode().getId()))) {
            return;
        }

        ZMQ.Socket sender = context.socket(SocketType.DEALER);
        if (getMyNode().getId() != null) {
            String myId = "ps" + getMyNode().getId();
            sender.setIdentity(myId.getBytes());
            String watermark = System.getenv("DMLC_PS_WATER_MARK");
            if (watermark != null) {
                int hwm = Integer.parseInt(watermark);
                sender.setSndHWM(hwm);
            }
        }

        // connect
        String addr = "tcp://" + node.getHostname() + ":" + node.getPort();
        if (System.getenv("DMLC_LOCAL") != null) {
            addr = "ipc:///tmp/" + node.getPort();
        }

        if (!sender.connect(addr)) {
            LOG.error("Failed to connect address: " + addr);
        }
        senders.put(id, sender);
    }

    @Override
    public synchronized int sendMsg(Message message) {
        int id = message.getMeta().getRecver();
        ZMQ.Socket socket = senders.get(id);
        String meta = packMeta(message.getMeta());
        int tag = ZMQ_SNDMORE;
        if (message.getData().size() == 0) {
            tag = 0;
        }
        if (!socket.send(meta, tag)) {
            LOG.error("Failed to send meta info: {}", meta);
            return -1;
        }

        int ret = meta.getBytes().length;
        for (byte[] feat : message.getData()) {
            if (!socket.send(feat, tag)) {
                LOG.error("Failed to send data: {}", feat);
                return -1;
            }
            ret += feat.length;
        }

        return ret;
    }

    @Override
    public synchronized int recvMsg(Message message) {
        byte[] buf;
        int ret = 0;
        for (int i = 0; ; i++) {
            Msg msg = receiver.base().recv(0);
            buf = msg.data();
            ret += buf.length;
            if (i == 0) {
                Integer id = getNodeID(new String(buf));
                message.getMeta().setSender(id);
                message.getMeta().setRecver(getMyNode().getId());
            } else if (i == 1) {
                Meta meta = unpackMeta(buf);
                message.setMeta(meta);
                if (!msg.hasMore())
                    break;
            } else {
                message.getData().add(buf);
                if (!msg.hasMore())
                    break;
            }
        }
        return ret;
    }
}
