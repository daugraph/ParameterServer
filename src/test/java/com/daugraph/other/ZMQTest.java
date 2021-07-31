package com.daugraph.other;

import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import zmq.Msg;

public class ZMQTest {
    /**
     *  可以使用 Python 客户端链接进行测试：https://zeromq.org/languages/python/
     *  import zmq
     *  context = zmq.Context()
     *  socket = context.socket(zmq.REQ)
     *  socket.connect("tcp://localhost:5555")
     *  socket.send(b"Hello")
     *  print(socket.recv())
     */
    @Test
    public void testZmqEchoServer() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:5555");
            while (!Thread.currentThread().isInterrupted()) {
                byte[] reply = socket.recv(0);
                socket.send(reply, 0);
            }
        }
    }

    @Test
    public void testRouterServer() {
        ZMQ.Context context = ZMQ.context(1);
        context.setMaxSockets(65536);
        ZMQ.Socket socket = context.socket(SocketType.ROUTER);
        socket.bind("tcp://*:5555");

        while (!Thread.currentThread().isInterrupted()) {
            Msg zmqMsg = socket.base().recv(0);
            System.out.println(zmqMsg);
            System.out.println(new String(zmqMsg.data()));
        }
    }

    @Test
    public void testDealerClient() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sender = context.socket(SocketType.DEALER);
        String identity = "ps1";
        sender.setIdentity(identity.getBytes());
        sender.connect("tcp://localhost:5555");
        sender.send("hello \nworld");
    }

}
