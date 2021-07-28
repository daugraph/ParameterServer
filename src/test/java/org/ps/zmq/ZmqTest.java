package org.ps.zmq;

import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ZmqTest {
    /**
     *  使用 Python 客户端链接进行测试：https://zeromq.org/languages/python/
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
}
