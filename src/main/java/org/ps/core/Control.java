package org.ps.core;

import java.util.ArrayList;
import java.util.List;

public final class Control {

    private Command command;
    private List<Node> nodes;
    private int barrierGroup;
    private long msgSig;

    public Control() {
        this.command = Command.EMPTY;
        this.nodes = new ArrayList<>();
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public int getBarrierGroup() {
        return barrierGroup;
    }

    public void setBarrierGroup(int barrierGroup) {
        this.barrierGroup = barrierGroup;
    }

    public long getMsgSig() {
        return msgSig;
    }

    public void setMsgSig(long msgSig) {
        this.msgSig = msgSig;
    }

    @Override
    public String toString() {
        return "Control{"
                + "command="
                + command
                + ", nodes="
                + nodes
                + ", barrierGroup="
                + barrierGroup
                + ", msgSig="
                + msgSig
                + '}';
    }

    boolean isEmpty() {
        return command == Command.EMPTY;
    }

    public enum Command {
        EMPTY,
        TERMINATE,
        ADD_NODE,
        BARRIER,
        ACK,
        HEARTBEAT;

        public static Command fromInteger(int x) {
            switch (x) {
                case 0:
                    return EMPTY;
                case 1:
                    return TERMINATE;
                case 2:
                    return ADD_NODE;
                case 3:
                    return BARRIER;
                case 4:
                    return ACK;
                case 5:
                    return HEARTBEAT;
            }
            return null;
        }
    }
}
