package cn.daugraph.ps.core;

import java.util.ArrayList;
import java.util.List;

public final class Control {

    private final List<Node> nodes;
    private Command command;
    private int barrierGroup;
    private long msgSig;

    public Control() {
        this.command = Command.EMPTY;
        this.nodes = new ArrayList<>();
    }

    public Control(Command command, int barrierGroup, List<Node> nodes, long msgSig) {
        this.command = command;
        this.barrierGroup = barrierGroup;
        this.nodes = nodes;
        this.msgSig = msgSig;
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
        return "Control{" + "command=" + command + ", nodes=" + nodes + ", barrierGroup=" + barrierGroup + ", msgSig="
                + msgSig + '}';
    }

    boolean isEmpty() {
        return command == Command.EMPTY;
    }


    public static class Builder {
        private Command command;
        private List<Node> nodes;
        private int barrierGroup;
        private long msgSig;

        public Builder setCommand(Command command) {
            this.command = command;
            return this;
        }

        public Builder setNodes(List<Node> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setBarrierGroup(int barrierGroup) {
            this.barrierGroup = barrierGroup;
            return this;
        }

        public Builder setMsgSig(long msgSig) {
            this.msgSig = msgSig;
            return this;
        }

        public Control build() {
            return new Control(command, barrierGroup, nodes, msgSig);
        }

    }
}
