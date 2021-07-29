package cn.daugraph.ps.core;

public enum Command {
    EMPTY, TERMINATE, ADD_NODE, BARRIER, ACK, HEARTBEAT;

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