package cn.daugraph.ps.core;

public enum Role {
    SERVER, WORKER, SCHEDULER;

    public static Role fromInteger(int x) {
        switch (x) {
            case 0:
                return SERVER;
            case 1:
                return WORKER;
            case 2:
                return SCHEDULER;
        }
        return null;
    }
}
