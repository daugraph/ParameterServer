package cn.daugraph.ps.core.common;

public final class Constants {
    public static final String DMLC_PS_VAN_TYPE = "DMLC_PS_VAN_TYPE";
    public static final String DEFAULT_DMLC_PS_VAN_TYPE = "ZMQ";

    public static final String PS_HEARTBEAT_TIMEOUT = "PS_HEARTBEAT_TIMEOUT";
    public static final Integer DEFAULT_PS_HEARTBEAT_TIMEOUT = 1200;

    public static final String PS_HEARTBEAT_INTERVAL = "PS_HEARTBEAT_INTERVAL";
    public static final Integer DEFAULT_PS_HEARTBEAT_INTERVAL = 6000;

    public static final String DMLC_NUM_WORKER = "DMLC_NUM_WORKER";
    public static final Integer DEFAULT_DMLC_NUM_WORKER = 1;

    public static final String DMLC_NUM_SERVER = "DMLC_NUM_SERVER";
    public static final Integer DEFAULT_DMLC_NUM_SERVER = 0;

    public static final String DMLC_ROLE = "DMLC_ROLE";
    public static final String DEFAULT_DMLC_ROLE = "scheduler";

    public static final String DMLC_PS_ROOT_URI = "DMLC_PS_ROOT_URI";
    public static final String DEFAULT_DMLC_PS_ROOT_URI = "localhost";

    public static final String DMLC_PS_ROOT_PORT = "DMLC_PS_ROOT_PORT";
    public static final Integer DEFAULT_DMLC_PS_ROOT_PORT = 20000;

    public static final String DMLC_NODE_HOST = "DMLC_NODE_HOST";
    public static final String DEFAULT_DMLC_NODE_HOST = "localhost";

    public static final String DMLC_NODE_PORT = "DMLC_NODE_PORT";
    public static final Integer DEFAULT_DMLC_NODE_PORT = 20010;

    public static final Integer SCHEDULER = 1;

    // 001
    public static final Integer GROUP_SCHEDULER = 1;
    // 010
    public static final Integer GROUP_SERVER = 2;
    // 011
    public static final Integer GROUP_SERVER_SCHEDULER = 3;
    // 100
    public static final Integer GROUP_WORKER = 4;
    // 101
    public static final Integer GROUP_WORKER_SCHEDULER = 5;
    // 110
    public static final Integer GROUP_WORKER_SERVER = 6;
    // 111
    public static final Integer GROUP_ALL = 7;
}
