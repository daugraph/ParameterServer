package cn.daugraph.ps.core;

public class Node {

    private Role role;
    private Integer id;
    private String hostname;
    private Integer port;
    private Integer customerId;
    private boolean isRecovery;

    public Node(Role role, String hostname, int port, Integer id, Integer customerId, Boolean isRecovery) {
        this.hostname = hostname;
        this.port = port;
        this.role = role;
        this.id = id;
        this.customerId = customerId;
        this.isRecovery = isRecovery;
    }

    public Node(Node other) {
        this.hostname = other.hostname;
        this.port = other.port;
        this.role = other.role;
        this.id = other.id;
        this.customerId = other.customerId;
        this.isRecovery = other.isRecovery;
    }

    @Override
    public String toString() {
        return "Node{" + "role=" + role + ", id=" + id + ", hostname='" + hostname + '\'' + ", port=" + port
                + ", customerId=" + customerId + ", isRecovery=" + isRecovery + '}';
    }

    public boolean isRecovery() {
        return isRecovery;
    }

    public void setRecovery(boolean recovery) {
        isRecovery = recovery;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

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
}
