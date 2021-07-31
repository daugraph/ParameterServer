package cn.daugraph.ps.core;

public class Node implements Comparable<Node> {

    private Role role;
    private int id;
    private String hostname;
    private int port;
    private int customerId;
    private boolean isRecovery;

    public Node(Role role) {
        this.role = role;
    }

    public Node(Role role, String hostname, int port, int id, int customerId, Boolean isRecovery) {
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

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "[" + role + ", " + id + ", " + hostname + ":" + port
                + ", " + customerId + ", " + isRecovery + "]";
    }

    public boolean isRecovery() {
        return isRecovery;
    }

    public void setRecovery(boolean recovery) {
        isRecovery = recovery;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAddress() {
        return hostname + ":" + port;
    }

    @Override
    public int compareTo(Node other) {
        if (hostname.equals(other.hostname))
            return port - other.port;
        return hostname.compareTo(other.hostname);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (port != node.port) return false;
        return hostname.equals(node.hostname);
    }

    @Override
    public int hashCode() {
        int result = hostname.hashCode();
        result = 31 * result + port;
        return result;
    }

    public static class Builder {
        private Role role;
        private int id;
        private String hostname;
        private int port;
        private int customerId;
        private boolean isRecovery;

        private Builder() {
        }

        public Builder setRole(Role role) {
            this.role = role;
            return this;
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setCustomerId(int customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder setRecovery(boolean recovery) {
            isRecovery = recovery;
            return this;
        }

        public Node build() {
            return new Node(role, hostname, port, id, customerId, isRecovery);
        }
    }
}
