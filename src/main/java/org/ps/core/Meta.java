package org.ps.core;

import java.util.ArrayList;
import java.util.List;

public class Meta {

    Control control;
    // 消息头部
    private int head;
    // 应用消息的唯一标识
    private int appId;
    private int customerId;
    private int timestamp;
    private int sender;
    private int recver;
    private boolean request;
    // 推送信息
    private boolean push;
    // 拉取信息
    private boolean pull;
    private boolean isSimpleApp;
    private String body;
    private List<DataType> dataTypes;
    // 字节大小
    private int dataSize;
    // 消息优先级
    private int priority;

    public Meta(
            int head,
            int appId,
            int customerId,
            int timestamp,
            int sender,
            int recver,
            boolean request,
            boolean push,
            boolean pull,
            boolean isSimpleApp,
            String body,
            List<DataType> dataTypes,
            Control control,
            int dataSize,
            int priority) {
        this.head = head;
        this.appId = appId;
        this.customerId = customerId;
        this.timestamp = timestamp;
        this.sender = sender;
        this.recver = recver;
        this.request = request;
        this.push = push;
        this.pull = pull;
        this.isSimpleApp = isSimpleApp;
        this.body = body;
        this.dataTypes = dataTypes;
        this.control = control;
        this.dataSize = dataSize;
        this.priority = priority;
    }

    public Meta(Control.Command terminate) {
        this.getControl().setCommand(terminate);
    }

    public Meta() {
        this.dataTypes = new ArrayList<>();
        this.control = new Control();
    }

    public boolean isPull() {
        return pull;
    }

    public void setPull(boolean pull) {
        this.pull = pull;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getHead() {
        return head;
    }

    public void setHead(int head) {
        this.head = head;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public int getSender() {
        return sender;
    }

    public void setSender(int sender) {
        this.sender = sender;
    }

    public boolean isRequest() {
        return request;
    }

    public void setRequest(boolean request) {
        this.request = request;
    }

    public boolean isPush() {
        return push;
    }

    public void setPush(boolean push) {
        this.push = push;
    }

    public boolean isSimpleApp() {
        return isSimpleApp;
    }

    public void setSimpleApp(boolean simpleApp) {
        isSimpleApp = simpleApp;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public Control getControl() {
        return control;
    }

    public void setControl(Control control) {
        this.control = control;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public int getRecver() {
        return recver;
    }

    public void setRecver(int recver) {
        this.recver = recver;
    }

    @Override
    public String toString() {
        return "Meta{"
                + "head="
                + head
                + ", appId="
                + appId
                + ", customerId="
                + customerId
                + ", timestamp="
                + timestamp
                + ", sender="
                + sender
                + ", recver="
                + recver
                + ", request="
                + request
                + ", push="
                + push
                + ", pull="
                + pull
                + ", isSimpleApp="
                + isSimpleApp
                + ", body='"
                + body
                + '\''
                + ", dataTypes="
                + dataTypes
                + ", control="
                + control
                + ", dataSize="
                + dataSize
                + ", priority="
                + priority
                + '}';
    }

    public static class Builder {

        Control control;
        private int head;
        private int appId;
        private int customerId;
        private int timestamp;
        private int sender;
        private int recver;
        private boolean request;
        private boolean push;
        private boolean pull;
        private boolean isSimpleApp;
        private String body;
        private List<DataType> dataTypes;
        private int dataSize;
        private int priority;

        public Builder setHead(int head) {
            this.head = head;
            return this;
        }

        public Builder setAppId(int appId) {
            this.appId = appId;
            return this;
        }

        public Builder setCustomerId(int customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder setTimestamp(int timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setSender(int sender) {
            this.sender = sender;
            return this;
        }

        public Builder setRecver(int recver) {
            this.recver = recver;
            return this;
        }

        public Builder setRequest(boolean request) {
            this.request = request;
            return this;
        }

        public Builder setPush(boolean push) {
            this.push = push;
            return this;
        }

        public Builder setPull(boolean pull) {
            this.pull = pull;
            return this;
        }

        public Builder setSimpleApp(boolean simpleApp) {
            isSimpleApp = simpleApp;
            return this;
        }

        public Builder setBody(String body) {
            this.body = body;
            return this;
        }

        public Builder setDataTypes(List<DataType> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public Builder setControl(Control control) {
            this.control = control;
            return this;
        }

        public Builder setDataSize(int dataSize) {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public Meta build() {
            return new Meta(
                    head,
                    appId,
                    customerId,
                    timestamp,
                    sender,
                    recver,
                    request,
                    push,
                    pull,
                    isSimpleApp,
                    body,
                    dataTypes,
                    control,
                    dataSize,
                    priority);
        }
    }
}
