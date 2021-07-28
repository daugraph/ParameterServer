package cn.daugraph.ps.core;

import cn.daugraph.ps.core.common.Utils;
import java.util.ArrayList;
import java.util.List;

public class Message {
    private Meta meta;
    private List<byte[]> data = new ArrayList<>();

    public Message() {
    }

    public Message(Meta meta) {
        this.meta = meta;
    }

    public <T> void addData(DataType dataType, T[] feat) {
        meta.getDataTypes().add(dataType);
        data.add(Utils.convertToByteArray(dataType, feat));
    }

    public Meta getMeta() {
        return meta;
    }

    public void setMeta(Meta meta) {
        this.meta = meta;
    }

    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }
}
