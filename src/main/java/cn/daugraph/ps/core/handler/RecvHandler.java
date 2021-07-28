package cn.daugraph.ps.core.handler;

import cn.daugraph.ps.core.Message;

public interface RecvHandler {
    void process(Message message);
}
