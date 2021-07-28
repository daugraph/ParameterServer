package org.ps.core.handler;

import org.ps.core.Message;

public interface RecvHandler {
    void process(Message message);
}
