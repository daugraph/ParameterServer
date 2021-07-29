package com.daugraph.other;

import cn.daugraph.ps.core.Node;
import org.junit.jupiter.api.Test;

public class NodeTest {
    @Test
    public void testNodeBuilder() {
        Node node = Node.builder().setCustomerId(1).build();
    }
}
