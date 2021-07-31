package com.daugraph.other;

import cn.daugraph.ps.core.Node;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class NodeTest {
    @Test
    public void testNodeBuilder() {
        Node node = Node.builder().setCustomerId(1).build();

        List<int []> tracker = new ArrayList<>();
        tracker.add(new int[]{20, 0});
        System.out.println(tracker.get(0)[0]);
    }
}
