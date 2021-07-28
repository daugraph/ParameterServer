package cn.daugraph.ps;

import java.util.ArrayList;
import java.util.List;
import cn.daugraph.ps.app.SimpleApp;
import cn.daugraph.ps.app.SimpleData;
import cn.daugraph.ps.app.SimpleHandler;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.common.Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    public static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {
        LOG.info("start Parameter Server ...");
        PostOffice ps = PostOffice.getInstance();
        int n = 100;
        ps.start(0);
        SimpleApp app = new SimpleApp(0, 0);
        SimpleHandler handler = new RequestHandler();
        app.setRequestHandler(handler);

        if (ps.isScheduler()) {
            List<Integer> ts = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int recver = Consts.ALL_GROUP;
                ts.add(app.request(1, "test", recver));
            }

            for (int t : ts) {
                app.wait(t);
            }
        }

        ps.finalize(0, true);

        if (handler.getCounter() != n) {
            LOG.error("Test failed, counter = {}, n = {}", handler.getCounter(), n);
        }
    }

    public static class RequestHandler implements SimpleHandler {

        private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
        private int counter = 0;

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("process simple data: {}", data);
            counter++;
        }

        public int getCounter() {
            return counter;
        }
    }
}
