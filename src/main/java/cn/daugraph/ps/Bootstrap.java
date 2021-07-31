package cn.daugraph.ps;

import cn.daugraph.ps.app.SimpleApp;
import cn.daugraph.ps.app.SimpleData;
import cn.daugraph.ps.app.SimpleHandler;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    public static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {
        LOG.info("Starting Parameter Server ...");
        PostOffice po = PostOffice.get();
        po.start(0);
        SimpleApp app = new SimpleApp(0, 0);
        SimpleHandler handler = new RequestHandler();
        app.setRequestHandler(handler);
        app.initialize();

        if (po.isScheduler()) {
            int requestId = app.request(1, "lijianmeng", Constants.GROUP_ALL);
            app.wait(requestId);
        }

        po.finalize(0, true);

        if (handler.getCounter() != 1) {
            LOG.error("Test failed, counter = {}, n = {}", handler.getCounter(), 1);
        }
    }

    public static class RequestHandler implements SimpleHandler {

        private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
        private int counter = 0;

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("Process simple app data: {}", data);
            counter++;
        }

        public int getCounter() {
            return counter;
        }
    }
}
