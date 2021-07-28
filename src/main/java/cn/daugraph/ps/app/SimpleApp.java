package cn.daugraph.ps.app;

import cn.daugraph.ps.core.Customer;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.handler.RecvHandler;
import cn.daugraph.ps.core.Message;
import cn.daugraph.ps.core.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimpleApp is basic class test Van.
 */
public class SimpleApp implements RecvHandler {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleApp.class);
    private SimpleHandler requestHandler = new DefaultRequestSimpleHandler();
    private SimpleHandler responseHandler = new DefaultResponseSimpleHandler();
    private Customer customer;

    public SimpleApp(int appId, int customerId) {
        customer = new Customer(appId, customerId, this);
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public void setRequestHandler(SimpleHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    public void setResponseHandler(SimpleHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public void wait(int timestamp) {
        try {
            customer.wait(timestamp);
        } catch (InterruptedException e) {
            LOG.error("SimpleApp interrupted while waiting ..");
        }
    }

    public void response(SimpleData req) {
        this.response(req, "");
    }

    public void response(SimpleData req, String body) {
        Meta meta = new Meta.Builder().setHead(req.getHead()).setBody(body).setTimestamp(req.getTimestamp())
                .setRequest(false).setSimpleApp(true).setAppId(customer.getAppId()).setCustomerId(req.getCustomerId())
                .setRecver(req.getSender()).build();
        PostOffice.getInstance().getVan().send(new Message(meta));
    }

    public int request(int reqHead, String reqBody, int recvId) {
        Meta meta = new Meta.Builder().setHead(reqHead).setBody(reqBody).setTimestamp(customer.newRequest(recvId))
                .setRequest(true).setSimpleApp(true).setAppId(customer.getAppId())
                .setCustomerId(customer.getCustomerId()).build();
        Message msg = new Message(meta);
        for (int r : PostOffice.getInstance().getNodeIds(recvId)) {
            msg.getMeta().setRecver(r);
            PostOffice.getInstance().getVan().send(msg);
        }

        return meta.getTimestamp();
    }

    @Override
    public void process(Message message) {
        SimpleData recv = new SimpleData();
        recv.setSender(message.getMeta().getSender());
        recv.setHead(message.getMeta().getHead());
        recv.setBody(message.getMeta().getBody());
        recv.setTimestamp(message.getMeta().getTimestamp());
        recv.setCustomerId(message.getMeta().getCustomerId());
        if (message.getMeta().isRequest()) {
            requestHandler.process(recv, this);
        } else {
            responseHandler.process(recv, this);
        }
    }

    private static class DefaultRequestSimpleHandler implements SimpleHandler {

        public static final Logger LOG = LoggerFactory.getLogger(DefaultRequestSimpleHandler.class);

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("process simple data: {}", data);
        }
    }

    private static class DefaultResponseSimpleHandler implements SimpleHandler {

        public static final Logger LOG = LoggerFactory.getLogger(DefaultRequestSimpleHandler.class);

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("process simple data: {}", data);
        }
    }
}
