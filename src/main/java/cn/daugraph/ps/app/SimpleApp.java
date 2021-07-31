package cn.daugraph.ps.app;

import cn.daugraph.ps.core.Customer;
import cn.daugraph.ps.core.Message;
import cn.daugraph.ps.core.Meta;
import cn.daugraph.ps.core.PostOffice;
import cn.daugraph.ps.core.handler.RecvHandler;
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
        PostOffice.get().addCustomer(customer);
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
        customer.waitRequest(timestamp);
    }

    public void response(SimpleData req) {
        this.response(req, "");
    }

    public void response(SimpleData req, String body) {
        Meta meta = new Meta.Builder()
                .setHead(req.getHead())
                .setBody(body)
                .setTimestamp(req.getTimestamp())
                .setRequest(false)
                .setSimpleApp(true)
                .setAppId(customer.getAppId())
                .setCustomerId(req.getCustomerId())
                .setRecver(req.getSender())
                .build();
        PostOffice.get().getVan().send(new Message(meta));
    }

    // 向目标组发送信息
    public int request(int reqHead, String reqBody, int targetGroup) {
        // tracker id 用于追踪请求处理状况
        int trackerId = customer.newRequest(targetGroup);
        LOG.info("Create request successfully, tracker/timestamp id is: {}", trackerId);
        for (int nodeId : PostOffice.get().getNodeIds(targetGroup)) {
            Meta meta = new Meta.Builder()
                    .setHead(reqHead)
                    .setBody(reqBody)
                    .setTimestamp(trackerId)
                    .setRequest(true)
                    .setSimpleApp(true)
                    .setAppId(customer.getAppId())
                    .setCustomerId(customer.getCustomerId())
                    .setRecver(nodeId)
                    .build();
            PostOffice.get().getVan().send(new Message(meta));
        }
        LOG.info("Send message to group {} successfully", targetGroup);
        return trackerId;
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

    public void initialize() {
        customer.initialize();
    }

    private static class DefaultRequestSimpleHandler implements SimpleHandler {

        public static final Logger LOG = LoggerFactory.getLogger(DefaultRequestSimpleHandler.class);

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("Handle simple data: {}", data);
        }
    }

    private static class DefaultResponseSimpleHandler implements SimpleHandler {

        public static final Logger LOG = LoggerFactory.getLogger(DefaultRequestSimpleHandler.class);

        @Override
        public void process(SimpleData data, SimpleApp app) {
            LOG.info("Handle simple data: {}", data);
        }
    }
}
