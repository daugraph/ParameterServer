package org.ps.app;

public class DefaultRequestSimpleHandler implements SimpleHandler {
    @Override
    public void process(SimpleData data, SimpleApp app) {
        app.response(data);
    }
}
