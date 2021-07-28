package cn.daugraph.ps.app;

public interface SimpleHandler {
    void process(SimpleData data, SimpleApp app);

    default int getCounter() {
        return 0;
    }
}
