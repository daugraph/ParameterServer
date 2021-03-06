package cn.daugraph.ps.core.common;

import cn.daugraph.ps.core.DataType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class Utils {

    public static final int BYTES = 8;

    public static final Map<String, String> envs = System.getenv();

    public static <T> byte[] convertToByteArray(DataType dataType, final T[] array) {
        ByteBuffer bb = ByteBuffer.allocate(array.length * BYTES);
        if (dataType == DataType.INT64) {
            Arrays.stream(array).forEach(t -> bb.putLong((Long) t));
        } else {
            Arrays.stream(array).forEach(t -> bb.putDouble((Double) t));
        }
        return bb.array();
    }

    public static long[] byteArrayToLong(final byte[] bytes) {
        long[] ret = new long[bytes.length / 8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < ret.length; i++) {
            ret[i] = bb.getLong();
        }
        return ret;
    }

    public static double[] byteArrayToDouble(final byte[] bytes) {
        double[] ret = new double[bytes.length / 8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < ret.length; i++) {
            ret[i] = bb.getDouble();
        }
        return ret;
    }

    public static int loadIntegerFromEnvironment(String key, int defaultValue) {
        if (envs.containsKey(key)) {
            return Integer.parseInt(envs.get(key));
        }
        return defaultValue;
    }

    public static String loadStringFromEnvironment(String key, String defaultValue) {
        if (envs.containsKey(key)) {
            return envs.get(key);
        }
        return defaultValue;
    }

}
