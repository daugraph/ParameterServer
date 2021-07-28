package org.ps.core;

public enum DataType {
    INT64,
    FLOAT64,
    OTHER;

    public static <T> DataType getType(T t) {
        Class<?> clazz = t.getClass();
        if (clazz.equals(Integer.class) || clazz.equals(Long.class)) {
            return INT64;
        } else if (clazz.equals(Float.class) || clazz.equals(Double.class)) {
            return FLOAT64;
        } else {
            return OTHER;
        }
    }

    public static DataType fromInteger(int x) {
        switch (x) {
            case 0:
                return INT64;
            case 1:
                return FLOAT64;
            case 2:
                return OTHER;
        }
        return null;
    }
}
