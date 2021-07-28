package org.ps.internal;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.ps.core.common.Utils;
import org.ps.core.DataType;

public class DataTypeTest {
    @Test
    public void testGetDataType() {
        int i1     = 123456789;
        Integer i2 = 123456789;
        long l1    = 1234567890L;
        Long l2    = 1234567890L;
        float f1    = 1234.567F;
        Float f2   = 1234.567F;
        double d1  = 123456.789;
        Double d2  = 123456.789;

        Assertions.assertSame(DataType.getType(l1), DataType.INT64);
        Assertions.assertSame(DataType.getType(l2), DataType.INT64);
        Assertions.assertSame(DataType.getType(d1), DataType.FLOAT64);
        Assertions.assertSame(DataType.getType(d2), DataType.FLOAT64);
    }

    @Test
    public void test() {
        byte[] a = Utils.convertToByteArray(DataType.INT64, new Long[]{1L, 2L, 3L, 4L});
        System.out.println(Arrays.toString(a));
        long[] b = Utils.byteArrayToLong(a);
        System.out.println(Arrays.toString(b));

        byte[] c = Utils.convertToByteArray(DataType.FLOAT64, new Double[]{1.2, 2.3, 3.4, 5.6});
        System.out.println(Arrays.toString(c));
        double[] d = Utils.byteArrayToDouble(c);
        System.out.println(Arrays.toString(d));

        byte[] e = new byte[] {'p', 's', '0', '1'};
        System.out.println(new String(e));
    }
}
