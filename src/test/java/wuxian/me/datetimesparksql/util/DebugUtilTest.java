package wuxian.me.datetimesparksql.util;

import org.junit.Assert;
import org.junit.Test;

public class DebugUtilTest {

    @Test
    public void testGetInsertIntoString() {
        Assert.assertTrue(DebugUtil.getInsertIntoString("insert           into test.user") != null);

        Assert.assertFalse(DebugUtil.getInsertIntoString("insert INTO user") != null);

        System.out.println(DebugUtil.getInsertIntoString("insert into test.user select * from postgres.usr"));
    }

    @Test
    public void testGetDatabaseDotString() {
        Assert.assertTrue(DebugUtil.getDatabaseDotFromInsertString("insert into test.user") != null);
        System.out.println(DebugUtil.getDatabaseDotFromInsertString("select * from test.user"));
    }

}