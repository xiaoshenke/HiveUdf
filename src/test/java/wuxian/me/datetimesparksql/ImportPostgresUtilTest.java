package wuxian.me.datetimesparksql;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImportPostgresUtilTest {

    @Test
    public void testIsValidInsertSelectSQL() {

        Assert.assertTrue(ImportPostgresUtil.isValidInsertSelectSQL("insert into a select * from b"));

        Assert.assertFalse(ImportPostgresUtil.isValidInsertSelectSQL("insert into a"));
    }

}