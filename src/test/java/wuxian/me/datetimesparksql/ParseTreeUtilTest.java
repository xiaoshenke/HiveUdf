package wuxian.me.datetimesparksql;

import org.junit.Test;

import static org.junit.Assert.*;

public class ParseTreeUtilTest {

    @Test
    public void testParsing() throws Exception {

        ParseTreeUtil.parseAndPrint("select * from abc");

        ParseTreeUtil.parseAndPrint("insert into hivexyz values (a,b,c)");
    }

}