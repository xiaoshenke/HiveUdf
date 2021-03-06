package wuxian.me.hiveudf;

import org.junit.Test;
import wuxian.me.hiveudf.util.PgJdbc;

public class ExportPostgresUDFTest {

    @Test
    public void testUDF() {

        ExportPostgresUDF udf = new ExportPostgresUDF();
        String orginString = "jdbc:xxxxx;user=hello;password=xxx";
        System.out.println("origin: " + orginString);
        try {
            PgJdbc.parsePostGresUrl(orginString);
        } catch (Exception e) {

        }

        System.out.println("url: " + udf.getUrl());
        System.out.println("user: " + udf.getUser());
        System.out.println("password: " + udf.getPassword());
    }

}