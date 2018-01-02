package wuxian.me.datetimesparksql.util;

import com.sun.istack.Nullable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.cli.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DebugUtil {

    private SessionState sessionState;
    private HiveConf hiveConf;

    public DebugUtil(SessionState sessionState) {
        this.sessionState = sessionState;

        Configuration configuration = MetastoreConf.newMetastoreConf();
        hiveConf = new HiveConf(configuration, HiveConf.class);
    }

    @Nullable
    public String getSQLString(ObjectInspector[] objectInspectors) {
        if (objectInspectors.length == 0) {
            return null;
        }
        if (objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspectors[0] instanceof ConstantObjectInspector)) {
                return null;
            }
            ConstantObjectInspector propertiesPath = (ConstantObjectInspector) objectInspectors[0];
            String pgurl = propertiesPath.getWritableConstantValue().toString();
            return pgurl;
        }
        return null;
    }

    private static final String SELECT_INTO_REG = "insert\\s*into\\s*[a-z0-9A-Z]*[.][a-z0-9A-Z]*";
    private static final Pattern SELECT_INTO_PATTERN = Pattern.compile(SELECT_INTO_REG);
    private static final String DATABASE_REG = "[a-z0-9A-Z]*[.]";
    private static final Pattern DATABASE_PATTERN = Pattern.compile(DATABASE_REG);

    @Nullable
    public static String getInsertIntoString(String origin) {
        Matcher matcher = SELECT_INTO_PATTERN.matcher(origin.toLowerCase());
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    public static String getDatabaseDotFromInsertString(String origin) {
        Matcher matcher = DATABASE_PATTERN.matcher(origin);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    public void debug(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        System.out.println("----------------------debug begin------------------------");

        String originSql = getSQLString(objectInspectors);
        if (originSql == null) {
            return;
        }

        debugWithInvokeProcess(originSql);
        //executeSQLWithDriver(hiveConf, originSql);
        //debugFormatString(originSql);
    }

    private void debugWithInvokeProcess(String originSql) throws UDFArgumentException {

        ProcessBuilder processBuilder = null;
        processBuilder = new ProcessBuilder("hive", "-e", originSql);
        try {
            Process p = processBuilder.start();
            p.waitFor();

            final InputStream is = p.getInputStream();
            final java.io.BufferedReader reader =
                    new java.io.BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void debugFormatString(String originSql) throws UDFArgumentException {
        String insertIntoSql = getInsertIntoString(originSql);
        if (insertIntoSql == null) {
            throw new UDFArgumentException("invalid sql string!");
        }

        System.out.println("insertIntoSql: " + insertIntoSql);
        String databaseDot = getDatabaseDotFromInsertString(insertIntoSql);
        if (databaseDot == null) {
            throw new UDFArgumentException("invalid sql string!");
        }

        int index = originSql.indexOf(databaseDot);
        StringBuilder builder = new StringBuilder("");
        builder.append(originSql.substring(0, index));
        builder.append(originSql.substring(index + databaseDot.length()));
        String sql = builder.toString();
        String hiveDatabase = databaseDot.substring(0, databaseDot.length() - 1);

        System.out.println("hiveDb: " + hiveDatabase + " ,formatted sql: " + sql);
    }

    private void debugWithHiveConf() {
        HiveConf hiveConf = sessionState.getConf();
        String msUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
        System.out.println("msUri: " + msUri);  //bingo!
    }

    private void debugWithHiveInterface(HiveConf hiveConf) {
        //Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml,
        // org.apache.hadoop.hive.conf.LoopingByteArrayInputStream@2eae8e6e, file:/root/tmp/spark-2.2.0/conf/hive-site.xml
        System.out.println("HiveConf: " + sessionState.getConf().toString());
        try {
            Hive hive = Hive.get(hiveConf);
            List<String> list = hive.getAllDatabases();
            System.out.println(list.toString());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean executeSQLWithCliDriver(HiveConf hiveConf, String sql) {
        if (sql == null || sql.length() == 0) {
            return false;
        }
        CliDriver driver = new CliDriver(); //Todo:利用反射把setConf()暴力改成public的？？
        //driver.
        return true;

    }


    private boolean executeSQLWithDriver(HiveConf hiveConf, String sql) {
        if (sql == null || sql.length() == 0) {
            return false;
        }
        Driver driver = new Driver(hiveConf);
        CommandProcessorResponse response = null;
        try {
            System.out.println("------------------begin to execute " + sql + "------------------------");
            response = driver.run(sql);
            System.out.println("--------------------print header!----------------------");
            printHeader(driver, System.out);
            ArrayList<String> res = new ArrayList<String>();
            System.out.println("--------------------fetched result!----------------------");
            while (driver.getResults(res)) {
                for (String r : res) {
                    System.out.println(r);
                }
                res.clear();
            }
            System.out.println("execute response, SQLState: " + response.getSQLState()
                    + " schema: " + response.getSchema() + " responseCode: " + response.getResponseCode());
            //execute response, SQLState: null schema: null responseCode: 0
            return true;
        } catch (Exception e) {
            System.out.println("exception: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void printHeader(Driver qp, PrintStream out) {
        List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
        if (true && fieldSchemas != null) {
            // Print the column names
            boolean first_col = true;
            for (FieldSchema fs : fieldSchemas) {
                if (!first_col) {
                    out.print('\t');
                }
                out.print(fs.getName());
                first_col = false;
            }
            out.println();
        }
    }
}
