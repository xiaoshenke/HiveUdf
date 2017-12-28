package wuxian.me.datetimesparksql.util;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;
import wuxian.me.datetimesparksql.util.ImportPostgresUtil;
import org.apache.hadoop.hive.cli.*;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class DebugUtil {

    private SessionState sessionState;

    public DebugUtil(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    public void debug() {
        System.out.println("----------------------debug begin------------------------");

        //debugWithDriver();

        //debugWithHiveInterface();

        //debugWithCliDriver();

        //debugWithHiveConf();

        debugWithMetastore();
    }

    private void debugWithMetastore() {
        Configuration configuration = MetastoreConf.newMetastoreConf();
        HiveConf hiveConf = sessionState.getConf();
        hiveConf = new HiveConf(configuration, HiveConf.class);
        debugWithHiveInterface(hiveConf);
        //Configuration metastoreConf = MetastoreConf.newMetastoreConf();
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

    private void debugWithCliDriver() {
        CliDriver cliDriver = new CliDriver();
        String sql = "use test";
        System.out.println("------------------begin to execute " + sql + "------------------------");
        cliDriver.processCmd(sql);
        System.out.println("------------------end------------------------");

    }

    private void debugWithDriver() {
        try {
            executeSQL("show databases");
        } catch (Exception e) {

        }
    }

    private String[] tokenizeCmd(String cmd) {
        return cmd.split("\\s+");
    }

    private boolean executeSQL(String sql) throws Exception {
        if (sql == null || sql.length() == 0) {
            return false;
        }
        String[] tokens = tokenizeCmd(sql);
        CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) sessionState.getConf());
        if (proc == null) {
            System.out.println("executeSQL,proc is null");
            return false;
        }

        if (proc instanceof Driver) {
            Driver driver = (Driver) proc;

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
        } else {
            System.out.println("proc not instanceOf Driver,just return false");
            return false;
        }

    }

    private void printHeader(Driver qp, PrintStream out) {
        List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
        if (true
                && fieldSchemas != null) {
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
