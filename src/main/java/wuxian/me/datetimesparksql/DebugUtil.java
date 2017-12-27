package wuxian.me.datetimesparksql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;
import wuxian.me.datetimesparksql.util.ImportPostgresUtil;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class DebugUtil {

    private SessionState sessionState;

    public DebugUtil(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    private Driver driver;

    public void debug() {
        //driver = new Driver(sessionState.getConf());
        //System.out.println("driver: " + driver.toString());

        System.out.println("----------------------debug begin------------------------");
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
            driver = (Driver) proc;

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
                    //                 counter += res.size();
                    res.clear();
                    //                if (out.checkError()) {
                    //                       break;
                    //                     }
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
