package wuxian.me.hiveudf;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HiveSemanticAnalyzerTest {

    private static HiveConf hiveConf;
    private static QueryState queryState;
    static HiveConf conf;
    private static String defaultDB = "default";
    private static String tblName = "testReplSA";
    private static ArrayList<String> cols = new ArrayList<String>(Arrays.asList("col1", "col2"));

    @BeforeClass
    public static void initialize() throws HiveException {

        hiveConf = new HiveConf(SemanticAnalyzer.class);
        queryState = new QueryState(hiveConf);
        conf = queryState.getConf();
        conf.set("hive.security.authorization.manager", "");
        SessionState.start(conf);
        /*
        Hive hiveDb = Hive.get(conf);
        hiveDb.createTable(defaultDB + "." + tblName, cols, null, OrcInputFormat.class, OrcOutputFormat.class);
        Table t = hiveDb.getTable(tblName);
        */
    }


    @Test
    public void testParser(HiveConf hiveConf) throws Exception {
        String sql = "insert into human select * from postgres.abc";
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(sql);
        ASTNode root = (ASTNode) tree.getChild(0);
        BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory.get(hiveConf, root);
        analyzer.analyze(root, new Context(conf));  //human not found
        System.out.println(tree.dump());
    }

    @AfterClass
    public static void teardown() throws HiveException {
    }
}
