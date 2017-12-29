package wuxian.me.datetimesparksql.util;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.lib.Node;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

public class ParseTreeUtil {

    public static ASTNode genASTNodeFrom(String sql) {
        if (sql == null || sql.length() == 0) {
            return null;
        }
        ASTNode node = null;

        try {
            ParseDriver pd = new ParseDriver();
            node = pd.parse(sql);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (ExceptionInInitializerError e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return node;
    }

    public static void parseAndPrint(String sql) throws UDFArgumentException {
        ASTNode tree = ParseTreeUtil.genASTNodeFrom(sql);
        if (tree == null) {
            throw new UDFArgumentException("parse sql: " + sql + " error!");
        }
        System.out.println("origin sql: " + sql);
        System.out.println("toStringTree: " + tree.toStringTree());
        ParseTreeUtil.walkAllChildNode(tree);
    }

    private ParseTreeUtil() {
    }

    public static void walkAllChildNode(ASTNode root) {

        if (root == null || root.getChildCount() == 0) {
            return;
        }
        for (Node node : root.getChildren()) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                Token tk = astNode.getToken();
                if (tk != null) {
                    System.out.println("token: " + tk.toString());
                }
                walkAllChildNode(astNode);
            }
        }

    }
}
