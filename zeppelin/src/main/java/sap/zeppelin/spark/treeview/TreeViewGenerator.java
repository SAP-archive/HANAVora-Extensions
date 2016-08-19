package sap.zeppelin.spark.treeview;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sap.zeppelin.spark.SapSqlInterpreter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Singleton Util to create treeviews
 *
 */
public class TreeViewGenerator {
    private Logger logger = LoggerFactory.getLogger(TreeViewGenerator.class);

    // buffers the read in html file
    private String treeviewHtmlBuffer = null;

    private static TreeViewGenerator instance;

    private TreeViewGenerator () {}

    public static synchronized TreeViewGenerator getInstance () {
        if (TreeViewGenerator.instance == null) {
            TreeViewGenerator.instance = new TreeViewGenerator ();
        }
        return TreeViewGenerator.instance;
    }

    public String createTreeView(DataFrame df, String idColumn,
                                 String predColumn, String nameColumn, int maxResult) {
        return "%angular " + fillTreeviewScriptWithData(convertDataToTree(df, idColumn,
                predColumn, nameColumn, maxResult));
    }

    private String convertDataToTree(DataFrame df, String idColumn,
                                           String predColumn, String nameColumn, int maxResult)
    {
        Map<String, List<TreeViewNode>> map = new java.util.HashMap<>();
        String[] columns = df.columns();
        Row[] rows = df.limit(maxResult).collect();
        TreeViewNode rootNode = null;
        for (int r = 0; r < maxResult && r < rows.length; r++) {
            Row row = rows[r];

            String id = null, pred = null, name = null;
            for (int i = 0; i < columns.length; i++) {
                String value = null;
                if (!row.isNullAt(i)) {
                    value = row.get(i).toString();
                } else {
                    value = "null";
                }

                if (columns[i].equals(idColumn)) {
                    id = value;
                } else if (columns[i].equals(predColumn)) {
                    pred = value;
                }

                if (columns[i].equals(nameColumn)) {
                    name = value;
                }
            }
            // pred null value case depends on type of the variable
            if (pred == null || pred.equalsIgnoreCase("")
                    || pred.equalsIgnoreCase("null") || pred.equalsIgnoreCase("0")) {
                logger.info("root node found: {}, {}", id, name);
                rootNode = new TreeViewNode(id, name);
            }

            if (map.get(pred) == null) {
                List<TreeViewNode> list = new LinkedList<>();
                list.add(new TreeViewNode(id, name));
                map.put(pred, list);
            } else {
                map.get(pred).add(new TreeViewNode(id, name));
            }
        }

        addChildrenToNode(rootNode, map);

        return rootNode.toString();
    }

    private void addChildrenToNode(TreeViewNode node, Map<String, List<TreeViewNode>> map) {
        List<TreeViewNode> list = map.get(node.getId());
        if (list != null) {
            node.setChildren(list);
            for (int i = 0; i < node.getChildren().size(); i++) {
                addChildrenToNode(node.getChildren().get(i), map);
            }
        }
    }

    private String fillTreeviewScriptWithData(String data) {
        String script = null;
        try {
            script = getTreeviewHtml();
            if(script == null || script.length() == 0) {
                throw new IOException("Error when reading treeview.html, size is empty");
            }
            logger.info("script length: "+(script!=null? script.length(): null) );
            script = script.replaceAll("TREEID", UUID.randomUUID().toString());
            script = script.replaceFirst("TREEDATA", data);
        } catch (IOException e) {
            logger.error("Error on reading resource file", e);
        }

        return script;
    }

    private synchronized String getTreeviewHtml() throws IOException {
        if(this.treeviewHtmlBuffer == null) {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream in = classLoader.getResourceAsStream("treeview.html");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(in, out);
            treeviewHtmlBuffer = new String(out.toByteArray());
        }
        return treeviewHtmlBuffer;
    }
}
