package org.apache.spark.sql;

import java.util.LinkedList;
import java.util.List;

/**
 * This class is used by SAP sql interpreter
 * to build a tree
 */
public class SqlInterpreterNode {
    private String id;
    private String name;
    private List<SqlInterpreterNode> children;

    SqlInterpreterNode(String id, String name) {
        this.id = id;
        this.name = name;
        this.children = new LinkedList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<SqlInterpreterNode> getChildren() {
        return children;
    }

    public void setChildren(List<SqlInterpreterNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        String childrenJson = "";
        if (!this.children.isEmpty()) {
            childrenJson = ", \"children\": " + this.children.toString() + "";
        }

        String json = "{ \"name\":\""+this.name+"\""+childrenJson+" }";
        return json;
    }
}
