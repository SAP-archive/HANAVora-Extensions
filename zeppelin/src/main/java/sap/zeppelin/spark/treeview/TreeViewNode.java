package sap.zeppelin.spark.treeview;

import java.util.LinkedList;
import java.util.List;

/**
 * Represents a node in the hierarchical view provided in the UI
 */
public class TreeViewNode {
    private String id;
    private String name;
    private List<TreeViewNode> children;

    TreeViewNode(String id, String name) {
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

    public List<TreeViewNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeViewNode> children) {
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