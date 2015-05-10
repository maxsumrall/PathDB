package bptree.impl;

public class RemoveResultProxy{
    public long removedNodeId;
    public long siblingNodeID;
    public boolean isLeaf;

    public RemoveResultProxy(long removedNodeId, long siblingNodeID, boolean isLeaf){
        this.removedNodeId = removedNodeId;
        this.siblingNodeID  = siblingNodeID;
        this.isLeaf = isLeaf;
    }
}

