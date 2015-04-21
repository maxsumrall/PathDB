package bptree.impl;

import bptree.RemoveResult;

import java.util.List;

public class RemoveResultImpl implements RemoveResult{

    private int removedKeys;
    private MergedNodesList mergedNodesList;

    public RemoveResultImpl(int removedKeys){
        mergedNodesList = new MergedNodesList();
        this.removedKeys = removedKeys;
    }
    public RemoveResultImpl(){
        this(0);
    }

    public int getN() {
        return this.removedKeys;
    }

    public void setN(int removedNodes){
        this.removedKeys = removedNodes;
    }

    public boolean containsNodesWhichRequireAttention(){
        return !mergedNodesList.isEmpty();
    }

    public List<Long[]> getMergedNodes() {
        return mergedNodesList.getMergedNodes();
    }

    public void declarePairHasBeenTakenCareOf(Long firstNodeID, Long mergedIntoNodeId) {
        mergedNodesList.removePair(firstNodeID, mergedIntoNodeId);
    }

    public void addMergedNodes(Long deletedNodeId, Long mergedIntoNodeId, boolean isLeaf){
        mergedNodesList.addMergedNodes(deletedNodeId, mergedIntoNodeId, isLeaf);
    }
}
