package bptree.impl;

import java.util.LinkedList;
import java.util.List;

public class MergedNodesList {
    public static final Long LEAF_NODES = -1l;
    public static final Long INTERNAL_NODES = -2l;
    private LinkedList<Long[]> modifiedNodes = new LinkedList<>();

    public List<Long[]> getMergedNodes(){
        return new LinkedList<>(modifiedNodes);
    }

    public void addMergedNodes(Long deletedNodeId,Long mergedIntoNodeId, boolean areLeaves){
        modifiedNodes.push(new Long[]{deletedNodeId, mergedIntoNodeId, areLeaves ? LEAF_NODES : INTERNAL_NODES});
    }

    public boolean isEmpty(){
        return modifiedNodes.isEmpty();
    }

    public void removePair(Long deletedNodeId, Long mergedIntoNodeId){
        Long[] pairToDelete = null;
        for(Long[] pair : modifiedNodes){
            if(deletedNodeId.equals(pair[0]) && mergedIntoNodeId.equals(pair[1])){
                pairToDelete = pair;
                break;
            }
        }
        if(pairToDelete != null){
            modifiedNodes.remove(pairToDelete);
        }
    }

}
