package bptree.impl;

import java.util.HashMap;
import java.util.Set;

public class MergedNodesList {
    private HashMap<Long, Long[]> modifiedNodes;
    public static final Long[] DELETED_NODE = new Long[]{-1l};
    public MergedNodesList(){
        modifiedNodes = new HashMap<>();
    }

    public Set<Long> getModifiedNodeIds(){
        return modifiedNodes.keySet();
    }

    public Long[] getNodeState(Long nodeId){
        return modifiedNodes.get(nodeId);
    }

    public void addModifiedNode(Long nodeId, Long[] newSmallestKey){
        modifiedNodes.put(nodeId, newSmallestKey);
    }

    public void addDeletedNode(Long nodeId){
        modifiedNodes.put(nodeId, DELETED_NODE);
    }

    public boolean isEmpty(){
        return modifiedNodes.isEmpty();
    }

    public void removeNode(Long idOfNodeToRemove){
        Long keyToDelete = null;
        for(Long keySetNodeId : modifiedNodes.keySet()){
            if(keySetNodeId == idOfNodeToRemove){
                keyToDelete = keySetNodeId;
                break;
            }
        }
        if(keyToDelete != null){
            modifiedNodes.remove(keyToDelete);
        }
    }

}
