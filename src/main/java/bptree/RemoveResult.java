package bptree;

import java.util.List;

/**
 * Created by max on 4/12/15.
 */
public interface RemoveResult {

    int getN();

    List<Long[]> getMergedNodes();

    void addMergedNodes(Long deletedNodeId, Long mergedIntoNodeId, boolean isLeaf);

    void declarePairHasBeenTakenCareOf(Long deletedNodeId, Long mergedIntoNodeId);

    boolean containsNodesWhichRequireAttention();
}
