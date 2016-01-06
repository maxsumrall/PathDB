/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

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
