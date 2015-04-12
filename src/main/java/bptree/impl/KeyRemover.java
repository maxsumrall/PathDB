package bptree.impl;

import bptree.RemoveResult;

import java.util.LinkedList;


public class KeyRemover {

    private CursorImpl cursor;

    public KeyRemover(CursorImpl cursor){
        this.cursor = cursor;
    }

    /**
     * Removes all elements from the current node returned by next() of the cursor.
     * @return RemoveResult resulting from this operation.
     */
    public RemoveResult removeAll(){
        LinkedList<Long[]> keysToBeSafelyRemovedLater = new LinkedList<>();
        LeafNode currLeaf = cursor.getCurrentLeaf();
        int removedKeys = 0;
        while(cursor.hasNext()){
            if(cursor.getCurrentLeaf() == currLeaf){
                keysToBeSafelyRemovedLater.add(cursor.next());
                removedKeys++;
            }
            else{
                removeKeysFromNode(keysToBeSafelyRemovedLater, currLeaf);
                currLeaf = cursor.getCurrentLeaf();
            }

        }
        removeKeysFromNode(keysToBeSafelyRemovedLater, currLeaf);
        return new RemoveResultImpl(removedKeys);
    }

    private void removeKeysFromNode(LinkedList<Long[]> keys, LeafNode node){
        for(Long[] key : keys){
            node.removeKeyWithoutWriting(key);
        }
        node.writeNodeToPage();
    }
}
