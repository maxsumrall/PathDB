package bptree.impl;

import bptree.Cursor;
import bptree.RemoveResult;

import java.io.IOException;
import java.util.LinkedList;


public class KeyRemover {

    private Cursor cursor;
    LeafNode currLeaf;
    RemoveResultImpl removeResult = new RemoveResultImpl(0);
    public KeyRemover(Cursor cursor){
        this.cursor = cursor;
    }

    /**
     * Removes all elements from the current node returned by next() of the cursor.
     * @return RemoveResult resulting from this operation.
     */
    public RemoveResult removeAll() throws IOException {
        if(cursor instanceof NullCursorImpl){
            return removeResult;
        }
        LinkedList<Long[]> keysToBeSafelyRemovedLater = new LinkedList<>();
        currLeaf = cursor.getCurrentLeaf();
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
        removeResult.setN(removedKeys);
        return removeResult;
    }

    private void removeKeysFromNode(LinkedList<Long[]> keysToBeSafelyRemovedLater, LeafNode node) throws IOException {
        for(Long[] key : keysToBeSafelyRemovedLater){
            node.removeKeyWithoutWriting(key);
        }
        node.writeNodeToPage();
        if(node.keys.size() == 0){
            node.nodeIsDeleted();
            removeResult.addMergedNodes(node.id, node.followingNodeId, true);
        }
    }
}
