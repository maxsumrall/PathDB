package bptree.impl;

import bptree.Cursor;

import java.io.IOException;

/**
 * A cursor for iterating over a result set
 */
public class CursorImpl implements Cursor{
    public Tree tree;
    public Long[] searchKey;
    private LeafNode currentLeaf;
    private int remainingElements = 0;
    private int cursorPosition;
    private int size = -1;

    public CursorImpl(Tree tree, LeafNode currentLeaf, Long[] searchKey, int cursorPosition){
        this.tree = tree;
        this.currentLeaf = currentLeaf;
        this.searchKey = searchKey;
        this.cursorPosition = cursorPosition;

        initializeAndCountValidKeysInThisNode();
    }

    private void initializeAndCountValidKeysInThisNode(){
        countValidKeysInThisNode();
        //In some cases the valid key values being at the exact beginning of a block. We don't know if there are valid keys in previous block, so we check.
        boolean noValidElementsWhileInitializing = remainingElements == 0;
        if(noValidElementsWhileInitializing){
            loadNextNode();
        }
    }

    private void countValidKeysInThisNode(){
        for(int i = cursorPosition; i < currentLeaf.keys.size(); i++){
            if(Node.keyComparator.validPrefix(searchKey, currentLeaf.keys.get(i))){
                remainingElements++;
            }
            else{
                break;
            }
        }
    }

    /**
     * Asks the Tree for the next sibling node,
     * Determines the number of remainingElements keys.
     * Returns true if the next node was loaded and has available keys.
     * Returns false if there was no next node or the next node contained no valid keys.
     */
    private boolean loadNextNode(){
        this.cursorPosition = 0;
        if(!currentLeaf.followingNodeId.equals(-1l)) {
            try {
                loadSiblingNodeAndSetRemainingElements();
            } catch (IOException e) {
                return false;
            }
        }
        return (this.remainingElements > 0);
    }

    private void loadSiblingNodeAndSetRemainingElements() throws IOException {
        this.currentLeaf = (LeafNode)tree.getNode(currentLeaf.followingNodeId);
        for(Long[] key: this.currentLeaf.keys){
            if(Node.keyComparator.validPrefix(searchKey, key)){
                this.remainingElements++;
            }
        }
    }

    @Override
    public LeafNode getCurrentLeaf(){
        return currentLeaf;
    }

    @Override
    public Long[] next(){
        if(remainingElements == 0){
            if(cursorPosition == currentLeaf.keys.size()){
                loadNextNode();
                return next();
            }
            else{//no more remainingElements, and yet there are still more keys in this node. We are truly finished.
                return new Long[]{};
            }
        }
            remainingElements--;
            return currentLeaf.keys.get(cursorPosition++);
    }


    @Override
    public boolean hasNext(){
        if(remainingElements == 0){
            if(cursorPosition == currentLeaf.keys.size()){ //No more remainingElements, but there are no more keys in this node...
                if(currentLeaf.keys.size() == 0){//TODO remove this if block.
                    //System.out.println("Empty Node Found!");
                    return false;
                }
                loadNextNode();
                return hasNext();
            }
            else{//no more remainingElements, and yet there are still more keys in this node. We are truly finished.
                return false;
            }
        }
        return true;
    }

    public int size(){
        if(size == -1) {
            size = validKeysInNode(currentLeaf);
            LeafNode node = currentLeaf;
            while (Node.keyComparator.validPrefix(searchKey, node.keys.get(node.keys.size()-1))) {
                try {
                    node = (LeafNode) tree.getNode(node.followingNodeId);
                    size += validKeysInNode(node);
                } catch (IOException e) {
                    break;
                }
            }

        }
        return size;
    }

    /**
     * Counts the valid keys in a given node
     * @param node
     * @return
     */
    private int validKeysInNode(LeafNode node){
        int sum = 0;
        for(Long[] key : node.keys){
            if(Node.keyComparator.validPrefix(searchKey, key)){
                sum++;
            }
        }
        return sum;
    }

}
