package bptree;

import java.io.IOException;

/**
 * A cursor for iterating over a result set
 */
public class Cursor {

    protected Tree tree;

    public Long[] search_key;

    protected LeafNode currentLeaf;

    private int remaining = 0; //The remaining number of valid keys in this node.

    private int position; //The index into the currentLeaf.keys where the next element is.

    private int size = -1;

    public Cursor(Tree tree, LeafNode currentLeaf, Long[] search_key, int position){
        this.tree = tree;
        this.currentLeaf = currentLeaf;
        this.search_key = search_key;
        this.position = position;
        for(int i = position; i < currentLeaf.keys.size(); i++){
            if(Node.keyComparator.validPrefix(search_key, currentLeaf.keys.get(i))){
                remaining++;
            }
            else{
                break;
            }
        }
    }

    /**
     * Asks the Tree for the next sibling node,
     * Determines the number of remaining keys.
     * Returns true if the next node was loaded and has available keys.
     * Returns false if there was no next node or the next node contained no valid keys.
     */
    private boolean loadNextNode(){
        position = 0;
        assert(remaining == 0);
        try {
            this.currentLeaf = (LeafNode)tree.getNode(currentLeaf.siblingID);
            for(Long[] key: this.currentLeaf.keys){
                if(Node.keyComparator.validPrefix(search_key, key)){
                    remaining++;
                }
            }
        }
        catch(IOException e){
            return false;
        }
        return (remaining > 0);
    }

    public Long[] next(){
        if(remaining == 0){
            if(position == currentLeaf.keys.size()){ //No more remaining, but there are no more keys in this node...
                loadNextNode();
                return next(); //
            }
            else{//no more remaining, and yet there are still more keys in this node. We are truly finished.
                return new Long[]{};
            }
        }
            remaining--;
            return currentLeaf.keys.get(position++);
    }



    public boolean hasNext(){
        if(remaining == 0){
            if(position == currentLeaf.keys.size()){ //No more remaining, but there are no more keys in this node...
                loadNextNode();
                return hasNext(); //
            }
            else{//no more remaining, and yet there are still more keys in this node. We are truly finished.
                return false;
            }
        }
        return true;
    }

    public int size(){
        if(size == -1) {
            size = sumValid(currentLeaf);
            LeafNode node = currentLeaf;
            while (Node.keyComparator.validPrefix(search_key, node.keys.getLast())) {
                try {
                    node = (LeafNode) tree.getNode(node.siblingID);
                } catch (IOException e) {
                    break;
                }
            size = sumValid(node);
            }

        }
        return size;

    }

    /**
     * Counts the valid keys in the given node
     * @param node
     * @return
     */
    private int sumValid(LeafNode node){
        int sum = 0;
        for(Long[] key : node.keys){
            if(Node.keyComparator.validPrefix(search_key, key)){
                sum++;
            }
        }
        return sum;
    }
}
