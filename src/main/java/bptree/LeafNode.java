package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by max on 2/13/15.
 */
public class LeafNode extends Node {

    public LeafNode(Tree tree, long id){
        this(new LinkedList<Long[]>(), tree, id);
    }
    public LeafNode(LinkedList<Long[]> k, Tree tree, Long id){
        this.keys = k;
        this.tree = tree;
        this.id = id;
    }

    public LeafNode(PageCursor cursor, Tree tree, Long id) throws IOException {
        this.tree = tree;
        this.id = id;
        deserialize(cursor); //set the keys and the siblingID, as read from the page cache
    }

    /**
     * Parses a page of a Leaf Node under the assumption that keys are the same length without any delimiter between keys.
     * @param cursor
     */
    @Override
    protected void sameLengthKeyDeserialization(PageCursor cursor){
        int keyLength = parseHeaderForKeyLength(cursor);
        int numberOfKeys = parseHeaderForNumberOfKeys(cursor);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++){
                newKey.add(cursor.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            deserialize_keys.add((Long[]) newKey.toArray());
            newKey.clear(); //clear if for the next round.
        }
        this.keys = deserialize_keys;
    }

    /**
     * Parses a page under the assumption that each key length is unknown and the keys are -1 delimited.
     * @param cursor
     */
    @Override
    protected void variableLengthKeyDeserialization(PageCursor cursor){
        int numberOfKeys = parseHeaderForNumberOfKeys(cursor);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        Long nextValue = cursor.getLong();
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(int i = 0; i < numberOfKeys; i++){ //check if we are at the final end of the block
            while(nextValue != DELIMITER_VALUE) { //while still within this key
                newKey.add(nextValue);
                nextValue = cursor.getLong();
            }
            deserialize_keys.add((Long[]) newKey.toArray());
            newKey.clear();
            nextValue = cursor.getLong();
        }
        this.keys = deserialize_keys;
    }


    /**
     * Serializes this nodes keys under the assumption that the keys are the same length.
     * Write the keys to the page without a delimiter.
     * @param cursor
     */
    @Override
    protected void sameLengthKeySerialization(PageCursor cursor){
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(Long[] key : keys){
            for(Long item : key){
                cursor.putLong(item);
            }
        }
    }

    /**
     * Serializes this nodes keys under the assumption that the keys can be any length.
     * Writes the keys to the page with a delimiter between keys.
     * @param cursor
     */
    @Override
    protected void variableLengthKeySerialization(PageCursor cursor) {
        cursor.setOffset(NODE_HEADER_LENGTH);
        for (Long[] key : keys) {
            for (Long item : key) {
                cursor.putLong(item);
            }
            cursor.putLong(DELIMITER_VALUE);
        }
    }

    /**
     * Determines if this node has space for another key.
     * Checks the byte representation size and compares it to the size of a page.
     * @return true if a new key can fit in this node.
     */
    @Override
    protected boolean notFull(Long[] newKey){
        int byte_representation_size = NODE_HEADER_LENGTH;
        //If the keys are the same length, there is no delimiter value. Need to check if this new key is of the same length.
        if(sameLengthKeys && sameKeyLength(newKey)){ //the keys here are the same length already AND the new key is of the same length
            byte_representation_size += (keys.size() + 1) * newKey.length * 8; //(Number of keys including the new one) * the number of longs in these keys * 8 bytes for each long.
        }
        else{ //Else, calculate the size including delimiters.
                for(Long[] key : keys){
                    byte_representation_size += (key.length + 1) * 8; //The number of longs in this key plus a delimiter * 8 bytes for each long = the byte size of this key.
                }
        }
        return byte_representation_size <= Tree.PAGE_SIZE;
    }

    /**
     *
     * @param search_key The key to use as a search parameter.
     * @return The first key matching this search parameter.
     */
    public Long[] find(Long[] search_key){
        for(Long[] key : keys){
            if (keyComparator.compare(key, search_key) == 0) { return keys.get(keys.indexOf(key)); } //returns the index of the correct pointer to the next block.
        }
        return new Long[]{}; //Did not find anything
    }

    /**
     * Returns the index of the correct pointer to the next block
     * @param search_key the key being searched for.
     * @return
     */
    @Override
    protected int search(Long[] search_key){
        for(Long[] key : keys){
            if (keyComparator.compare(key,search_key) >= 0) { return keys.indexOf(key); }
        }
        return -1;
    }

    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    @Override
    public SplitResult insert(Long[] key) throws IOException {

        /**
         * If this block is full, we must split it and insert the new key
         */
        if (notFull(key)){
            //There is room to insert the new key in this block without splitting.
            this.insertNotFull(key);
            tree.writeNodeToPage(this);
        }
        else{
            insertNotFull(key);//put the key in here for the split operation about to happen.
            int midPoint = keys.size() / 2;
            LeafNode sibling = tree.createLeafNode();

            sibling.keys.addAll(keys.subList(midPoint,keys.size()));
            keys.subList(midPoint, keys.size()).clear();

            //Rearrange sibling id's.
            sibling.siblingID = this.siblingID;
            this.siblingID = sibling.id;
            tree.writeNodeToPage(this);
            tree.writeNodeToPage(sibling);
            return new SplitResult(sibling.keys.get(0), this.id, sibling.id);
        }

        return null; //No split result since we did not split. Calling function checks for nulls.
    }
    protected void insertNotFull(Long[] key){
        keys.add(key);
        Collections.sort(keys, keyComparator);
    }
}
