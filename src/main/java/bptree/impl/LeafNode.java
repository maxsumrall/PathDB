package bptree.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by max on 2/13/15.
 */
public class LeafNode extends Node {

    public LeafNode(Tree tree, long id) throws IOException {
        this(tree, id, new LinkedList<>());
    }
    public LeafNode(Tree tree, Long id, LinkedList<Long[]> k) throws IOException {
        this(tree, id, k, -1l);
    }

    public LeafNode(Tree tree, Long id, LinkedList<Long[]> keys, Long followingNodeID) throws IOException {
        this.tree = tree;
        this.id = id;
        this.keys = keys;
        this.followingNodeID = followingNodeID;
        determineIfKeysAreSameLength();
        tree.writeNodeToPage(this);
    }

    private LeafNode(ByteBuffer buffer, Tree tree, Long id) throws IOException {
        this.tree = tree;
        this.id = id;
        deserialize(buffer); //set the keys and the followingNodeID, as read from the page cache
    }

    public static Node instantiateNodeFromBuffer(ByteBuffer buffer, Tree tree, long id) throws IOException {
        return new LeafNode(buffer, tree, id);
    }

    /**
     * Parses a page of a Leaf Node under the assumption that keys are the same length without any delimiter between keys.
     * @param buffer
     */
    @Override
    protected void sameLengthKeyDeserialization(ByteBuffer buffer){
        int keyLength = NodeHeader.getKeyLength(buffer);
        int numberOfKeys = NodeHeader.getNumberOfKeys(buffer);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++){
                newKey.add(buffer.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            deserialize_keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear(); //clear if for the next round.
        }
        this.keys = deserialize_keys;
    }

    /**
     * Parses a page under the assumption that each key length is unknown and the keys are -1 delimited.
     * @param buffer
     */
    @Override
    protected void variableLengthKeyDeserialization(ByteBuffer buffer){
        int numberOfKeys = NodeHeader.getNumberOfKeys(buffer);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        Long nextValue = buffer.getLong();
        for(int i = 0; i < numberOfKeys; i++){ //check if we are at the final end of the block
            while(nextValue != KEY_DELIMITER) { //while still within this key
                newKey.add(nextValue);
                nextValue = buffer.getLong();
            }
            deserialize_keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear();
            if(i + 1 < numberOfKeys) {
                nextValue = buffer.getLong();//Without this check, there are problems for the last value being at the very end of the block.
            }
        }
        this.keys = deserialize_keys;
    }


    /**
     * Serializes this nodes keys under the assumption that the keys are the same length.
     * Write the keys to the page without a delimiter.
     * @param buffer
     */
    @Override
    protected void sameLengthKeySerialization(ByteBuffer buffer){
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        for(Long[] key : keys){
            for(Long item : key){
                buffer.putLong(item);
            }
        }
    }

    /**
     * Serializes this nodes keys under the assumption that the keys can be any length.
     * Writes the keys to the page with a delimiter between keys.
     * @param buffer
     */
    @Override
    protected void variableLengthKeySerialization(ByteBuffer buffer) {
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        for (Long[] key : keys) {
            for (Long item : key) {
                buffer.putLong(item);
            }
            buffer.putLong(KEY_DELIMITER);
        }
    }

    /**
     * Determines if this node has space for another key.
     * Checks the byte representation size and compares it to the size of a page.
     * @return true if a new key can fit in this node.
     */
    @Override
    protected boolean notFull(Long[] newKey){
        int byte_representation_size = NodeHeader.NODE_HEADER_LENGTH;
        //If the keys are the same length, there is no delimiter value. Need to check if this new key is of the same length.
        if(hasSameKeyLength(newKey)){ //the keys here are the same length already AND the new key is of the same length
            byte_representation_size += (keys.size() + 1) * newKey.length * 8; //(Number of keys including the new one) * the number of longs in these keys * 8 bytes for each long.
        }
        else{ //Else, calculate the size including delimiters.
                for(Long[] key : keys){
                    byte_representation_size += (key.length + 1) * 8; //The number of longs in this key plus a delimiter * 8 bytes for each long = the byte size of this key.
                }
            byte_representation_size += (newKey.length + 1) * 8;
        }
        return byte_representation_size < DiskCache.PAGE_SIZE;
    }

    /**
     *
     * @param search_key The key to use as a search parameter.
     * @return The first key matching this search parameter.
     */
    public CursorImpl find(Long[] search_key){
        for(Long[] key : keys){
            if (keyComparator.prefixCompare(search_key, key) == 0) { return new CursorImpl(tree, this, search_key, keys.indexOf(key));} //returns the index of the correct pointer to the next block.
        }
        return new CursorImpl(tree, this, search_key, 0); //Did not find anything
    }

    /**
     * Returns the index of the correct pointer to the next block
     * @param search_key the key being searched for.
     * @return
     */
    @Override
    protected int search(Long[] search_key){
        for(Long[] key : keys){
            if (keyComparator.prefixCompare(search_key, key) >= 0) { return keys.indexOf(key); }
        }
        return 0;
    }

    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    @Override
    public SplitResult insert(Long[] key) throws IOException {
        SplitResult splitResult = null;
        updateSameLengthKeyFlag(key);

        if (notFull(key)){
            addKeyAndSort(key);
        }
        else{
            addKeyAndSortWithoutPageWrite(key);
            int midPoint = keys.size() / 2;

            LinkedList<Long[]> siblingKeys = new LinkedList<>(keys.subList(midPoint,keys.size()));
            LeafNode sibling = tree.createLeafNode(siblingKeys, this.followingNodeID);

            LinkedList<Long[]> newKeys = new LinkedList<>(keys.subList(0, midPoint));
            updateAfterSplit(newKeys, sibling.id);

            splitResult = new SplitResult(sibling.keys.getFirst(), this.id, sibling.id);
        }
        return splitResult;
    }

    private void updateAfterSplit(LinkedList<Long[]> updatedKeyList, Long newSiblingID){
        keys = updatedKeyList;
        determineIfKeysAreSameLength();
        this.followingNodeID = newSiblingID;
        tree.writeNodeToPage(this);
    }

}
