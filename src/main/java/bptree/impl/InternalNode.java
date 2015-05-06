package bptree.impl;

import bptree.Cursor;
import bptree.RemoveResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by max on 2/13/15.
 */
public class InternalNode extends Node {

    public ArrayList<Long> children = new ArrayList<>();

    public InternalNode(Tree tree, long id) throws IOException {
        this(tree, id, new ArrayList<>(), new ArrayList<>());
    }
    public InternalNode(Tree tree, long id, ArrayList<Long[]> keys) throws IOException {
        this(tree, id, keys, new ArrayList<>());
    }
    public InternalNode(Tree tree, long id, ArrayList<Long[]> keys, ArrayList<Long> children) throws IOException {
        this.tree = tree;
        this.id = id;
        this.keys = keys;
        this.children = children;
        tree.writeNodeToPage(this);
    }

    private InternalNode(ByteBuffer buffer, Tree tree, Long id) throws IOException{
        this.tree = tree;
        this.id = id;
        keys = new ArrayList<>();
        children = new ArrayList<>();
        deserialize(buffer);
    }

    public static Node instantiateNodeFromBuffer(ByteBuffer buffer, Tree tree, long id) throws IOException {
        return new InternalNode(buffer, tree, id);
    }

    /**
     * Parses a page of an Internal Node under the assumption that keys are the same length without any delimiter between keys.
     * @param buffer
     */
    @Override
    protected void sameLengthKeyDeserialization(ByteBuffer buffer){
        int keyLength = NodeHeader.getKeyLength(buffer);
        int numberOfKeys = NodeHeader.getNumberOfKeys(buffer);
        //ArrayList<Long> newKey = new ArrayList<>();
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        //Read all of the children id values
        //for(int i = 0; (numberOfKeys > 0) && i < (numberOfKeys + 1); i++){ //There is +1 children ids more than the number of keys
        for(int i = 0; i < (numberOfKeys + 1); i++){ //There is +1 children ids more than the number of keys
            children.add(buffer.getLong());
        }
        for(int i = 0; i < numberOfKeys; i++){
            Long[] newKey = new Long[keyLength];
            for(int j = 0; j < keyLength; j++){
                newKey[j] = (buffer.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            //keys.add(newKey.toArray(new Long[newKey.size()]));
            keys.add(newKey);
            //newKey.clear(); //clear if for the next round.
        }
    }


    /**
     * Parses a page under the assumption that each key length is unknown and the keys are -1 delimited.
     * @param buffer
     */
    @Override
    protected void variableLengthKeyDeserialization(ByteBuffer buffer){
        int numberOfKeys = NodeHeader.getNumberOfKeys(buffer);
        ArrayList<Long> newKey = new ArrayList<>();
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; i < numberOfKeys + 1; i++){ //There is +1 children ids more than the number of keys
            children.add(buffer.getLong());
        }
        Long nextValue = buffer.getLong();
        for(int i = 0; (numberOfKeys > 0) && (i < numberOfKeys); i++){ //check if we are at the final end of the block
            while(nextValue != KEY_DELIMITER) { //while still within this key
                newKey.add(nextValue);
                nextValue = buffer.getLong();
            }
            keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear();
            if(i + 1 < numberOfKeys) {
                nextValue = buffer.getLong();//Without this check, there are problems for the last value being at the very end of the block.
            }
        }
    }

    @Override
    protected void sameLengthKeySerialization(ByteBuffer buffer) {
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        children.forEach(buffer::putLong);
        for(Long[] key : keys){
            for(Long item : key){
                buffer.putLong(item);
            }
        }
    }

    @Override
    protected void variableLengthKeySerialization(ByteBuffer buffer) {
        buffer.position(NodeHeader.NODE_HEADER_LENGTH);
        children.forEach(buffer::putLong);
        for (Long[] key : keys){
            for (Long item : key){
                buffer.putLong(item);
            }
            buffer.putLong(KEY_DELIMITER);
        }

    }
    @Override
    protected ByteBuffer serializeHeaderToBuffer(ByteBuffer buffer){
        buffer.put(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 2);
        buffer.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, sameLengthKeys ? (keys.size() > 0 ? keys.get(0).length : 0) : -1);
        //buffer.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, keys.size());
        if(( keys.size() == 0) && (children.size() == 0)){
            buffer.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        }
        else{
            buffer.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, keys.size());
        }
        buffer.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, followingNodeId);
        buffer.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, precedingNodeId);
        return buffer;
    }

    /**
     * Determines if the byte representation of this block with the new key added is still
     * small enough to fit in a pages worth of bytes. If so, the variable sizeInBytes is updated to reflect
     *
     * When a new key is added to an Internal Node, there is 1 new key added and 2 children pointers.
     *
     * @param newKey The new key to be added to the block.
     * @return True if this key can be added will still allowing for this block to fit in a page.
     */
    @Override
    protected boolean notFull(Long[] newKey) {
        return byteRepresentationSize(newKey) <= DiskCache.PAGE_SIZE;
    }

    public int byteRepresentationSize(Long[] newKey){
        int byte_representation_size = NodeHeader.NODE_HEADER_LENGTH;

        byte_representation_size += (children.size() + 1) * 8; //Each child * 8bytes, +1 for the new child

        //If the keys are the same length, there is no delimiter value. Need to check if this new key is of the same length.
        if(hasSameKeyLength(newKey)){ //the keys here are the same length already AND the new key is of the same length
            byte_representation_size += (keys.size() + 1) * newKey.length * 8; //(Number of keys including the new one) * the number of longs in these keys * 8 bytes for each long.
        }
        else{ //Else, calculate the size including delimiters.
            for(Long[] key : keys){
                byte_representation_size += (key.length + 1) * 8; //The number of longs in this key plus a delimiter * 8 bytes for each long = the byte size of this key.
            }
            byte_representation_size += (newKey.length + 1) *8; // the new key
        }
        return byte_representation_size;
    }

    /**
     * returns the index of the correct pointer to the next block.
     * @param search_key The key to search the position for.
     * @return the index.
     */
    @Override
    public int search(Long[] search_key){
            for (int i = 0; i < keys.size(); i++){
            if (keyComparator.prefixCompare(search_key, keys.get(i)) < 0) { return i; }
        }
        return keys.size(); //Then the position is the last one.
    }

    /**
     * Determines which child node leads to the specified key, and calls find on that node.
     * @param search_key
     * @return A single key matching the search key.
     * @throws IOException
     */
    @Override
    public Cursor find(Long[] search_key) throws IOException {
        if(children.size() == 0){
            throw new IOException("No Children in internal node");
        }
        return tree.getNode(children.get(search(search_key))).find(search_key);
    }

    @Override
    public RemoveResult remove(Long[] search_key) throws IOException {
        RemoveResult removeResult = tree.getNode(children.get(search(search_key))).remove(search_key);
        boolean thisNodeChanged = handleModifiedChildNodes(removeResult);
        if(thisNodeChanged){
            addNodeToRemoveResult(this, removeResult);
        }
        return removeResult;
    }

    protected boolean handleModifiedChildNodes(RemoveResult removeResult) throws IOException {
        for(Long[] mergedPairs : removeResult.getMergedNodes()){
            Long childIdObjFromChildrenList = getChild(mergedPairs[0]);
            if(childIdObjFromChildrenList != null){
                updateToReflectCurrentStateOfChild(mergedPairs);
                removeResult.declarePairHasBeenTakenCareOf(mergedPairs[0], mergedPairs[1]);
            }
        }
        if(removeResult.containsNodesWhichRequireAttention()){
            passRemoveResultToSibling(removeResult);
        }
        if(children.size() == 0){
            return true;
        }
        return false;
    }

    private RemoveResult passRemoveResultToSibling(RemoveResult removeResult) throws IOException {
        InternalNode sibling = (InternalNode)tree.getNode(followingNodeId);
        boolean siblingChanged = sibling.handleModifiedChildNodes(removeResult);
        if(siblingChanged) {
            addNodeToRemoveResult(sibling, removeResult);
        }
        return removeResult;
    }

    private void addNodeToRemoveResult(InternalNode node, RemoveResult removeResult){
        removeResult.addMergedNodes(node.id, node.followingNodeId, false);
    }

    private Long getChild(Long desiredChild){
        for(Long childId : children){
            if(childId.equals(desiredChild)){
                return childId;
            }
        }
        return null;
    }

    private void updateToReflectCurrentStateOfChild(Long[] mergedPair) throws IOException {
        if(mergedPair[2].equals(MergedNodesList.LEAF_NODES)){
            //Delete Child Pointer to deleted child
            //DELETE the key which divides/divided deleted child and mergedIntoChild
            int index = children.indexOf(getChild(mergedPair[0]));
            if(index == keys.size() && keys.size() > 0 && children.size() > 1){
                keys.remove(index -1); //More than 1 child, but the right-most child is deleted.
            }
            else if(index < keys.size()){ // There exists a key to delete
                keys.remove(index);
            }
            children.remove(index);
        }
        else{//Internal Nodes
            //Delete Child Pointer to deleted child
            //DRAG (MOVE) the key which divides/divided deleted child and mergedIntoChild into mergedIntoChild.
            int index = children.indexOf(getChild(mergedPair[0]));
           /* if(index < keys.size()){ // There exists a key to delete TODO possible bug here, simialr to the fix done in the LeafNode case.
                Long[] movingKey = keys.get(index);
                keys.remove(index);
                Node mergedIntoChild = tree.getNode(mergedPair[1]);
                mergedIntoChild.addKey(movingKey);
            }
            */
            if(index == keys.size() && keys.size() > 0 && children.size() > 1){
                keys.remove(index -1); //More than 1 child, but the right-most child is deleted.
            }
            else if(index < keys.size()){ // There exists a key to delete
                keys.remove(index);
            }
            children.remove(index);
        }
        this.writeNodeToPage();

    }

    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    @Override
    public SplitResult insert(Long[] key) throws IOException {
        int index = search(key);
        SplitResult result = tree.getNode(children.get(index)).insert(key);

        if(result != null) { //Only insert into this block if there is a split result from the child.
            return insertFromResult(result);
        }
        return null;
    }

    public SplitResult insertFromResult(SplitResult result) throws IOException {
        updateSameLengthKeyFlag(result.key);
        /**
         * If this block is full, we must split it and insert the new key
         */
        if (notFull(result.key)){
            //There is room to insert the new key in this block without splitting.
            this.insertNotFull(result);
            tree.writeNodeToPage(this);
        }
        else{
            insertNotFull(result); //insert into this node anyways, to prepare for the splitting below.

            int midPoint = keys.size() / 2;
            InternalNode sibling = tree.createInternalNode();
            sibling.keys = new ArrayList<>(keys.subList(midPoint, keys.size()));
            keys = new ArrayList<>(keys.subList(0, midPoint));

            sibling.children = new ArrayList<>(children.subList(midPoint + 1, children.size()));
            children = new ArrayList<>(children.subList(0, midPoint + 1));

            sibling.followingNodeId = this.followingNodeId;
            sibling.precedingNodeId = this.id;
            if(!this.followingNodeId.equals(-1l)) {
                Node secondSibling = tree.getNode(followingNodeId);
                secondSibling.setPrecedingNodeId(sibling.id);
            }
            this.followingNodeId = sibling.id;

            this.determineIfKeysAreSameLength();
            sibling.determineIfKeysAreSameLength();

            SplitResult newResult = new SplitResult(sibling.keys.remove(0), this.id, sibling.id);

            tree.writeNodeToPage(this);
            tree.writeNodeToPage(sibling);
            return newResult;
        }

        return null; //No split result since we did not split. Calling function checks for nulls.
    }

    /**
     * Inserts a key and child into this node when there is sufficient space for it.
     * @param result
     */
    protected void insertNotFull(SplitResult result) throws IOException {
        int index = search(result.key);
        if(index == keys.size()){
            //Insertion at the end of the array.
            keys.add(result.key);
            //children.add(result.left);
            children.add(result.right);
        }
        else{ //Insertion somewhere within the array. Need to shift elements in array to make room.
            children.set(index, result.left);
            children.add(index + 1, result.right);
            keys.add(index, result.key);
        }
    }
}
