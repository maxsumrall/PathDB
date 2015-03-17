package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by max on 2/13/15.
 */
public class InternalNode extends Node {

    public LinkedList<Long> children = new LinkedList<>();

    public InternalNode(Tree tree, long id) throws IOException {
        this(new LinkedList<Long[]>(), new LinkedList<Long>(), tree, id);
    }
    public InternalNode(LinkedList<Long[]> k, Tree tree, long id) throws IOException {
        this(k, new LinkedList<Long>(), tree, id);
    }
    public InternalNode(LinkedList<Long[]> keys, LinkedList<Long> children, Tree tree, long id) throws IOException {
        this.keys = keys;
        this.children = children;
        this.tree = tree;
        this.id = id;
        tree.writeNodeToPage(this);
    }

    public InternalNode(ByteBuffer buffer, Tree tree, Long id) throws IOException{
        this.tree = tree;
        this.id = id;
        deserialize(buffer);

    }

    /**
     * Parses a page of an Internal Node under the assumption that keys are the same length without any delimiter between keys.
     * @param cursor
     */
    @Override
    protected void sameLengthKeyDeserialization(ByteBuffer buffer){
        int keyLength = parseHeaderForKeyLength(buffer);
        int numberOfKeys = parseHeaderForNumberOfKeys(buffer);
        LinkedList<Long[]> deserialized_keys = new LinkedList<>();
        LinkedList<Long> deserialized_children = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        buffer.position(NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; (numberOfKeys > 0) && i < (numberOfKeys + 1); i++){ //There is +1 children ids more than the number of keys
            deserialized_children.add(buffer.getLong());
        }
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++){
                newKey.add(buffer.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            deserialized_keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear(); //clear if for the next round.
        }
        this.keys = deserialized_keys;
        this.children = deserialized_children;
    }


    /**
     * Parses a page under the assumption that each key length is unknown and the keys are -1 delimited.
     * @param buffer
     */
    @Override
    protected void variableLengthKeyDeserialization(ByteBuffer buffer){
        int numberOfKeys = parseHeaderForNumberOfKeys(buffer);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> deserialized_children = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        buffer.position(NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; i < numberOfKeys + 1; i++){ //There is +1 children ids more than the number of keys
            deserialized_children.add(buffer.getLong());
        }
        Long nextValue = buffer.getLong();
        for(int i = 0; (numberOfKeys > 0) && (i < numberOfKeys); i++){ //check if we are at the final end of the block
            while(nextValue != DELIMITER_VALUE) { //while still within this key
                newKey.add(nextValue);
                nextValue = buffer.getLong();
            }
            deserialize_keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear();
            nextValue = buffer.getLong();
        }
        this.children = deserialized_children;
        this.keys = deserialize_keys;
    }

    @Override
    protected void sameLengthKeySerialization(ByteBuffer buffer) {
        buffer.position(NODE_HEADER_LENGTH);
        children.forEach(buffer::putLong);
        for(Long[] key : keys){
            for(Long item : key){
                buffer.putLong(item);
            }
        }
    }

    @Override
    protected void variableLengthKeySerialization(ByteBuffer buffer) {
        buffer.position(NODE_HEADER_LENGTH);
        children.forEach(buffer::putLong);
        for (Long[] key : keys){
            for (Long item : key){
                buffer.putLong(item);
            }
            buffer.putLong(DELIMITER_VALUE);
        }

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
        return byte_representation_size(newKey) <= Tree.PAGE_SIZE;
    }

    public int byte_representation_size(Long[] newKey){
        int byte_representation_size = NODE_HEADER_LENGTH;

        byte_representation_size += (children.size() + 1) * 8; //Each child * 8bytes, +1 for the new child

        //If the keys are the same length, there is no delimiter value. Need to check if this new key is of the same length.
        if(sameLengthKeys && sameKeyLength(newKey)){ //the keys here are the same length already AND the new key is of the same length
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
    protected int search(Long[] search_key){
            for (int i = 0; i < keys.size(); i++){
            if (keyComparator.prefixCompare(search_key, keys.get(i)) <= 0) { return i; }
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
        return tree.getNode(children.get(search(search_key))).find(search_key);
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
            sibling.keys = new LinkedList<>(keys.subList(midPoint, keys.size()));
            keys = new LinkedList<>(keys.subList(0, midPoint));

            sibling.children = new LinkedList<>(children.subList(midPoint + 1, children.size()));
            children = new LinkedList<>(children.subList(0, midPoint + 1));

            this.determineAndSetSameLengthKeysFlag();
            sibling.determineAndSetSameLengthKeysFlag();

            SplitResult newResult = new SplitResult(sibling.keys.pop(), this.id, sibling.id);

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
