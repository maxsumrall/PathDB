package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by max on 2/13/15.
 */
public class InternalNode extends Node {

    public LinkedList<Long> children = new LinkedList<>();

    public InternalNode(Tree tree, long ID){
        this(new LinkedList<Long[]>(), tree, ID);
    }
    public InternalNode(LinkedList<Long[]> k, Tree tree, long id){
        this.keys = k;
        this.tree = tree;
        this.id = id;
    }

    public InternalNode(PageCursor cursor, Tree tree, Long id) throws IOException{
        this.tree = tree;
        this.id = id;
        deserialize(cursor);

    }

    /**
     * Parses a page of an Internal Node under the assumption that keys are the same length without any delimiter between keys.
     * @param cursor
     */
    @Override
    protected void sameLengthKeyDeserialization(PageCursor cursor){
        int keyLength = parseHeaderForKeyLength(cursor);
        int numberOfKeys = parseHeaderForNumberOfKeys(cursor);
        LinkedList<Long[]> deserialized_keys = new LinkedList<>();
        LinkedList<Long> deserialized_children = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        cursor.setOffset(NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; i < numberOfKeys + 1; i++){ //There is +1 children ids more than the number of keys
            deserialized_children.add(cursor.getLong());
        }
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++){
                newKey.add(cursor.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            deserialized_keys.add((Long[]) newKey.toArray());
            newKey.clear(); //clear if for the next round.
        }
        this.keys = deserialized_keys;
        this.children = deserialized_children;
    }


    /**
     * Parses a page under the assumption that each key length is unknown and the keys are -1 delimited.
     * @param cursor
     */
    @Override
    protected void variableLengthKeyDeserialization(PageCursor cursor){
        int numberOfKeys = parseHeaderForNumberOfKeys(cursor);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> deserialized_children = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        cursor.setOffset(NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; i < numberOfKeys + 1; i++){ //There is +1 children ids more than the number of keys
            deserialized_children.add(cursor.getLong());
        }
        Long nextValue = cursor.getLong();
        for(int i = 0; i < numberOfKeys; i++){ //check if we are at the final end of the block
            while(nextValue != DELIMITER_VALUE) { //while still within this key
                newKey.add(nextValue);
                nextValue = cursor.getLong();
            }
            deserialize_keys.add((Long[]) newKey.toArray());
            newKey.clear();
            nextValue = cursor.getLong();
        }
        this.children = deserialized_children;
        this.keys = deserialize_keys;
    }

    @Override
    protected void sameLengthKeySerialization(PageCursor cursor) {
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(Long child : children){
            cursor.putLong(child);
        }
        for(Long[] key : keys){
            for(Long item : key){
                cursor.putLong(item);
            }
        }
    }

    @Override
    protected void variableLengthKeySerialization(PageCursor cursor) {
        cursor.setOffset(NODE_HEADER_LENGTH);
        for(Long child : children){
            cursor.putLong(child);
        }
        for (Long[] key : keys){
            for (Long item : key){
                cursor.putLong(item);
            }
            cursor.putLong(DELIMITER_VALUE);
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
        int byte_representation_size = NODE_HEADER_LENGTH;

        byte_representation_size += children.size() * 8; //Each child * 8bytes

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
     * returns the index of the correct pointer to the next block.
     * @param search_key The key to search the position for.
     * @return the index.
     */
    @Override
    protected int search(Long[] search_key){
        for (Long[] key : keys){
            if (keyComparator.compare(key, search_key) > 0) { return keys.indexOf(key); }
        }
        return keys.size(); //Then the position is the last one.
    }

    /**
     * Determines which child node leads to the specified key, and calls find on that node.
     * @param key
     * @return A single key matching the search key.
     * @throws IOException
     */
    @Override
    public Long[] find(Long[] key) throws IOException {
        return tree.getNode(search(key)).find(key);
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

        if(result != null){ //Only insert into this block if there is a split result from the child.
            /**
             * If this block is full, we must split it and insert the new key
             */
            if (notFull(key)){
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

                tree.writeNodeToPage(this);
                tree.writeNodeToPage(sibling);
                return new SplitResult(sibling.keys.pop(), this.id, sibling.id);
            }
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
            //children.add(index, result.left);
            children.add(index + 1, result.right);
            keys.add(index, result.key);
        }
    }
}
