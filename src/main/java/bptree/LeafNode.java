package bptree;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.util.ArrayList;
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

    private void deserialize(PageCursor cursor){
        //Read in the data from the page cache
        this.siblingID = parseHeaderForSiblingID(cursor);
        if(sameLengthKeys(cursor)){
            sameLengthKeyDeserialization(cursor);
        }
        else{
            variableLengthKeyDeserialization(cursor);
        }
    }

    /**
     * Parses a page under the assumption that keys are the same length without any delimiter between keys.
     * @param cursor
     */
    private void sameLengthKeyDeserialization(PageCursor cursor){
        int keyLength = parseHeaderForKeyLength(cursor);
        int numberOfKeys = parseHeaderForNumberOfKeys(cursor);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        Long nextValue = cursor.getLong();
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
    private void variableLengthKeyDeserialization(PageCursor cursor){
        cursor.setOffset(NODE_HEADER_LENGTH);
        LinkedList<Long[]> deserialize_keys = new LinkedList<>();
        LinkedList<Long> newKey = new LinkedList<>();
        Long nextValue = cursor.getLong();
        while(nextValue != -1){ //check if we are at the final end of the block
            while(nextValue != -1) { //while still within this key
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
     * Writes the keys to the page cache
     * @param cursor to write with
     */
    private void serialize(PageCursor cursor){
        cursor.setOffset(0);
        cursor.putLong(this.siblingID);
        for(Long[] key : keys){
            for(Long item : key){
                cursor.putLong(item);
            }
            cursor.putLong(-1);
        }
        cursor.putLong(-1);
    }

    private void propagateChangesToDisk() throws IOException {
        try (PageCursor cursor = tree.pagedFile.io(id, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    serialize(cursor);
                }
                while (cursor.shouldRetry());
            }
        }
    }

    /**
     *
     * @param key The key to use as a search parameter.
     * @return The set of values matching this key. If only a small portion of the key is specific this could be a lot of stuff.
     */
    protected Key[] get(Key key){
        for(Key mKey : keys){
            if (mKey.compareTo(key) == 0) { return new Key[]{keys.get(keys.indexOf(mKey))}; } //returns the index of the correct pointer to the next block.
        }
        return new Key[]{}; //Did not find anything
    }


    /**
     * Returns the index of the correct pointer to the next block
     * @param key
     * @return
     */
    protected int search(Key key){
        for(Key mKey : keys){
            if (mKey.compareTo(key) >= 0) { return keys.indexOf(mKey); }
        }
        return -1;
    }

    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    public SplitResult insert(Key key){

        /**
         * If this block is full, we must split it and insert the new key
         */
        if (keys.size() >= BlockManager.KEYS_PER_LBLOCK){
            insertNotFull(key);//put the key in here
            int midPoint = keys.size() / 2;
            LBlock sibling = blockManagerInstance.createLBlock();

            sibling.keys.addAll(keys.subList(midPoint,keys.size()));
            keys.subList(midPoint, keys.size()).clear();


            //Setup pointer to next block.
            sibling.nextBlock = nextBlock; //for the first block, there is no sibling already
            nextBlock = sibling.blockID;


            return new SplitResult(sibling.keys.get(0), this.blockID, sibling.blockID);

        }
        else{  //There is room to insert the new key in this block without splitting.
            this.insertNotFull(key);
        }

        return null; //No split result since we did not split. Calling function checks for nulls.
    }

    protected void insertNotFull(Key key){
        keys.add(key);
        Collections.sort(keys, key);

    }
}
