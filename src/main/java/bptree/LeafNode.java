package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;

/**
 * Created by max on 2/13/15.
 */
public class LeafNode extends Node {

    public LeafNode(BlockManager blockManager, long ID){
        this(new Key[blockManager.KEYS_PER_LBLOCK], blockManager, ID); //Something about the nulls here worries me. Might be a future problem.
    }
    public LeafNode(Key[] k, BlockManager blockManager, long ID){
        this.keys = k;
        blockManagerInstance = blockManager;
        this.blockID = ID;
    }


    public LeafNode(PageCursor cursor, BlockManager blockManager, long id) throws IOException {
        this.keys = new Key[blockManager.KEYS_PER_LBLOCK];
        this.blockManagerInstance = blockManager;
        this.blockID = id;

        cursor.setOffset(Node.HEADER_OFFSET); //set it to the beginning after header
        int keyIndex = 0;
        while(cursor.getLong() != -1l){ //check if we are at the final end of the block
            cursor.setOffset(cursor.getOffset() - 8); //rewind since we are still reading valid data
            int keyLength = 0;
            while(cursor.getLong() != -1){ //while still within this key
                keyLength++;
            }
            //Key Length determined, make key array to hold it all.
            long[] keyVals = new long[keyLength];
            cursor.setOffset(cursor.getOffset() - (8 * keyLength) - 8); //move cursor back to read those keys for real this time
            int i = 0;
            while(cursor.getLong() != -1l){
                keyVals[i++] = cursor.getLong(cursor.getOffset() - 8); //get the long I just read in the previous line.
            }
            this.keys[keyIndex++] = new Key(keyVals);
        }


    }


    /**
     *
     * @param key The key to use as a search parameter.
     * @return The set of values matching this key. If only a small portion of the key is specific this could be a lot of stuff.
     */
    protected Key[] get(Key key){
        int beginningIndex = 0;
        int endIndex = num;
        for(int i = 0; i < num; i++){
            if (keys[i].compareTo(key) == 0) { return new Key[]{keys[i]}; } //returns the index of the correct pointer to the next block.
        }
        return new Key[]{}; //Did not find anything
    }
    protected int search(Key key){

        for(int i = 0; i < num; i++){
            if (keys[i].compareTo(key) >= 0) { return i; } //returns the index of the correct pointer to the next block.
        }
        return num; //The index into the list of child blocks to follow next.
    }

    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    public SplitResult insert(Key key){
        int index = search(key);
        /**
         * If this block is full, we must split it and insert the new key
         */
        if (num >= BlockManager.KEYS_PER_LBLOCK){
            //System.out.println("Before Split: \n" + this);

            //Step 1. Create a new block and insert half of this blocks contents into the new block.
            // Step 2. Insert the key into the correct block, and bubbling up a new key.

            int midPoint = (BlockManager.KEYS_PER_LBLOCK + 1) / 2;
            int sNum = num - midPoint;
            LeafNode sibling = blockManagerInstance.createLBlock();
            sibling.num = sNum;
            System.arraycopy(this.keys, midPoint, sibling.keys, 0, sNum);
            Key[] empty = new Key[this.keys.length - midPoint];
            System.arraycopy(empty, 0, this.keys, midPoint, empty.length);

            this.num = midPoint;
            //Which block does the new key belong?
            //the new key is result.key
            if(index < midPoint){ //bptree.Key goes in new sibling block
                this.insertNotFull(key);
            }
            else{ //bptree.Key goes in this block
                sibling.insertNotFull(key);
            }
            //System.out.println("After Split, This \n" + this);
            //System.out.println("After Split, Sibling \n" + sibling);
            return new SplitResult(sibling.keys[0], this.blockID, sibling.blockID);


        }
        else{  //There is room to insert the new key in this block without splitting.
            this.insertNotFull(key);
        }

    return null; //No split result since we did not split. Calling function checks for nulls.
    }

    protected void insertNotFull(Key key){
        int index = search(key);
        assert(num < BlockManager.KEYS_PER_LBLOCK);

        if(index < num && keys[index].equals(key)){
            //Same
            keys[index] = key;
        }
        else{ //Insertion somewhere within the array. Need to shift elements in array to make room.
            System.arraycopy(keys, index, keys, index + 1, num - index);
            keys[index] = key;
        }
        num++;
    }


}
