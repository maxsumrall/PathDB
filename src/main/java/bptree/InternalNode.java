package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by max on 2/13/15.
 */
public class InternalNode extends Node {
    /*
    The Header of the block when stored as bytes is:
    (1) Byte - Is this a leaf block?
    (1) Byte - Is this a block where all keys are the same length?
    (4) int - the number of values representing child blocks.
    (4) int - The size of the keys, only relevant if keys are the same length.
     */
    public static int IBLOCK_HEADER_LENGTH = 1 + 1 + 4 + 4;
    public static int IBLOCK_FOOTER_LENGTH = 1;


    protected long[] children = new long[BlockManager.CHILDREN_PER_IBLOCK]; //Leaf blocks don't have children

    public InternalNode(BlockManager blockManager, long ID){
        this(new Key[blockManager.KEYS_PER_IBLOCK], blockManager, ID); //Something about the nulls here worries me. Might be a future problem.
    }
    public InternalNode(Key[] k, BlockManager blockManager, long ID){
        this.keys = k;
        blockManagerInstance = blockManager;
        this.blockID = ID;
    }

    /**
     * Construct an IBlock from a page
     * @param cursor
     * @param blockManager
     * @param id
     * @throws IOException
     */
    public InternalNode(PageCursor cursor, BlockManager blockManager, long id) throws IOException {
        this.keys = new Key[blockManager.KEYS_PER_LBLOCK]; //TODO change this to linkedlist
        this.blockManagerInstance = blockManager;
        this.blockID = id;
        int keyLength;
        int numOfChildPointers = Node.readNumberOfChildPointers(cursor);
        sizeInBytes = IBLOCK_HEADER_LENGTH + IBLOCK_FOOTER_LENGTH + (numOfChildPointers * 8);
        if(Node.identicalKeyLengthFromHeader(cursor)){
            keyLength = Node.readKeyLength(cursor);
            sizeInBytes += (keyLength * (numOfChildPointers - 1)) * 8;
            cursor.setOffset(IBLOCK_HEADER_LENGTH);
            for(int i = 0; i < numOfChildPointers; i++){
                children[i] = cursor.getLong();
            }
            for(int i = 0; i < numOfChildPointers - 1; i++) {
                this.keys[i] = new Key(new long[keyLength]);
                for (int j = 0; j < keyLength; j++) {
                    this.keys[i].vals[j] = cursor.getLong();
                }
            }
        }
        else{ //Keys are variable length within block, delimiter of (-1) is used.
            cursor.setOffset(IBLOCK_HEADER_LENGTH);
            for(int i = 0; i < numOfChildPointers; i++){
                children[i] = cursor.getLong();
            }
            cursor.setOffset(cursor.getOffset() - 8);//necessary precondition since in following loop will move it 8 bytes down.
            for(int i = 0; i < numOfChildPointers - 1; i++) {
                keyLength = 0; //reset this
                cursor.setOffset(cursor.getOffset() + 8);
                while(cursor.getLong() != -1l){ //while still within this key
                    keyLength++;
                }
                sizeInBytes += (keyLength + 1) * 8;// this key, plus a delimiter
                this.keys[i] = new Key(new long[keyLength]);
                cursor.setOffset(cursor.getOffset() - (8 * keyLength) - 8); //move cursor back to read those keys for real this time
                for(int j = 0; j < keyLength; j++) {
                    this.keys[i].vals[j] = cursor.getLong();
                }
            }
        }
    }

    public byte[] asByteArray(){
        int numChildren = numberOfChildren();
        ByteBuffer byteRep = ByteBuffer.allocate(BlockManager.PAGE_SIZE);
        byteRep.put((byte) 0); //Not a leaf block
        if(sameLengthKeys){
            byteRep.put((byte) 1); //keys are same length
            byteRep.putInt(numChildren);
            byteRep.putInt(keys[0].vals.length);//since all are same length this is fine
            for(int i = 0; i < numChildren; i++){
                byteRep.putLong(children[i]);
            }
            for(int i = 0; i < numChildren -1; i++){
                for(int j = 0; j < keys[0].vals.length; j++)
                byteRep.putLong(keys[i].vals[j]);
            }
        }
        else{
            byteRep.put((byte) 0); //keys are NOT same length
            byteRep.putInt(numChildren);
            byteRep.putInt(9); //variable key length, doesn't mean anything. useful number for debugging
            for(int i = 0; i < numChildren; i++){
                byteRep.putLong(children[i]);
            }
            for(int i = 0; i < numChildren -1; i++){
                for(int j = 0; j < keys[i].vals.length; j++) {
                    byteRep.putLong(keys[i].vals[j]);
                }
                byteRep.putLong(-1l); //Delimiter between variable length keys
            }

        }
        return byteRep.array();
    }

    public int numberOfChildren(){
        int numChildren = 0;
        for(int i = 0; children[i] != 0; i++){
            numChildren++;
        }
        return numChildren;
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
    private boolean isSpaceAvailableFor(Key newKey) {
        int futureSize = sizeInBytes //The current size
                + ((newKey.vals.length + 2) * 8) //an extra +2 to account for possible delimiters
                + 8; //The additional child pointer

        if(BlockManager.PAGE_SIZE < futureSize) {
            return false;
        }
        return true;
    }


    /**
     *
     * @param key The key to use as a search parameter.
     * @return The set of values matching this key. If only a small portion of the key is specific this could be a lot of stuff.
     */
    protected Key[] get(Key key){
        return blockManagerInstance.getBlock(children[search(key)]).get(key);
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
    /*public SplitResult insert(Key key){
        int index = search(key);
        SplitResult result = blockManagerInstance.getBlock(children[index]).insert(key);

        if(result != null){
            if (num >= BlockManager.KEYS_PER_IBLOCK){
                //Step 1. Create a new block and insert half of this blocks contents into the new block.
                // Step 2. Insert the key into the correct block, adding the children nodes in the right place, and bubbling up a new key.
                int midPoint = ((BlockManager.KEYS_PER_IBLOCK + 1) / 2) -1;
                int sNum = num - midPoint;
                IBlock sibling = blockManagerInstance.createIBlock();
                sibling.num = sNum;
                System.arraycopy(this.keys, midPoint, sibling.keys, 0, sNum);
                System.arraycopy(this.children, midPoint, sibling.children, 0, sNum+1);
                //clear this array after midpoint
                Key[] empty = new Key[this.keys.length - midPoint];
                long[] emptyC = new long[this.children.length - midPoint];
                System.arraycopy(empty, 0, this.keys, midPoint, empty.length);
                System.arraycopy(emptyC, 0, this.children, midPoint, emptyC.length);

                this.num = midPoint;

                //Which block does the new key belong?
                //the new key is result.key
                if(key.compareTo(sibling.keys[0]) >= 0){ //bptree.Key goes in new sibling block
                    sibling.insertNotFull(result);
                }
                else{ //bptree.Key goes in this block
                    this.insertNotFull(result);
                }
                System.out.println(sibling.keys[0]);
                SplitResult splitResult = new SplitResult(sibling.keys[0], this.blockID, sibling.blockID); //changing which key bubbles up
                //Shift sibling keys down. We don't want to keep around duplicate keys in internal nodes
                sibling.keys = shiftLeft(sibling.keys);
                System.out.println(sibling.keys[0]);
                return splitResult;

            }
            else{  //There is room to insert the new key in this block without splitting.
                this.insertNotFull(result);
            }
        }
        return null; //No split result since we did not split. Calling function checks for nulls.
    }
    */

    public SplitResult insert(Key key){
        int index = search(key);
        SplitResult result = blockManagerInstance.getBlock(children[index]).insert(key);

        if(result != null){
            /**
             * If this block is full, we must split it and insert the new key
             */
            if (isSpaceAvailableFor(key)){ //A check if there is room in this block. Must account for the byte representaton size
                //Step 1. Create a new block and insert half of this blocks contents into the new block.
                // Step 2. Insert the key into the correct block, adding the children nodes in the right place, and bubbling up a new key.
                Key[] newKeys = new Key[BlockManager.KEYS_PER_IBLOCK + 1];
                System.arraycopy(keys, 0, newKeys, 0, keys.length); //copy into new keys.
                keys = newKeys;
                long[] newChildren = new long[BlockManager.CHILDREN_PER_IBLOCK + 1];
                System.arraycopy(children, 0, newChildren, 0, children.length); //copy into new keys.
                children = newChildren;
                insertNotFull(result);// the key is now in this oversized block.

                int midPoint = ((BlockManager.KEYS_PER_IBLOCK + 1) / 2);
                int sNum = num - midPoint;
                InternalNode sibling = blockManagerInstance.createIBlock();
                sibling.num = sNum;
                System.arraycopy(this.keys, midPoint, sibling.keys, 0, sNum);
                System.arraycopy(this.children, midPoint, sibling.children, 0, sNum+1);
                //clear this array after midpoint
                Key[] empty = new Key[this.keys.length - midPoint];
                long[] emptyC = new long[this.children.length - midPoint];
                this.num = midPoint;
                //resize this back to normal size
                System.arraycopy(keys, 0, empty, 0, num);
                System.arraycopy(children, 0, emptyC, 0, num + 1);
                keys = empty;
                children = emptyC;

                SplitResult splitResult = new SplitResult(sibling.keys[0], this.blockID, sibling.blockID); //changing which key bubbles up
                //Shift sibling keys down. We don't want to keep around duplicate keys in internal nodes
                sibling.keys = shiftLeft(sibling.keys);
                return splitResult;

            }
            else{  //There is room to insert the new key in this block without splitting.
                this.insertNotFull(result);
            }
        }
        return null; //No split result since we did not split. Calling function checks for nulls.
    }

    protected void insertNotFull(SplitResult result){
        int index = search(result.key);

        if(index == num){
            //Insertion at the end of the array.
            keys[index] = result.key;
            children[index] = result.left;
            children[index + 1] = result.right;
        }
        else{ //Insertion somewhere within the array. Need to shift elements in array to make room.
            System.out.print(keys.length + " " + index + " " + (num - index) + "\n");
            System.arraycopy(keys, index, keys, index + 1, num - index);
            System.arraycopy(children, index, children, index + 1, num - index + 1); //+ 1 becasue of two children
            children[index]  = result.left;
            children[index + 1] = result.right;
            keys[index] = result.key;
        }
        num++;
    }
    static Key[] shiftLeft(Key[] arr) {
        System.arraycopy(arr, 1, arr, 0, arr.length - 1);
        return arr;
    }

}
