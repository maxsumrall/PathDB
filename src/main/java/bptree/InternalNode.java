package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by max on 2/13/15.
 */
public class InternalNode extends Node {

    public LinkedList<Long> children = new LinkedList<Long>(); //Leaf blocks don't have children

    public InternalNode(Tree tree, long ID){
        this(new LinkedList<Long[]>(), tree, ID); //Something about the nulls here worries me. Might be a future problem.
    }
    public InternalNode(LinkedList<Long[]> k, Tree tree, long id){
        this.keys = k;
        this.tree = tree;
        this.id = id;
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
        this.id = id;
        int keyLength;
        int numOfChildPointers = Node.readNumberOfChildPointers(cursor);
        sizeInBytes = INTERNAL_NODE_HEADER_LENGTH + INTERNAL_NODE_FOOTER_LENGTH + (numOfChildPointers * 8);
        if(Node.identicalKeyLengthFromHeader(cursor)){
            keyLength = Node.readKeyLength(cursor);
            sizeInBytes += (keyLength * (numOfChildPointers - 1)) * 8;
            cursor.setOffset(INTERNAL_NODE_HEADER_LENGTH);
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
            cursor.setOffset(INTERNAL_NODE_HEADER_LENGTH);
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
        return blockManagerInstance.getBlock(children.get(search(key))).get(key);
    }

    /**
     * returns the index of the correct pointer to the next block.
     * @param key
     * @return the index.
     */
    protected int search(Key key){
        for (Key mKey : keys){
            if (mKey.compareTo(key) > 0) { return keys.indexOf(mKey); }
        }
        return keys.size(); //Then the position is the last one.
    }
    /**
     *
     * @param key
     * @return SplitResult: Null if no split
     *  Otherwise: The left and right blocks which replace this block, and the new key for the right block.
     */
    public SplitResult insert(Key key){
        int index = search(key);
        SplitResult result = blockManagerInstance.getBlock(children.get(index)).insert(key);

        if(result != null){
            /**
             * If this block is full, we must split it and insert the new key
             */
            if (keys.size() >= BlockManager.KEYS_PER_IBLOCK){
                //Step 1. Create a new block and insert half of this blocks contents into the new block.
                // Step 2. Insert the key into the correct block, adding the children nodes in the right place, and bubbling up a new key.

                insertNotFull(result);

                int midPoint = keys.size() / 2;
                IBlock sibling = blockManagerInstance.createIBlock();
                sibling.keys = new LinkedList<Key>(keys.subList(midPoint, keys.size()));
                keys = new LinkedList<Key>(keys.subList(0, midPoint));

                sibling.children = new LinkedList<Long>(children.subList(midPoint + 1, children.size()));
                children = new LinkedList<Long>(children.subList(0, midPoint + 1));

                return new SplitResult(sibling.keys.pop(), this.blockID, sibling.blockID);

            }
            else{  //There is room to insert the new key in this block without splitting.
                this.insertNotFull(result);
            }
        }
        return null; //No split result since we did not split. Calling function checks for nulls.
    }

    protected void insertNotFull(SplitResult result){
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
