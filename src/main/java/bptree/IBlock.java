package bptree; /**
 * Created by max on 2/13/15.
 */
public class IBlock extends Block {

    protected long[] children = new long[BlockManager.CHILDREN_PER_IBLOCK]; //Leaf blocks don't have children

    public IBlock(BlockManager blockManager, long ID){
        this(new Key[blockManager.KEYS_PER_IBLOCK], blockManager, ID); //Something about the nulls here worries me. Might be a future problem.
    }
    public IBlock(Key[] k, BlockManager blockManager, long ID){
        this.keys = k;
        blockManagerInstance = blockManager;
        this.blockID = ID;
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
    public SplitResult insert(Key key, Key keynull){
        int index = search(key);
        SplitResult result = blockManagerInstance.getBlock(children[index]).insert(key);

        if(result != null){
            /**
             * If this block is full, we must split it and insert the new key
             */
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
    public SplitResult insert(Key key){
        int index = search(key);
        SplitResult result = blockManagerInstance.getBlock(children[index]).insert(key);

        if(result != null){
            /**
             * If this block is full, we must split it and insert the new key
             */
            if (num >= BlockManager.KEYS_PER_IBLOCK){
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
                IBlock sibling = blockManagerInstance.createIBlock();
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
