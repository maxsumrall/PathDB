package bptree;

import java.io.FileNotFoundException;

/**
 * Created by max on 2/10/15.
 */
public class BPTree{

    public BlockManager bm;

    /**
     * Build the main.java.bptree.BPTree or initialize it.
     *
     *
     */
    public BPTree() throws FileNotFoundException {

        bm = new BlockManager();
    }

    public Key[] find(Key key){

        return bm.rootBlock.get(key);

    }

    /**
     * Get the root block and call insert on it.
     * If the root returns a split result, make a new block and set it as the root.
     * @param key
     */
    public void insert(Key key){
        Block.SplitResult result = bm.rootBlock.insert(key);
        if (result != null){ //Root block split.
            IBlock newRoot = bm.createIBlock();
            newRoot.num = 1;
            newRoot.keys[0] = result.key;
            newRoot.children[0] = result.left;
            newRoot.children[1] = result.right;
            bm.rootBlock = newRoot;
        }
    }


}
