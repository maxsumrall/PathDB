package bptree; /**
 * Created by max on 2/12/15.
 */
public abstract class Block{

     // Internal blocks have different structure than leaf blocks, therefore they will have different number of elements.

    //TODO Rewrite this size checking of the block to account for variable size keys
    protected int num = 0; // The current number of elements in the block
    public Key[] keys;
    public long blockID;
    public long sizeInBytes = 0;
    public BlockManager blockManagerInstance;

    /**
     * Given a key, return the index where it belongs in the bptree.Block
     * If this is an INode, it represents the child node to follow.
     * This happens until the final leaf block is found.
     * @param key
     * @return
     */
    abstract protected int search(Key key);

    /**
     * Search for a key in the tree and return the full keys which match it.
     * @param key The search key
     * @return
     */
    abstract protected Key[] get(Key key);

    abstract public SplitResult insert(Key key);


    public Object DSgetValue(){
        String strRep = "[";
        for (Key k : keys) {
            if(k != null) {
                strRep += k.toString();
            }
        }
        strRep += "]";
        return strRep;

    }

    public String toString(){
        String strRep = this instanceof IBlock ? "Internal bptree.Block" : "Leaf bptree.Block";
        strRep += "\nbptree.Block ID: " + this.blockID + "\n Keys: ";
        for (Key k : keys){strRep += k + ", ";}
        if(this instanceof IBlock) {
            strRep += "\n Children: ";
            for (long ch : ((IBlock) this).children)
                strRep += ch + ", ";
        }
        return strRep + "\n";
    }



    class SplitResult {
        public final Key key;
        public final long left;
        public final long right;
        public SplitResult(Key k, long l, long r){key = k; left = l; right = r;}
    }



}
