package bptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Created by max on 2/12/15.
 */
public abstract class Node {

     // Internal blocks have different structure than leaf blocks, therefore they will have different number of elements.

    //TODO Rewrite this size checking of the block to account for variable size keys
    protected int num = 0; // The current number of elements in the block
    public Key[] keys;
    public long blockID;
    public int sizeInBytes = 0;
    public boolean sameLengthKeys = true;
    public BlockManager blockManagerInstance;

    /**
     * Reads the first byte of the page to determine the block type
     * @param cursor
     * @return
     */
    public static boolean isLeafBlock(PageCursor cursor){
        return cursor.getByte(0) == (byte) 1;
    }

    public boolean sameKeyLength(Key key){return (keys.length == 0) || keys[1].vals.length == key.vals.length;}

    public static boolean identicalKeyLengthFromHeader(PageCursor cursor){
        return cursor.getByte(1) == (byte) 1;
    }

    public static int readNumberOfChildPointers(PageCursor cursor){
        return cursor.getInt(2);
    }
    public static int readKeyLength(PageCursor cursor){
        return cursor.getInt(6);
    }
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
        String strRep = this instanceof InternalNode ? "Internal bptree.Block" : "Leaf bptree.Block";
        strRep += "\nbptree.Block ID: " + this.blockID + "\n Keys: ";
        for (Key k : keys){strRep += k + ", ";}
        if(this instanceof InternalNode) {
            strRep += "\n Children: ";
            for (long ch : ((InternalNode) this).children)
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
