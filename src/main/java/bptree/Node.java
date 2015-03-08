package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.util.LinkedList;

/**
 * Created by max on 2/12/15.
 */
public abstract class Node {

    /*
    The Header of the block when stored as bytes is:
    (1) Byte - Is this a leaf block?
    (4) int - The size of the keys, only relevant if keys are the same length.
    (4) int - the number of values representing child node ids.
     */
    protected final int NODE_HEADER_LENGTH = 1 + 4 + 4 + 8;
    private final int NODE_FOOTER_LENGTH = 1; //TODO determine if I need a footer.
    private final int BYTE_POSITION_NODE_TYPE = 0;
    private final int BYTE_POSITION_KEY_LENGTH = 1;
    private final int BYTE_POSITION_KEY_COUNT = 5;
    private final int BYTE_POSITION_SIBLING_ID = 9;


    //TODO Rewrite this size checking of the block to account for variable size keys
    protected int num = 0; // The current number of elements in the block
    public LinkedList<Long[]> keys;
    public long id;
    public long siblingID = -1;
    public int sizeInBytes = 0;
    public Tree tree;

    /**
     * Reads the first byte at the cursor to determine the block type
     * @param cursor
     * @return
     */
    protected boolean parseHeaderForNodeTypeFlag(PageCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE) == (byte) 1;
    }

    /**
     * Read a byte from the header to determine if this block contains keys of the same length.
     * If so, the keys can be written without a delimiter, which can increase the number of keys
     * which can fit in a page quite a lot. (For a page holding keys with k = 2, an additional 50~ keys will fit in a page this way.)
     * @param cursor
     * @return true if the keys here are of the same length.
     */
    protected boolean sameLengthKeys(PageCursor cursor){
        return parseHeaderForKeyLength(cursor) != -1;
    }

    /**
     * Reads an int from the header to determine how many keys are here.
     * @param cursor
     * @return the number of child ids stored in this page
     */
    protected int parseHeaderForNumberOfKeys(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_COUNT);
    }

    /**
     * Reads an int from the page header to determine the length of the keys stored in this block.
     * If keys in this block are various lengths, then this value is -1.
     * @param cursor
     * @return The length of the keys in this block, or -1 if they are not the same length.
     */
    protected int parseHeaderForKeyLength(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_LENGTH);
    }

    /**
     * Reads a long from the page header to determine the id of the next page id.
     * THis is only something you would use on the leaf nodes to do a sequential scan of values across multiple blocks.
     * This is basically a pointer to the next node.
     * @param cursor
     * @return the page id for the next node.
     */
    protected long parseHeaderForSiblingID(PageCursor cursor){return cursor.getLong(BYTE_POSITION_SIBLING_ID);}

    /**
     * Checks if this key has the same length at the first key in the list of keys.
     * @param key
     * @return
     */
    protected boolean sameKeyLength(Long[] key){return (keys.size() == 0) || keys.get(1).length == key.length;}


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


    public String toString(){
        String strRep = this instanceof InternalNode ? "Internal bptree.Block" : "Leaf bptree.Block";
        strRep += "\nbptree.Block ID: " + this.nodeID + "\n Keys: ";
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
