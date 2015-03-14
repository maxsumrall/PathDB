package bptree;

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by max on 2/12/15.
 */
public abstract class Node {

    /*
    The Header of the block when stored as bytes is:
    (1) Byte - Is this a leaf block?
    (4) int - The size of the keys, only relevant if keys are the same length.
    (4) int - the number of keys in this node. A slightly delicate topic in the internal nodes, since they also contain child node ids and not just keys.
    (8) long - the id to the next node, the sibling node.
     */
    protected static final int NODE_HEADER_LENGTH = 1 + 4 + 4 + 8;
    private static final int NODE_FOOTER_LENGTH = 1; //TODO determine if I need a footer. I don't think so.
    private static final int BYTE_POSITION_NODE_TYPE = 0;
    private static final int BYTE_POSITION_KEY_LENGTH = 1;
    private static final int BYTE_POSITION_KEY_COUNT = 5;
    private static final int BYTE_POSITION_SIBLING_ID = 9;
    protected static final long DELIMITER_VALUE = -1; //The value between keys when variable length keys are written to a page.
    protected static final Key keyComparator = new Key(); //Used for doing comparison between keys.
    public static final int LEAF_FLAG = 1;

    //TODO Rewrite this size checking of the block to account for variable size keys
    protected LinkedList<Long[]> keys;
    protected long id;
    protected long siblingID = -1;
    protected Tree tree;
    protected boolean sameLengthKeys = true;

    /**
     * Reads the first byte at the cursor to determine the block type
     * @param cursor
     * @return
     */
    protected static int parseHeaderForNodeTypeFlag(PageCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE);
    }

    /**
     * Read a byte from the header to determine if this block contains keys of the same length.
     * If so, the keys can be written without a delimiter, which can increase the number of keys
     * which can fit in a page quite a lot. (For a page holding keys with k = 2, an additional 50~ keys will fit in a page this way.)
     * @param cursor
     * @return true if the keys here are of the same length.
     */
    protected boolean parseHeaderForSameLengthKeys(PageCursor cursor){
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
    protected boolean sameKeyLength(Long[] key) {
        if (keys.size() == 0) {
            return true;
        } else if (keys.getFirst().length == key.length) {
            return true;
        }
        return false;
    }
    protected boolean updateSameLengthKeyFlag(Long[] key){
        if(sameLengthKeys){
            if(keys.size() > 0 && key.length != keys.getFirst().length){
                sameLengthKeys = false;
            }
        }
        return sameLengthKeys;
    }

    protected void determineAndSetSameLengthKeysFlag(){
        sameLengthKeys = true;
        for(int i = 0 ; i < keys.size(); i++){
            if (keys.getFirst().length != keys.get(i).length){ sameLengthKeys = false; }
        }
    }

    /**
     * Write the header of this block to the cursor given.
     * @param cursor
     */
    protected void serializeHeader(PageCursor cursor){
        cursor.putByte(BYTE_POSITION_NODE_TYPE, (byte) (this instanceof LeafNode ? 1 : 2));
        cursor.putInt(BYTE_POSITION_KEY_LENGTH, sameLengthKeys ? (keys.size() > 0 ? keys.getFirst().length : 0) : -1);
        cursor.putInt(BYTE_POSITION_KEY_COUNT, keys.size());
        cursor.putLong(BYTE_POSITION_SIBLING_ID, siblingID);
    }

    /**
     * Reads from the header of this cursor for node variables.
     * @param cursor
     */
    protected void parseHeader(PageCursor cursor){
        this.sameLengthKeys = parseHeaderForSameLengthKeys(cursor);
        this.siblingID = parseHeaderForSiblingID(cursor);
    }


    /**
     * Writes the keys to the page cache
     * @param cursor to write with
     */
    public void serialize(PageCursor cursor){
        serializeHeader(cursor);
        if(sameLengthKeys){
            sameLengthKeySerialization(cursor);
        }
        else{
            variableLengthKeySerialization(cursor);
        }
    }
    /**
     * Parses a node from the specified cursor, initializing this nodes variables.
     * @param cursor
     */
    protected void deserialize(PageCursor cursor){
        parseHeader(cursor);
        if(parseHeaderForSameLengthKeys(cursor)){
            sameLengthKeyDeserialization(cursor);
        }
        else{
            variableLengthKeyDeserialization(cursor);
        }
    }

    public SplitResult getSplitResult(Long[] k, Long left, Long right){
        return new SplitResult(k, left, right);
    }


    /**
     * Returns a string representation of this node.
     * @return
     */
    public String toString(){
        String strRep = this instanceof InternalNode ? "Internal bptree.Block" : "Leaf bptree.Block";
        strRep += "\nbptree.Block ID: " + this.id + "\n Keys: ";
        for (Long[] k : keys){strRep += k + ", ";}
        if(this instanceof InternalNode) {
            strRep += "\n Children: ";
            for (long ch : ((InternalNode) this).children)
                strRep += ch + ", ";
        }
        return strRep + "\n";
    }


    abstract protected void sameLengthKeyDeserialization(PageCursor cursor);

    abstract protected void variableLengthKeyDeserialization(PageCursor cursor);

    abstract protected void sameLengthKeySerialization(PageCursor cursor);

    abstract protected void variableLengthKeySerialization(PageCursor cursor);

    abstract protected boolean notFull(Long[] newKey);

    abstract protected int search(Long[] key);

    abstract public Long[] find(Long[] key) throws IOException;

    abstract public SplitResult insert(Long[] key) throws IOException;


    public class SplitResult {
        public final Long[] key;
        public final long left;
        public final long right;
        public SplitResult(Long[] k, long l, long r){key = k; left = l; right = r;}
    }



}
