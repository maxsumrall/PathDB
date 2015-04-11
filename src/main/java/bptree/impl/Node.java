package bptree.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by max on 2/12/15.
 */
public abstract class Node {
    protected static final long KEY_DELIMITER = -1;
    public static final KeyImpl keyComparator = KeyImpl.getComparator();
    protected Tree tree;
    protected NodeHeader nodeHeader;
    public LinkedList<Long[]> keys;
    public long id;
    protected long precedingNodeID = -1;
    protected long followingNodeID = -1;
    protected boolean sameLengthKeys = true;

    protected boolean hasSameKeyLength(Long[] newKey) {
        if (keys.size() == 0) {
            return true;
        }
        else if(!sameLengthKeys){
            return false;
        }
        else if (keys.getFirst().length == newKey.length) {
            return true;
        }
        else {
            return false;
        }
    }
    protected boolean updateSameLengthKeyFlag(Long[] newKey){
        if(sameLengthKeys){
            if(keys.size() > 0 && newKey.length != keys.getFirst().length){
                sameLengthKeys = false;
            }
        }
        return sameLengthKeys;
    }

    protected void determineIfKeysAreSameLength(){
        sameLengthKeys = true;
        for(int i = 0 ; i < keys.size(); i++){
            if (keys.getFirst().length != keys.get(i).length){ sameLengthKeys = false; }
        }
    }

    protected void addKey(Long[] newKey){
        keys.add(newKey);
        tree.writeNodeToPage(this);
    }

    protected void removeKey(Long[] removeKey){
        Long[] keyToRemove = null;
        for(Long[] key : keys){
            if(removeKey.equals(key)){
                keyToRemove = key;
            }
        }
        keys.remove(keyToRemove);
        tree.writeNodeToPage(this);
    }

    protected void addKeyAndSort(Long[] newKey){
        keys.add(newKey);
        Collections.sort(keys, keyComparator);
        tree.writeNodeToPage(this);

    }
    protected void addKeyAndSortWithoutPageWrite(Long[] newKey){
        keys.add(newKey);
        Collections.sort(keys, keyComparator);
    }


    /**
     * Write the header of this block to the buffer given.
     * @param buffer
     */
    protected ByteBuffer serializeHeaderToBuffer(ByteBuffer buffer){
        buffer.put(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) (this instanceof LeafNode ? 1 : 2));
        buffer.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, sameLengthKeys ? (keys.size() > 0 ? keys.getFirst().length : 0) : -1);
        buffer.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, keys.size());
        buffer.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, followingNodeID);
        return buffer;
    }

    /**
     * Writes the keys to the a ByteBuffer.
     */
    public ByteBuffer serialize(){
        return serialize(ByteBuffer.allocate(DiskCache.PAGE_SIZE));
    }

    /**
     * Serializes this node to the specified ByteBuffer
     */
    public ByteBuffer serialize(ByteBuffer buffer){
    serializeHeaderToBuffer(buffer);
    if(sameLengthKeys){
        sameLengthKeySerialization(buffer);
    }
    else{
        variableLengthKeySerialization(buffer);
    }

    return buffer;
}
    /**
     * Parses a node from the specified cursor, initializing this nodes variables.
     * @param buffer
     */
    protected void deserialize(ByteBuffer buffer){
        sameLengthKeys = NodeHeader.isNodeWithSameLengthKeys(buffer);
        followingNodeID = NodeHeader.getSiblingID(buffer);
        if(sameLengthKeys){
            sameLengthKeyDeserialization(buffer);
        }
        else{
            variableLengthKeyDeserialization(buffer);
        }
    }

    /**
     * Returns a string representation of this node.
     * @return
     */
    public String toString(){
        String stringRepresentation = this instanceof InternalNode ? "Internal bptree.Block" : "Leaf bptree.Block";
        stringRepresentation += "\n Node ID: ";
        stringRepresentation += this.id + "\n Keys: ";
        for (Long[] key : keys){
            stringRepresentation += key + ", ";
        }
        if(this instanceof InternalNode) {
            stringRepresentation += "\n Children: ";
            for (long childID : ((InternalNode) this).children) {
                stringRepresentation += childID + ", ";
            }
        }
        stringRepresentation += "\n";
        return stringRepresentation;
    }

    abstract protected void sameLengthKeyDeserialization(ByteBuffer buffer);

    abstract protected void variableLengthKeyDeserialization(ByteBuffer buffer);

    abstract protected void sameLengthKeySerialization(ByteBuffer buffer);

    abstract protected void variableLengthKeySerialization(ByteBuffer buffer);

    abstract protected boolean notFull(Long[] newKey);

    abstract protected int search(Long[] key);

    abstract public CursorImpl find(Long[] key) throws IOException;

    abstract public SplitResult insert(Long[] key) throws IOException;



}
