package bptree.impl;

import java.nio.ByteBuffer;

public final class NodeHeader {
    /*
The Header of the block when stored as bytes is:
(1) Byte - Is this a leaf block?
(4) int - The size of the keys, only relevant if keys are the same length.
(4) int - the number of keys in this node. A slightly delicate topic in the internal nodes, since they also contain child node ids and not just keys.
(8) long - the id to the next node, the sibling node.
 */
    public static final int BYTE_POSITION_NODE_TYPE = 0;
    public static final int BYTE_POSITION_KEY_LENGTH = 1;
    public static final int BYTE_POSITION_KEY_COUNT = 5;
    public static final int BYTE_POSITION_SIBLING_ID = 9;
    public static final int LEAF_FLAG = 1;
    public static final int NODE_HEADER_LENGTH = 1 + 4 + 4 + 8;

    protected static boolean isLeafNode(ByteBuffer buffer){
        return buffer.get(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }

    protected static boolean isNodeWithSameLengthKeys(ByteBuffer buffer){
        return getKeyLength(buffer) != -1;
    }

    protected static int getNumberOfKeys(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_COUNT);
    }

    protected static int getKeyLength(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_LENGTH);
    }

    protected static long getSiblingID(ByteBuffer buffer){
        return buffer.getLong(BYTE_POSITION_SIBLING_ID);
    }

}
