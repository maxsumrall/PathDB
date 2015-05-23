package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PageCursor;

import java.nio.ByteBuffer;

public final class NodeHeader {
    /*
The Header of the block when stored as bytes is:
(1) Byte - Is this a leaf block?
(4) int - The size of the keys, only relevant if keys are the same length.
(4) int - the number of keys in this node. A slightly delicate topic in the internal nodes, since they also contain child node ids and not just keys.
(8) long - the id to the next node, the sibling node.
(8) long - the id to the previous node, the preceding node.
 */
    public static final int BYTE_POSITION_NODE_TYPE = 0;
    public static final int BYTE_POSITION_KEY_LENGTH = 1;
    public static final int BYTE_POSITION_KEY_COUNT = 5;
    public static final int BYTE_POSITION_SIBLING_ID = 9;
    public static final int BYTE_POSITION_PRECEDING_ID = 17;
    public static final int LEAF_FLAG = 1;
    public static final int SAME_LENGTH_KEYS_FLAG = 1;
    public static final int NODE_HEADER_LENGTH = 1 + 4 + 4 + 8 + 8;
    public static final long KEY_DELIMITER = -1;

    protected static boolean isLeafNode(ByteBuffer buffer){
        return buffer.get(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }
    protected static boolean isLeafNode(PageCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }
    protected static boolean isLeafNode(PageProxyCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }



    protected static boolean isNodeWithSameLengthKeys(ByteBuffer buffer){
        return getKeyLength(buffer) != -1;
    }
    protected static boolean isNodeWithSameLengthKeys(PageCursor cursor){
        return getKeyLength(cursor) != -1;
    }
    protected static boolean isNodeWithSameLengthKeys(PageProxyCursor cursor){
        return getKeyLength(cursor) != -1;
    }




    protected static int getNumberOfKeys(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_COUNT);
    }
    protected static int getNumberOfKeys(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_COUNT);
    }
    protected static int getNumberOfKeys(PageProxyCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_COUNT);
    }


    protected static void setNumberOfKeys(PageCursor cursor, int numberOfKeys){
        cursor.putInt(BYTE_POSITION_KEY_COUNT, numberOfKeys);
    }

    protected static void setNumberOfKeys(PageProxyCursor cursor, int numberOfKeys){
        cursor.putInt(BYTE_POSITION_KEY_COUNT, numberOfKeys);
    }




    protected static int getKeyLength(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_LENGTH);
    }
    protected static int getKeyLength(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_LENGTH);
    }

    protected static int getKeyLength(PageProxyCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_LENGTH);
    }



    protected static void setKeyLength(PageCursor cursor, int keyLength){
        cursor.putInt(BYTE_POSITION_KEY_LENGTH, keyLength);
    }

    protected static void setKeyLength(PageProxyCursor cursor, int keyLength){
        cursor.putInt(BYTE_POSITION_KEY_LENGTH, keyLength);
    }




    protected static long getSiblingID(ByteBuffer buffer){
        return buffer.getLong(BYTE_POSITION_SIBLING_ID);
    }

    protected static long getSiblingID(PageCursor cursor){
        return cursor.getLong(BYTE_POSITION_SIBLING_ID);
    }
    protected static long getSiblingID(PageProxyCursor cursor){
        return cursor.getLong(BYTE_POSITION_SIBLING_ID);
    }




    protected static long getPrecedingID(ByteBuffer buffer){
        return buffer.getLong(BYTE_POSITION_PRECEDING_ID);
    }

    protected static long getPrecedingID(PageCursor cursor){
        return cursor.getLong(BYTE_POSITION_PRECEDING_ID);
    }
    protected static long getPrecedingID(PageProxyCursor cursor){
        return cursor.getLong(BYTE_POSITION_PRECEDING_ID);
    }




    protected static void setFollowingID(PageCursor cursor, long followingId){
        cursor.putLong(BYTE_POSITION_SIBLING_ID, followingId);
    }

    protected static void setFollowingID(PageProxyCursor cursor, long followingId){
        cursor.putLong(BYTE_POSITION_SIBLING_ID, followingId);
    }

    protected static void setPrecedingId(PageCursor cursor, long precedingId){
        cursor.putLong(BYTE_POSITION_PRECEDING_ID, precedingId);
    }

    protected static void setPrecedingId(PageProxyCursor cursor, long precedingId){
        cursor.putLong(BYTE_POSITION_PRECEDING_ID, precedingId);
    }

    protected static void initializeLeafNode(PageCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 1);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }
    public static void initializeLeafNode(PageProxyCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 1);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }

    protected static void initializeInternalNode(PageCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 2);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }
    protected static void initializeInternalNode(PageProxyCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 2);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }

}
