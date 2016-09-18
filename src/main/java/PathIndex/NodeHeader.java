/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package PathIndex;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

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
    public static final int NODE_HEADER_LENGTH = 1 + 4 + 4 + 8 + 8;

    public static boolean isLeafNode(ByteBuffer buffer){
        return buffer.get(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }
    public static boolean isLeafNode(PageCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }
    public static boolean isLeafNode(PageProxyCursor cursor){
        return cursor.getByte(BYTE_POSITION_NODE_TYPE) == LEAF_FLAG;
    }
    public static boolean isUninitializedNode(PageCursor cursor){return cursor.getByte(BYTE_POSITION_NODE_TYPE) == 0;}


    public static int getNumberOfKeys(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_COUNT);
    }
    public static int getNumberOfKeys(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_COUNT);
    }
    public static int getNumberOfKeys(PageProxyCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_COUNT);
    }


    public static void setNumberOfKeys(PageCursor cursor, int numberOfKeys){
        cursor.putInt(BYTE_POSITION_KEY_COUNT, numberOfKeys);
    }

    public static void setNumberOfKeys(ByteBuffer buffer, int numberOfKeys){
        buffer.putInt(BYTE_POSITION_KEY_COUNT, numberOfKeys);
    }

    public static void setNumberOfKeys(PageProxyCursor cursor, int numberOfKeys){
        cursor.putInt(BYTE_POSITION_KEY_COUNT, numberOfKeys);
    }

    public static int getKeyLength(ByteBuffer buffer){
        return buffer.getInt(BYTE_POSITION_KEY_LENGTH);
    }
    public static int getKeyLength(PageCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_LENGTH);
    }

    public static int getKeyLength(PageProxyCursor cursor){
        return cursor.getInt(BYTE_POSITION_KEY_LENGTH);
    }



    public static void setKeyLength(PageCursor cursor, int keyLength){
        cursor.putInt(BYTE_POSITION_KEY_LENGTH, keyLength);
    }

    public static void setKeyLength(ByteBuffer buffer, int keyLength){
        buffer.putInt(BYTE_POSITION_KEY_LENGTH, keyLength);
    }

    public static void setKeyLength(PageProxyCursor cursor, int keyLength){
        cursor.putInt(BYTE_POSITION_KEY_LENGTH, keyLength);
    }

    public static void setNodeTypeLeaf(PageProxyCursor cursor){
        cursor.putByte(BYTE_POSITION_NODE_TYPE, (byte) 1);
    }
    public static void setNodeTypeLeaf(ByteBuffer buffer){
        buffer.put(BYTE_POSITION_NODE_TYPE, (byte) 1);
    }




    public static long getSiblingID(ByteBuffer buffer){
        return buffer.getLong(BYTE_POSITION_SIBLING_ID);
    }

    public static long getSiblingID(PageCursor cursor){
        return cursor.getLong(BYTE_POSITION_SIBLING_ID);
    }
    public static long getSiblingID(PageProxyCursor cursor){
        return cursor.getLong(BYTE_POSITION_SIBLING_ID);
    }




    public static long getPrecedingID(ByteBuffer buffer){
        return buffer.getLong(BYTE_POSITION_PRECEDING_ID);
    }

    public static long getPrecedingID(PageCursor cursor){
        return cursor.getLong(BYTE_POSITION_PRECEDING_ID);
    }
    public static long getPrecedingID(PageProxyCursor cursor){
        return cursor.getLong(BYTE_POSITION_PRECEDING_ID);
    }




    public static void setFollowingID(PageCursor cursor, long followingId){
        cursor.putLong(BYTE_POSITION_SIBLING_ID, followingId);
    }

    public static void setFollowingID(PageProxyCursor cursor, long followingId){
        cursor.putLong(BYTE_POSITION_SIBLING_ID, followingId);
    }
    public static void setFollowingID(ByteBuffer buffer, long followingId){
        buffer.putLong(BYTE_POSITION_SIBLING_ID, followingId);
    }

    public static void setPrecedingId(PageCursor cursor, long precedingId){
        cursor.putLong(BYTE_POSITION_PRECEDING_ID, precedingId);
    }

    public static void setPrecedingId(PageProxyCursor cursor, long precedingId){
        cursor.putLong(BYTE_POSITION_PRECEDING_ID, precedingId);
    }

    public static void setPrecedingId(ByteBuffer buffer, long precedingId){
        buffer.putLong(BYTE_POSITION_PRECEDING_ID, precedingId);
    }

    public static void initializeLeafNode(PageCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 1);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }
    public static void initializeLeafNode(PageProxyCursor cursor){
        cursor.deferWriting();
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 1);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
        cursor.resumeWriting();
    }
    public static void initializeLeafNode(PageProxyCursor cursor, int keySize){
        cursor.deferWriting();
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 1);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, keySize);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
        cursor.resumeWriting();
    }

    public static void initializeInternalNode(PageCursor cursor){
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 2);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
    }
    public static void initializeInternalNode(PageProxyCursor cursor){
        cursor.deferWriting();
        cursor.putByte(NodeHeader.BYTE_POSITION_NODE_TYPE, (byte) 2);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_LENGTH, 0);
        cursor.putInt(NodeHeader.BYTE_POSITION_KEY_COUNT, 0);
        cursor.putLong(NodeHeader.BYTE_POSITION_SIBLING_ID, -1);
        cursor.putLong(NodeHeader.BYTE_POSITION_PRECEDING_ID, -1);
        cursor.resumeWriting();
    }

}
