/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package storage;

public class PersistedPageHeader implements PageHeader
{
    private CompressedPageFile cursor;

    public boolean isLeafNode()
    {
        return cursor.getByte( BYTE_POSITION_NODE_TYPE ) == LEAF_FLAG;
    }

    public boolean isUninitializedNode()
    {
        return cursor.getByte( BYTE_POSITION_NODE_TYPE ) == 0;
    }

    public int getNumberOfKeys()
    {
        return cursor.getInt( BYTE_POSITION_KEY_COUNT );
    }

    public void setNumberOfKeys( int numberOfKeys )
    {
        cursor.putInt( BYTE_POSITION_KEY_COUNT, numberOfKeys );
    }

    public int getKeyLength()
    {
        return cursor.getInt( BYTE_POSITION_KEY_LENGTH );
    }

    public void setKeyLength( int keyLength )
    {
        cursor.putInt( BYTE_POSITION_KEY_LENGTH, keyLength );
    }

    public long getSiblingID()
    {
        return cursor.getLong( BYTE_POSITION_SIBLING_ID );
    }

    public long getPrecedingID()
    {
        return cursor.getLong( BYTE_POSITION_PRECEDING_ID );
    }

    public void setNodeTypeLeaf()
    {
        cursor.putByte( BYTE_POSITION_NODE_TYPE, (byte) 1 );
    }

    public void setFollowingID( long followingId )
    {
        cursor.putLong( BYTE_POSITION_SIBLING_ID, followingId );
    }

    public void setPrecedingId( long precedingId )
    {
        cursor.putLong( BYTE_POSITION_PRECEDING_ID, precedingId );
    }

    public void initializeLeafNode()
    {
        cursor.putByte( PersistedPageHeader.BYTE_POSITION_NODE_TYPE, (byte) 1 );
        cursor.putInt( PersistedPageHeader.BYTE_POSITION_KEY_LENGTH, 0 );
        cursor.putInt( PersistedPageHeader.BYTE_POSITION_KEY_COUNT, 0 );
        cursor.putLong( PersistedPageHeader.BYTE_POSITION_SIBLING_ID, -1 );
        cursor.putLong( PersistedPageHeader.BYTE_POSITION_PRECEDING_ID, -1 );
    }

    public void initializeInternalNode()
    {
        cursor.putByte( PersistedPageHeader.BYTE_POSITION_NODE_TYPE, (byte) 2 );
        cursor.putInt( PersistedPageHeader.BYTE_POSITION_KEY_LENGTH, 0 );
        cursor.putInt( PersistedPageHeader.BYTE_POSITION_KEY_COUNT, 0 );
        cursor.putLong( PersistedPageHeader.BYTE_POSITION_SIBLING_ID, -1 );
        cursor.putLong( PersistedPageHeader.BYTE_POSITION_PRECEDING_ID, -1 );
    }
}
