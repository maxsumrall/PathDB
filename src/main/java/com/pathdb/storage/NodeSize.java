/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.storage;

import com.pathdb.pathIndex.diskBased.IndexTree;

import java.io.IOException;


public class NodeSize
{

    public static CompressedPageFile cursor;

    public static boolean leafNodeContainsSpaceForNewKey( CompressedPageFile cursor, long[] newKey )
    {
        return leafNodeByteSize( cursor, newKey ) < DiskCache.PAGE_SIZE;
    }

    public static int leafNodeByteSize( IndexTree tree, long nodeId, long[] newKey )
    {
        int size = 0;
        try
        {
            CompressedPageFile cursor = tree.disk.getCursor( nodeId );
            size = leafNodeByteSize( cursor, newKey );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return size;
    }

    public static int leafNodeByteSize( CompressedPageFile cursor, long[] newKey )
    {
        int byteSize = 0;
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );
        byteSize += PersistedPageHeader.NODE_HEADER_LENGTH;

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        byteSize += ((numberOfKeys + 1) * keyLength * 8);

        return byteSize;
    }

    public static boolean internalNodeContainsSpaceForNewKeyAndChild( CompressedPageFile cursor, long[] newKey )
    {
        return internalNodeByteSize( cursor, newKey ) < DiskCache.PAGE_SIZE;
    }

    public static int internalNodeByteSize( IndexTree tree, long nodeId, long[] newKey )
    {
        int size = 0;
        try
        {
            CompressedPageFile cursor = tree.disk.getCursor( nodeId );
            size = internalNodeByteSize( cursor, newKey );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return size;
    }

    public static int internalNodeByteSize( CompressedPageFile cursor, long[] newKey )
    {
        int byteSize = 0;
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );
        byteSize += PersistedPageHeader.NODE_HEADER_LENGTH;
        byteSize += (numberOfKeys + 2) * 8; //calculate number of children;

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        byteSize += ((numberOfKeys + 1) * keyLength * 8);

        return byteSize;
    }
}
