/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import com.pathdb.storage.DiskCache;
import com.pathdb.storage.PersistedPageHeader;

import java.io.IOException;


public class SimpleDataGenerator
{

    public int numberOfPages;
    public int keyLength = 4;
    public int keysPerPage = (((DiskCache.PAGE_SIZE - PersistedPageHeader.NODE_HEADER_LENGTH) / Long.BYTES) / keyLength);
    public DiskCache disk = DiskCache.temporaryDiskCache( false );

    public SimpleDataGenerator( int numberOfPages ) throws IOException
    {
        this.numberOfPages = numberOfPages;
        for ( int i = 0; i < numberOfPages; i++ )
        {
            PageProxyCursor cursor = disk.getCursor( i );
            PersistedPageHeader.setNodeTypeLeaf( cursor );
            PersistedPageHeader.setFollowingID( cursor, cursor.getCurrentPageId() + 1 );
            PersistedPageHeader.setPrecedingId( cursor, cursor.getCurrentPageId() - 1 );
            PersistedPageHeader.setKeyLength( cursor, keyLength );
            PersistedPageHeader.setNumberOfKeys( cursor, keysPerPage );
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
            for ( int j = 0; j < keysPerPage; j++ )
            {
                for ( int k = 0; k < keyLength; k++ )
                {
                    cursor.putLong( (i * keysPerPage) + j + 1 );
                }

            }
        }
    }
}
