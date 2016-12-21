/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package pathIndex.tree;

import storage.PersistedPageHeader;

import java.io.IOException;

public class SearchCursor
{
    long siblingNode;
    int currentKeyIndex;
    int keyLength;
    long[] searchKey;
    public long pageID;
    int keysInNode;


    public SearchCursor( long pageID, long siblingNode, int position, long[] searchKey, int keyLength, int keysInNode )
    {
        this.siblingNode = siblingNode;
        this.searchKey = searchKey;
        this.keyLength = keyLength;
        this.currentKeyIndex = position;
        this.pageID = pageID;
        this.keysInNode = keysInNode;
    }

    public long[] next( PageProxyCursor cursor ) throws IOException
    {
        long[] next = getNext( cursor );
        if ( next != null )
        {
            currentKeyIndex++;
        }
        return next;
    }

    private long[] getNext( PageProxyCursor cursor ) throws IOException
    {
        if ( cursor.getCurrentPageId() != pageID )
        {
            cursor.goToPage( pageID );
        }
        long[] currentKey = new long[keyLength];
        if ( currentKeyIndex < keysInNode )
        {
            for ( int i = 0; i < keyLength; i++ )
            {
                int bytePosition = PersistedPageHeader.NODE_HEADER_LENGTH + (currentKeyIndex * keyLength * 8) + (i * 8);
                currentKey[i] = cursor.getLong( bytePosition );
            }
        }
        else
        {
            if ( siblingNode != -1 )
            {
                loadSiblingNode( cursor );
                return getNext( cursor );
            }
            else
            {
                return null;
            }
        }
        if ( KeyImpl.getComparator().validPrefix( searchKey, currentKey ) )
        {
            return currentKey;
        }
        return null;
    }

    public boolean hasNext( PageProxyCursor cursor ) throws IOException
    {
        return getNext( cursor ) != null;
    }


    private void loadSiblingNode( PageProxyCursor cursor ) throws IOException
    {
        cursor.goToPage( siblingNode );
        this.pageID = siblingNode;
        this.keysInNode = PersistedPageHeader.getNumberOfKeys( cursor );
        this.currentKeyIndex = 0;
        this.siblingNode = PersistedPageHeader.getSiblingID( cursor );
    }
}
