/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.diskBased;

import com.pathdb.pathDB.PathPrefix;
import com.pathdb.storage.PersistedPageHeader;

import java.io.IOException;


public class IndexSearch
{
    public PageProxyCursor cursor;
    public IndexTree tree;

    public IndexSearch( IndexTree tree )
    {
        this.tree = tree;
    }

    public SearchCursor find( PathPrefix key )
    {
        long[] entry = null;
        SearchCursor resultsCursor = null;
        int[] searchResult;
        try
        {
            PageProxyCursor cursor = tree.disk.getCursor( tree.rootNodeId );
            searchResult = find( cursor, key );
            long currentNode = cursor.getCurrentPageId();
            if ( searchResult[0] == 0 )
            {
                int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys( cursor, key );
                if ( currentNode != cursor.getCurrentPageId() )
                {
                    searchResult = altResult;
                }
            }
            resultsCursor =
                    new SearchCursor( cursor.getCurrentPageId(), PersistedPageHeader.getSiblingID( cursor ), searchResult[0],
                            key, PersistedPageHeader.getKeyLength( cursor ), PersistedPageHeader.getNumberOfKeys( cursor ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public SearchCursor findWithCursor( PageProxyCursor cursor, long[] key )
    {
        SearchCursor resultsCursor = null;
        int[] searchResult;
        try
        {
            cursor.goToPage( tree.rootNodeId );
            searchResult = find( cursor, key );
            long currentNode = cursor.getCurrentPageId();
            if ( searchResult[0] == 0 )
            {
                int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys( cursor, key );
                if ( currentNode != cursor.getCurrentPageId() )
                {
                    searchResult = altResult;
                }
            }
            resultsCursor =
                    new SearchCursor( cursor.getCurrentPageId(), PersistedPageHeader.getSiblingID( cursor ), searchResult[0],
                            key, PersistedPageHeader.getKeyLength( cursor ), PersistedPageHeader.getNumberOfKeys( cursor ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public int[] find( PageProxyCursor cursor, long[] key ) throws IOException
    {
        int[] searchResult;
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            searchResult = search( cursor, key );
        }
        else
        {
            int index = search( cursor, key )[0];
            long child = tree.getChildIdAtIndex( cursor, index );
            cursor.goToPage( child );
            searchResult = find( cursor, key );
        }
        return searchResult;
    }

    public int[] search( long nodeId, long[] key )
    {
        int[] result = new int[]{-1, -1};
        try
        {
            PageProxyCursor cursor = tree.disk.getCursor( nodeId );
            result = search( cursor, key );

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return result;
    }


    static int[] search( PageProxyCursor cursor, long[] key )
    {
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            return searchLeafNodeSameLengthKeys( cursor, key );
        }
        else
        {
            return searchInternalNodeSameLengthKeys( cursor, key );
        }
    }

    private static int[] searchInternalNodeSameLengthKeys( PageProxyCursor cursor, long[] key )
    {
        int index = -1;
        int offset = -1;
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );
        if ( numberOfKeys == 0 )
        {
            return new int[]{0, PersistedPageHeader.NODE_HEADER_LENGTH};
        }
        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + ((numberOfKeys + 1) * 8) ); //header + children
        long[] currKey = new long[keyLength];
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            for ( int j = 0; j < keyLength; j++ )
            {
                currKey[j] = cursor.getLong();
            }
            if ( IndexTree.comparator.prefixCompare( key, currKey ) < 0 )
            {
                index = i;
                offset = cursor.getOffset() - (8 * keyLength);
                break;
            }
        }
        if ( index == -1 )
        { //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }


    private static int[] searchLeafNodeSameLengthKeys( PageProxyCursor cursor, long[] key )
    {
        int index = -1;
        int offset = -1;
        int numberOfKeys = cursor.getInt( PersistedPageHeader.BYTE_POSITION_KEY_COUNT );
        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        long[] currKey = new long[keyLength];
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            for ( int j = 0; j < keyLength; j++ )
            {
                currKey[j] = cursor.getLong();
            }
            if ( IndexTree.comparator.prefixCompare( key, currKey ) <= 0 )
            {
                index = i;
                offset = cursor.getOffset() - (8 * keyLength);
                break;
            }
        }
        if ( index == -1 )
        { //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }


    private static int[] moveCursorBackIfPreviousNodeContainsValidKeys( PageProxyCursor cursor, long[] key )
            throws IOException
    {
        long currentNode = cursor.getCurrentPageId();
        long previousNode = PersistedPageHeader.getPrecedingID( cursor );
        if ( previousNode != -1 )
        {
            cursor.goToPage( previousNode );
        }
        int[] result = search( cursor, key );
        if ( result[0] == PersistedPageHeader.getNumberOfKeys( cursor ) )
        {
            cursor.goToPage( currentNode );
        }
        return result;
    }
}
