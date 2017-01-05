/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.tree;

import com.pathdb.pathDB.PathPrefix;
import com.pathdb.storage.DiskCache;
import com.pathdb.storage.PersistedPageHeader;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

public class IndexTree
{

    public PagedFile pagedFile;
    public DiskCache disk;
    public static KeyImpl comparator = new KeyImpl();
    public long rootNodeId = 0;
    public int keySize;
    public PageProxyCursor cursor;
    public IndexSearch nodeSearch;
    public IndexInsertion nodeInsertion;
    public IndexDeletion nodeDeletion;

    public IndexTree( int keySize, long rootNodeId, DiskCache disk )
    {
        this.rootNodeId = rootNodeId;
        pagedFile = disk.pagedFile;
        this.disk = disk;
        this.keySize = keySize;
        this.nodeSearch = new IndexSearch( this );
        this.nodeInsertion = new IndexInsertion( this );
        this.nodeDeletion = new IndexDeletion( this );
    }

    public IndexTree( int keySize, DiskCache disk ) throws IOException
    {
        pagedFile = disk.pagedFile;
        this.disk = disk;
        this.keySize = keySize;
        rootNodeId = acquireNewLeafNode();
        this.nodeSearch = new IndexSearch( this );
        this.nodeInsertion = new IndexInsertion( this );
        this.nodeDeletion = new IndexDeletion( this );
    }


    public void newRoot( long childA, long childB, long[] key )
    {
        try
        {
            PageProxyCursor cursor = disk.getCursor( rootNodeId );
            rootNodeId = acquireNewInternalNode( cursor );
            cursor.goToPage( rootNodeId );
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
            cursor.putLong( childA );
            cursor.putLong( childB );
            for ( int i = 0; i < key.length; i++ )
            {
                cursor.putLong( key[i] );
            }
            PersistedPageHeader.setKeyLength( cursor, key.length );
            PersistedPageHeader.setNumberOfKeys( cursor, 1 );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }


    public SearchCursor find( PathPrefix path) throws IOException
    {
        return nodeSearch.find( path );
    }

    public SearchCursor find( PageProxyCursor cursor, long[] key ) throws IOException
    {
        return nodeSearch.findWithCursor( cursor, key );
    }

    public void insert( long[] key )
    {
        SplitResult result = nodeInsertion.insert( key );

        if ( result != null )
        {
            newRoot( result.left, result.right, result.primkey );
        }
    }

    public void remove( long[] key )
    {
        nodeDeletion.remove( key );
    }


    public void setPrecedingId( long nodeId, long newPrecedingId )
    {
        try
        {
            PageProxyCursor cursor = disk.getCursor( rootNodeId );
            PersistedPageHeader.setPrecedingId( cursor, newPrecedingId );

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    public void setFollowingId( long nodeId, long newFollowingId )
    {
        try
        {
            PageProxyCursor cursor = disk.getCursor( rootNodeId );
            PersistedPageHeader.setFollowingID( cursor, newFollowingId );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    public long getChildIdAtIndex( PageProxyCursor cursor, int indexOfChild )
    {
        long childId = 0;
        childId = cursor.getLong( PersistedPageHeader.NODE_HEADER_LENGTH + indexOfChild * 8 );
        return childId;
    }

    public long getChildIdAtIndex( long nodeId, int indexOfChild )
    {
        long childId = 0;
        try
        {
            PageProxyCursor cursor = disk.getCursor( rootNodeId );
            childId = getChildIdAtIndex( cursor, indexOfChild );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return childId;
    }


    public int getIndexOfChild( long nodeId, long childId )
    {
        int childIndex = -1;
        try
        {
            PageProxyCursor cursor = disk.getCursor( rootNodeId );
            childIndex = getIndexOfChild( cursor, childId );

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return childIndex;
    }

    public static int getIndexOfChild( PageProxyCursor cursor, long childId )
    {
        int childIndex = 0;
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        long child = cursor.getLong();
        while ( child != childId )
        {
            child = cursor.getLong();
            childIndex++;
        }
        return childIndex;
    }


    public static void updateSiblingAndFollowingIdsInsertion( PageProxyCursor cursor, long nodeId, long newNodeId )
            throws IOException
    {
        cursor.goToPage( nodeId );
        long oldFollowing = PersistedPageHeader.getSiblingID( cursor );
        PersistedPageHeader.setFollowingID( cursor, newNodeId );
        if ( oldFollowing != -1l )
        {
            cursor.goToPage( oldFollowing );
            PersistedPageHeader.setPrecedingId( cursor, newNodeId );
        }
        cursor.goToPage( newNodeId );
        PersistedPageHeader.setFollowingID( cursor, oldFollowing );
        PersistedPageHeader.setPrecedingId( cursor, nodeId );
    }

    public static void updateSiblingAndFollowingIdsDeletion( PageProxyCursor cursor, long nodeId ) throws IOException
    {
        cursor.goToPage( nodeId );
        long following = PersistedPageHeader.getSiblingID( cursor );
        long preceding = PersistedPageHeader.getPrecedingID( cursor );
        if ( following != -1l )
        {
            cursor.goToPage( following );
            PersistedPageHeader.setPrecedingId( cursor, preceding );
        }
        if ( preceding != -1l )
        {
            cursor.goToPage( preceding );
            PersistedPageHeader.setFollowingID( cursor, following );
        }
    }

    public static long acquireNewLeafNode( PageProxyCursor cursor ) throws IOException
    {
        long newNodeId = TreeNodeIDManager.acquire();
        cursor.goToPage( newNodeId );
        PersistedPageHeader.initializeLeafNode( cursor );
        return newNodeId;
    }

    public long acquireNewLeafNode() throws IOException
    {
        long newNodeId = TreeNodeIDManager.acquire();
        try
        {
            PageProxyCursor cursor = disk.getCursor( newNodeId );
            PersistedPageHeader.initializeLeafNode( cursor, this.keySize );
        }
        catch ( Exception e )
        {

        }
        return newNodeId;
    }

    public static long acquireNewInternalNode( PageProxyCursor cursor ) throws IOException
    {
        long newNodeId = TreeNodeIDManager.acquire();
        cursor.goToPage( newNodeId );
        PersistedPageHeader.initializeInternalNode( cursor );
        return newNodeId;
    }

    public static void releaseNode( long nodeId )
    {
        TreeNodeIDManager.release( nodeId );
    }

    public static void removeFirstKeyInInternalNode( PageProxyCursor cursor )
    {
        byte[] compactionBytes = new byte[DiskCache.PAGE_SIZE - PersistedPageHeader.NODE_HEADER_LENGTH - 8]; //removing child
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + 8 );
        cursor.getBytes( compactionBytes );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.putBytes( compactionBytes );

        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        compactionBytes =
                new byte[DiskCache.PAGE_SIZE - PersistedPageHeader.NODE_HEADER_LENGTH - (numberOfKeys * 8) - (8 * keyLength)];
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + (numberOfKeys * 8) + (8 * keyLength) );
        cursor.getBytes( compactionBytes );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + (numberOfKeys * 8) );
        cursor.putBytes( compactionBytes );

        PersistedPageHeader.setNumberOfKeys( cursor, numberOfKeys - 1 );
    }
}
