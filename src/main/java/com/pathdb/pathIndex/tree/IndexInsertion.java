/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.tree;

import com.pathdb.storage.DiskCache;
import com.pathdb.storage.PersistedPageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;


public class IndexInsertion
{

    public static PageProxyCursor cursor;
    public static DiskCache disk;
    public IndexTree tree;

    public IndexInsertion( IndexTree tree )
    {
        this.tree = tree;
    }

    public SplitResult insert( long[] key )
    {
        SplitResult result = null;
        PageProxyCursor cursor = null;
        try
        {
            cursor = tree.disk.getCursor( tree.rootNodeId );
            if ( PersistedPageHeader.isLeafNode( cursor ) )
            {
                result = addKeyToLeafNode( cursor, key );
            }
            else
            {
                int index = IndexSearch.search( cursor, key )[0];
                long child = tree.getChildIdAtIndex( cursor, index );
                long id = cursor.getCurrentPageId();
                cursor.goToPage( child );
                result = insert( cursor, key );
                if ( result != null )
                {
                    cursor.goToPage( id );
                    result = addKeyAndChildToInternalNode( cursor, id, result.primkey, result.right );
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        return result;
    }

    private SplitResult insert( PageProxyCursor cursor, long[] key ) throws IOException
    {
        SplitResult result = null;
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            result = addKeyToLeafNode( cursor, key );
        }
        else
        {
            int index = IndexSearch.search( cursor, key )[0];
            long child = tree.getChildIdAtIndex( cursor, index );
            long id = cursor.getCurrentPageId();
            cursor.goToPage( child );
            result = insert( cursor, key );
            if ( result != null )
            {
                cursor.goToPage( id );
                result = addKeyAndChildToInternalNode( cursor, id, result.primkey, result.right );
            }
        }
        return result;
    }

    public SplitResult addKeyAndChildToInternalNode( long nodeId, long[] key, long child )
    {
        SplitResult result = null;
        try
        {
            PageProxyCursor cursor = tree.disk.getCursor( nodeId );
            result = addKeyAndChildToInternalNode( cursor, nodeId, key, child );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return result;
    }

    public static SplitResult addKeyAndChildToInternalNode( PageProxyCursor cursor, long nodeId, long[] key,
            long child ) throws IOException
    {
        SplitResult result = null;
        if ( !cursor.internalNodeContainsSpaceForNewKeyAndChild( key ) )
        {
            long newInternalNodeId = IndexTree.acquireNewInternalNode( cursor );
            result = new SplitResult();
            result.left = nodeId;
            result.right = newInternalNodeId;
            IndexTree.updateSiblingAndFollowingIdsInsertion( cursor, nodeId, newInternalNodeId );
            result.primkey = insertAndBalanceKeysBetweenInternalNodes( cursor, nodeId, newInternalNodeId, key, child );
            if ( !newKeyBelongsInNewNode( cursor, key ) )
            {
                cursor.goToPage( nodeId );
            }
        }
        else
        {
            int[] searchResult = IndexSearch.search( cursor, key );
            insertKeyAtIndex( cursor, searchResult[1], key );
            insertChildAtIndex( cursor, searchResult[0] + 1, child );
        }
        return result;
    }

    public SplitResult addKeyToLeafNode( long nodeId, long[] key )
    {
        SplitResult result = null;
        try
        {
            PageProxyCursor cursor = tree.disk.getCursor( nodeId );
            result = addKeyToLeafNode( cursor, key );

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return result;
    }

    private static SplitResult addKeyToLeafNode( PageProxyCursor cursor, long[] key ) throws IOException
    {
        SplitResult result = null;
        if ( !cursor.leafNodeContainsSpaceForNewKey( key ) )
        {
            result = new SplitResult();
            result.left = cursor.getCurrentPageId();
            long newLeafNodeId = IndexTree.acquireNewLeafNode( cursor );
            result.right = newLeafNodeId;
            IndexTree.updateSiblingAndFollowingIdsInsertion( cursor, result.left, newLeafNodeId );
            result.primkey = insertAndBalanceKeysBetweenLeafNodes( cursor, result.left, result.right, key );
        }
        else
        {
            int[] searchResult = IndexSearch.search( cursor, key );
            insertKeyAtIndex( cursor, searchResult[1], key );
        }
        return result;
    }

    private static long[] insertAndBalanceKeysBetweenLeafNodes( PageProxyCursor cursor, long fullNode, long emptyNode,
            long[] newKey ) throws IOException
    {
        //grab half of the keys from the first node, dump into the new node.
        cursor.goToPage( fullNode );
        long[] returnedKey;
        byte[] keysA;
        byte[] keysB;

        int[] searchResults = IndexSearch.search( cursor, newKey );
        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        int originalNumberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );
        int keysInclInsert = originalNumberOfKeys + 1;

        returnedKey = new long[keyLength];
        byte[] keys = new byte[originalNumberOfKeys * keyLength * Long.BYTES];
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.getBytes( keys );
        keys = insertKeyAtIndex( keys, newKey, searchResults[0],
                returnedKey );//TODO there is something wrong here in this function and splitting
        keysA = new byte[((keysInclInsert / 2) * keyLength) * Long.BYTES];
        keysB = new byte[(((keysInclInsert + 1) / 2) * keyLength) * Long.BYTES];
        System.arraycopy( keys, 0, keysA, 0, keysA.length );
        System.arraycopy( keys, keysA.length, keysB, 0, keysB.length );


        PersistedPageHeader.setNumberOfKeys( cursor, keysInclInsert / 2 );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.putBytes( keysA );

        cursor.goToPage( emptyNode );
        PersistedPageHeader.setNumberOfKeys( cursor, (keysInclInsert + 1) / 2 );
        PersistedPageHeader.setKeyLength( cursor, keyLength );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.putBytes( keysB );

        return returnedKey;
    }

    private static long[] insertAndBalanceKeysBetweenInternalNodes( PageProxyCursor cursor, long fullNode,
            long emptyNode, long[] newKey, long newChild ) throws IOException
    {
        //grab half of the keys from the first node, dump into the new node.
        cursor.goToPage( fullNode );
        long[] returnedKey;
        byte[] childrenA;
        byte[] childrenB;
        byte[] keysA;
        byte[] keysB;

        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );

        int[] searchResults = IndexSearch.search( cursor, newKey );
        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        int originalNumberOfKeys = PersistedPageHeader.getNumberOfKeys( cursor );
        int keysInclInsert = originalNumberOfKeys + 1;

        returnedKey = new long[keyLength];
        byte[] keys = new byte[originalNumberOfKeys * keyLength * Long.BYTES];
        byte[] children = new byte[(originalNumberOfKeys + 1) * Long.BYTES];
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.getBytes( children );
        cursor.getBytes( keys );
        keys = insertKeyAtIndex( keys, newKey, searchResults[0], returnedKey );
        children = insertChildAtIndex( children, newChild,
                searchResults[0] + 1 );//TODO there is something wrong here in this function and splitting

        childrenA = new byte[((int) Math.ceil( (keysInclInsert + 1) / 2.0 )) * Long.BYTES];
        childrenB = new byte[((int) Math.floor( (keysInclInsert + 1) / 2.0 )) * Long.BYTES];

        keysA = new byte[((int) Math.floor( (keysInclInsert + 1) / 2.0 )) * keyLength * Long.BYTES];
        int middleAfterDroppedKey = keysA.length + keyLength * Long.BYTES;
        keysB = new byte[keysInclInsert % 2 == 0 ? keysA.length - (keyLength * Long.BYTES) : keys.length];

        System.arraycopy( keys, 0, keysA, 0, keysA.length );
        System.arraycopy( keys, middleAfterDroppedKey, keysB, 0, keysB.length );
        System.arraycopy( children, 0, childrenA, 0, childrenA.length );
        System.arraycopy( children, childrenA.length, childrenB, 0, childrenB.length );

        cursor.deferWriting();
        PersistedPageHeader.setNumberOfKeys( cursor, keysInclInsert / 2 );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.putBytes( childrenA );
        cursor.putBytes( keysA );
        cursor.resumeWriting();

        cursor.goToPage( emptyNode );
        cursor.deferWriting();
        PersistedPageHeader.setNumberOfKeys( cursor, originalNumberOfKeys / 2 );
        PersistedPageHeader.setKeyLength( cursor, keyLength );
        cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        cursor.putBytes( childrenB );
        cursor.putBytes( keysB );
        cursor.resumeWriting();

        return returnedKey;
    }

    private static boolean newKeyBelongsInNewNode( PageProxyCursor cursor, long[] newKey )
    {
        return IndexTree.comparator.prefixCompare( newKey, getFirstKeyInNode( cursor ) ) > 0;

    }

    private static long[] getFirstKeyInNode( PageProxyCursor cursor )
    {
        long[] firstKey;
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        }
        else
        {
            int children = PersistedPageHeader.getNumberOfKeys( cursor ) + 1;
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + children * Long.BYTES );
        }

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        firstKey = new long[keyLength];
        for ( int i = 0; i < keyLength; i++ )
        {
            firstKey[i] = cursor.getLong();
        }


        return firstKey;
    }

    public static byte[] getFirstKeyInNodeAsBytes( PageProxyCursor cursor )
    {
        byte[] firstKey;
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        }
        else
        {
            int children = PersistedPageHeader.getNumberOfKeys( cursor ) + 1;
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + children * Long.BYTES );
        }

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        firstKey = new byte[keyLength * Long.BYTES];
        cursor.getBytes( firstKey );

        return firstKey;
    }

    public static byte[] popFirstKeyInNodeAsBytes( PageProxyCursor cursor )
    {
        byte[] firstKey;
        if ( PersistedPageHeader.isLeafNode( cursor ) )
        {
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH );
        }
        else
        {
            int children = PersistedPageHeader.getNumberOfKeys( cursor ) + 1;
            cursor.setOffset( PersistedPageHeader.NODE_HEADER_LENGTH + children * Long.BYTES );
        }

        int keyLength = PersistedPageHeader.getKeyLength( cursor );
        firstKey = new byte[keyLength * Long.BYTES];
        cursor.getBytes( firstKey );


        return firstKey;
    }

    private static void insertKeyAtIndex( PageProxyCursor cursor, int offset, long[] key )
    {
        byte[] tmp_bytes;
        PersistedPageHeader.setNumberOfKeys( cursor, PersistedPageHeader.getNumberOfKeys( cursor ) + 1 );

        tmp_bytes = new byte[cursor.capacity() - offset - (key.length * Long.BYTES)];


        cursor.setOffset( offset );
        cursor.getBytes( tmp_bytes );
        cursor.setOffset( offset );
        cursor.deferWriting();
        for ( long item : key )
        {
            cursor.putLong( item );
        }

        cursor.putBytes( tmp_bytes );

        cursor.resumeWriting();
    }

    private static void insertChildAtIndex( PageProxyCursor cursor, int index, long child )
    {
        cursor.deferWriting();
        int childInsertionOffset = PersistedPageHeader.NODE_HEADER_LENGTH + (index * Long.BYTES);
        byte[] shiftDownBytes = new byte[DiskCache.PAGE_SIZE - childInsertionOffset - Long.BYTES];
        cursor.setOffset( childInsertionOffset );
        cursor.getBytes( shiftDownBytes );
        cursor.setOffset( childInsertionOffset );
        cursor.putLong( child );
        cursor.putBytes( shiftDownBytes );
        cursor.resumeWriting();
    }

    private static byte[] insertKeyAtIndex( byte[] keys, long[] newKey, int index, long[] returnedKey )
    {
        LongBuffer keyB = ByteBuffer.wrap( keys ).asLongBuffer();
        byte[] updatedKeys = new byte[keys.length + (newKey.length * Long.BYTES)];
        ByteBuffer updatedKeysBB = ByteBuffer.wrap( updatedKeys );
        LongBuffer updatedKeysLB = updatedKeysBB.asLongBuffer();
        for ( int i = 0; i < index; i++ )
        {
            for ( int j = 0; j < newKey.length; j++ )
            {
                updatedKeysLB.put( keyB.get() );
            }
        }
        updatedKeysLB.put( newKey );
        int remaining = keyB.remaining();
        for ( int i = 0; i < remaining; i++ )
        {
            updatedKeysLB.put( keyB.get() );
        }

        int middle = updatedKeysLB.capacity() / 2;
        for ( int i = 0; i < returnedKey.length; i++ )
        {
            returnedKey[i] = updatedKeysLB.get( middle + i );
        }

        return updatedKeysBB.array();
    }

    private static byte[] insertChildAtIndex( byte[] children, long newChild, int index )
    {
        LongBuffer childrenB = ByteBuffer.wrap( children ).asLongBuffer();
        byte[] updatedChildren = new byte[children.length + Long.BYTES];
        ByteBuffer updatedChildrenBB = ByteBuffer.wrap( updatedChildren );
        LongBuffer updatedChildrenLB = updatedChildrenBB.asLongBuffer();
        for ( int i = 0; i < index; i++ )
        {
            updatedChildrenLB.put( childrenB.get() );
        }
        updatedChildrenLB.put( newChild );
        int remaining = childrenB.remaining();
        for ( int i = 0; i < remaining; i++ )
        {
            updatedChildrenLB.put( childrenB.get() );
        }
        return updatedChildrenBB.array();
    }

}
