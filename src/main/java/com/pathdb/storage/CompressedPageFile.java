/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.pagecache.PageCursor;

import static com.pathdb.storage.DiskCache.PAGE_SIZE;

public class CompressedPageFile implements PageFile<P>
{
    private final int maxPageSize = PAGE_SIZE * 15;
    private final ByteBuffer uncompressedBytes = ByteBuffer.allocate( maxPageSize );
    private final int sameID = 128;
    private final int sameFirstNode = 64;
    private final PersistedPageFile persistedPageFile;
    private final ByteBuffer compressedBytes = ByteBuffer.allocate( PAGE_SIZE );
    private long currentPageId;

    private boolean deferWriting = false;
    private int mostRecentCompressedLeafSize = PAGE_SIZE;//the default value


    public CompressedPageFile( File file ) throws IOException
    {
        persistedPageFile = new PersistedPageFile( file );
    }

    private void decompress( ByteBuffer compressedBytes )
    {
        if ( PersistedPageHeader.isLeafNode( compressedBytes ) )
        {
            decompressLeaf( compressedBytes );
        }
        else if ( !PersistedPageHeader.isUninitializedNode( compressedBytes ) )
        {
            decompressInternalNode( compressedBytes );
        }
    }

    public void goToPage( long page ) throws IOException
    {
        compressedBytes.put( persistedPageFile.getBytes( page ) );
        decompress( compressedBytes );
        currentPageId = page;
    }

    public void flush()
    {
        if ( !deferWriting )
        {
            int mark = uncompressedBytes.position();
            if ( PersistedPageHeader.isLeafNode( uncompressedBytes ) )
            {
                compressAndWriteLeaf();
            }
            else
            {
                writeInternal();
            }
            uncompressedBytes.position( mark );
        }
    }

    private void writeInternal()
    {
        persistedPageFile.writeBytes( 0, uncompressedBytes.array() );
    }


    private void compressAndWriteLeaf()
    {
        //compress the contents of uncompressedBytes. Must first determine how big uncompressedBytes actually is.
        //assumption that this is a leaf node.
        //will just check if the path id is zero, if so, this is the end of this block.
        int decompressedSize = getLastUsedLeafBufferPosition() - PersistedPageHeader.NODE_HEADER_LENGTH;
        writeHeaderToCursor();
        if ( decompressedSize != 0 )
        {
            compress();
            persistedPageFile.writeBytes( PersistedPageHeader.NODE_HEADER_LENGTH, compressedBytes.array() );
            mostRecentCompressedLeafSize = compressedBytes.position() + PersistedPageHeader.NODE_HEADER_LENGTH;
        }
    }

    public void compress()
    {
        int keyLength = PersistedPageHeader.getKeyLength( uncompressedBytes );
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( uncompressedBytes );
        long[] next = new long[keyLength];
        long[] prev = new long[keyLength];
        uncompressedBytes.position( PersistedPageHeader.NODE_HEADER_LENGTH );
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            for ( int j = 0; j < keyLength; j++ )
            {
                next[j] = uncompressedBytes.getLong();
            }
            compressedBytes.put( encodeKey( next, prev ) );
            prev = next;
        }
    }

    public byte[] encodeKey( long[] key, long[] prev )
    {
        int maxNumBytes = 0;
        int firstEncodedIndex = 0;
        byte header = (byte) 0;
        if ( key[0] == prev[0] )
        {
            //set first bit
            firstEncodedIndex++;
            header |= sameID;

            if ( key[1] == prev[1] )
            {
                //set second bit
                firstEncodedIndex++;
                header |= sameFirstNode;
            }
        }

        for ( int i = firstEncodedIndex; i < key.length; i++ )
        {
            maxNumBytes = Math.max( maxNumBytes, numberOfBytes( key[i] - prev[i] ) );
        }

        byte[] encoded = new byte[1 + (maxNumBytes * (key.length - firstEncodedIndex))];
        header |= maxNumBytes;
        encoded[0] = header;
        for ( int i = 0; i < key.length - firstEncodedIndex; i++ )
        {
            toBytes( key[i + firstEncodedIndex] - prev[i + firstEncodedIndex], encoded, 1 + (i * maxNumBytes),
                    maxNumBytes );
        }
        return encoded;
    }

    public static int numberOfBytes( long value )
    {
        long abs = Math.abs( value );
        int minBytes = 8;
        if ( abs <= 127 )
        {
            minBytes = 1;
        }
        else if ( abs <= 32768 )
        {
            minBytes = 2;
        }
        else if ( abs <= 8388608 )
        {
            minBytes = 3;
        }
        else if ( abs <= 2147483648l )
        {
            minBytes = 4;
        }
        else if ( abs <= 549755813888l )
        {
            minBytes = 5;
        }
        else if ( abs <= 140737488355328l )
        {
            minBytes = 6;
        }
        else if ( abs <= 36028797018963968l )
        {
            minBytes = 7;
        }
        return minBytes;
    }

    public static void toBytes( long val, byte[] dest, int position, int numberOfBytes )
    { //rewrite this to put bytes in a already made array at the right position.
        for ( int i = numberOfBytes - 1; i > 0; i-- )
        {
            dest[position + i] = (byte) val;
            val >>= 8;
        }
        dest[position] = (byte) val;
    }


    public static long toLong( PageCursor cursor, int offset, int length )
    {
        long l = cursor.getByte( offset ) < (byte) 0 ? -1 : 0;
        for ( int i = offset; i < offset + length; i++ )
        {
            l <<= 8;
            l ^= cursor.getByte( i ) & 0xFF;
        }
        return l;
    }

    public static long toLong( ByteBuffer buffer, int offset, int length )
    {
        long l = buffer.get( offset ) < (byte) 0 ? -1 : 0;
        for ( int i = offset; i < offset + length; i++ )
        {
            l <<= 8;
            l ^= buffer.get( i ) & 0xFF;
        }
        return l;
    }


    private int getLastUsedLeafBufferPosition()
    {
        int keyLength = PersistedPageHeader.getKeyLength( uncompressedBytes );
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( uncompressedBytes );
        if ( keyLength == 0 || numberOfKeys == 0 )
        {
            return PersistedPageHeader.NODE_HEADER_LENGTH;
        }
        uncompressedBytes.position( PersistedPageHeader.NODE_HEADER_LENGTH );
        while ( uncompressedBytes.remaining() > keyLength * Long.BYTES )
        {
            if ( uncompressedBytes.getLong() == 0l )
            {
                uncompressedBytes.position( uncompressedBytes.position() - Long.BYTES );
                break;
            }
            uncompressedBytes.position( uncompressedBytes.position() + (keyLength - 1) * Long.BYTES );
        }
        return uncompressedBytes.position();
    }

    private void writeHeaderToCursor()
    {
        compressedBytes.position( 0 );
        for ( int i = 0; i < PersistedPageHeader.NODE_HEADER_LENGTH; i++ )
        {
            compressedBytes.put( uncompressedBytes.get( i ) );
        }
    }

    private void decompressLeaf( ByteBuffer compressedBytes )
    {
        int keyLength = PersistedPageHeader.getKeyLength( compressedBytes );
        int numberOfKeys = PersistedPageHeader.getNumberOfKeys( compressedBytes );
        uncompressedBytes.limit( maxPageSize );
        uncompressedBytes.position( 0 );
        compressedBytes.position( 0 );
        for ( int i = 0; i < PersistedPageHeader.NODE_HEADER_LENGTH; i++ )
        {
            uncompressedBytes.put( compressedBytes.get() );
        }

        int position = PersistedPageHeader.NODE_HEADER_LENGTH;
        int reqBytes;
        long val;
        byte header;
        int firstEncodedIndex;
        boolean samePath;
        boolean sameFirstID;
        long[] prev = new long[keyLength];
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            header = compressedBytes.get( position++ );

            //
            firstEncodedIndex = 0;
            samePath = (sameID & header) == sameID;
            sameFirstID = (sameFirstNode & header) == sameFirstNode;
            if ( samePath )
            {
                firstEncodedIndex++;
                val = prev[0];
                uncompressedBytes.putLong( val );
            }
            if ( sameFirstID )
            {
                firstEncodedIndex++;
                val = prev[1];
                uncompressedBytes.putLong( val );
            }
            header &= ~(1 << 7);
            header &= ~(1 << 6);
            reqBytes = header;
            //

            for ( int j = firstEncodedIndex; j < (keyLength); j++ )
            {
                val = prev[j] + toLong( compressedBytes, position, reqBytes );
                uncompressedBytes.putLong( val );
                prev[j] = val;
                position += reqBytes;
            }
        }
        mostRecentCompressedLeafSize = position;
    }

    private void decompressInternalNode( ByteBuffer compressedBytes )
    {
        Arrays.fill( uncompressedBytes.array(), (byte) 0 );
        uncompressedBytes.position( 0 );
        compressedBytes.position( 0 );
        uncompressedBytes.limit( PAGE_SIZE );
        for ( int i = 0; i < PAGE_SIZE; i++ )
        {
            uncompressedBytes.put( compressedBytes.get() );
        }
    }

    public long getCurrentPageId()
    {
        return currentPageId;
    }

    public void setOffset( int offset )
    {
        uncompressedBytes.position( offset );
    }

    public int getOffset()
    {
        return uncompressedBytes.position();
    }

    public void getBytes( byte[] dest )
    {
        uncompressedBytes.get( dest );
    }

    public byte getByte( int offset )
    {
        return uncompressedBytes.get( offset );
    }

    public void putBytes( byte[] src )
    {
        uncompressedBytes.put( src );
        flush();
    }

    public void putByte( int offset, byte val )
    {
        uncompressedBytes.put( offset, val );
        flush();
    }

    public long getLong()
    {
        return uncompressedBytes.getLong();
    }

    public long getLong( int offset )
    {
        return uncompressedBytes.getLong( offset );
    }

    public void putLong( long val )
    {
        uncompressedBytes.putLong( val );
        flush();
    }

    public void putLong( int offset, long val )
    {
        uncompressedBytes.putLong( offset, val );
        flush();
    }

    public int getInt()
    {
        return uncompressedBytes.getInt();
    }

    public int getInt( int offset )
    {
        return uncompressedBytes.getInt( offset );
    }

    public void putInt( int offset, int val )
    {
        uncompressedBytes.putInt( offset, val );
        flush();
    }

    public boolean leafNodeContainsSpaceForNewKey( long[] newKey )
    {
        //return NodeSize.leafNodeContainsSpaceForNewKey(this, newKey);
        int magic = 10;
        return mostRecentCompressedLeafSize + (newKey.length * Long.BYTES) + magic < PAGE_SIZE;
    }

    public void deferWriting()
    {
        deferWriting = true;
    }

    public void resumeWriting()
    {
        deferWriting = false;
        flush();//maybe not necessary, and this is maybe redundant.
    }

    public boolean internalNodeContainsSpaceForNewKeyAndChild( long[] newKey )
    {
        return NodeSize.internalNodeContainsSpaceForNewKeyAndChild( this, newKey );
    }

    @Override
    public int writePage( P page ) throws WriteCapacityExceededException
    {
        return 0;
    }

    @Override
    public Page readPage( int pageId )
    {
        return null;
    }
}
