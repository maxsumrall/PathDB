/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.storage;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;

public class DiskBlockStorageTest
{
    final static int page_size = 8192;
    @Test
    public void diskAndInMemoryBlockStorageMatchTest() throws Exception
    {
        // given
        File file = new File( "/tmp/pathdb_test/data.db" );
        file.deleteOnExit();
        BlockStorage diskMemory = new PersistedPageFile( file );
        BlockStorage inMemory = new InMemoryPageFile( page_size );

        // when
        byte[] bytes = new byte[page_size];
        for(int i = 0; i < page_size; i++)
        {
            bytes[i] = (byte) (i % Byte.MAX_VALUE);
        }
        diskMemory.writeBytes( 0, bytes );
        inMemory.writeBytes( 0, bytes );

        // then

        assertArrayEquals( inMemory.getBytes( 0 ), diskMemory.getBytes( 0 ) );
    }

}
