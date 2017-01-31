/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.persisted;

import com.pathdb.pathDB.PathTest;
import com.pathdb.pathIndex.Path;
import com.pathdb.pathIndex.PathPrefix;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;

public class LMDBIndexFactoryTest
{

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void createLMDBTest() throws Exception
    {
        LMDB build = new LMDBIndexFactory( tmp.newFolder("testDir") )
                .withMaxDBSize( 42, MEBIBYTES )
                .build();
    }

    @Test
    public void openInsertCloseOpenRead() throws Exception
    {
        LMDB db = new LMDBIndexFactory( tmp.newFolder("testDir") )
            .withMaxDBSize( 42, MEBIBYTES )
            .build();

        Path path = PathTest.simplePath( 42L, 4, 42L );
        db.insert( path );

        PathPrefix pathPrefix = new PathPrefix( 42L, 4 );
        Iterable<Path> paths = db.getPaths( pathPrefix );
        for(Path p: paths)
        {
            System.out.println(p);
        }
    }
}
