/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import com.pathdb.pathIndex.inMemoryTree.InMemoryIndexFactory;
import org.junit.Test;

import static org.junit.Assert.fail;

public class inMemoryIndexFactoryTest
{

    @Test
    public void inMemoryIndexTest() throws Exception
    {
        // given
        IndexFactory factory = new InMemoryIndexFactory();

        // when
        PathIndex ephemeralIndex = factory.getInMemoryIndex();

        // then
        //all good.
    }

    @Test
    public void diskBackedTest() throws Exception
    {
        // given
        IndexFactory factory = new InMemoryIndexFactory();

        // when
        try
        {
            PathIndex persistedDiskBasedIndex = factory.getPersistedDiskBasedIndex();
            fail( "this is not supported yet." );
        }
        catch ( UnsupportedOperationException e )
        {
            //expected
        }
        // then
        //all good.
    }
}
