/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.storage;

import java.util.HashMap;
import java.util.Map;

public class InMemoryPageFile<P extends Page> implements PageFile<P>
{
    private final long pageSize;
    private Map<Long,Page> inMemoryStorage;

    public InMemoryPageFile( long pageSize )
    {
        this.pageSize = pageSize;
        inMemoryStorage = new HashMap<>();
    }

    @Override
    public int writePage( P page )
    {
        inMemoryStorage.put( page.getPageId(), page );
    }

    @Override
    public Page readPage( int pageId )
    {
        return inMemoryStorage.get( pageId );
    }
}
