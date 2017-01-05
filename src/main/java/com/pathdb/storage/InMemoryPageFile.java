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
