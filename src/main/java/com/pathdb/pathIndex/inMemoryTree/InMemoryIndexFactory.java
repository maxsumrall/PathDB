package com.pathdb.pathIndex.inMemoryTree;

import com.pathdb.pathIndex.IndexFactory;
import com.pathdb.pathIndex.PathIndex;

public class InMemoryIndexFactory implements IndexFactory
{
    private PathIndex index;
    public PathIndex getEphemeralIndex()
    {
        if(index == null)
        {
            index = new InMemoryIndex();
        }
        return index;
    }

    public PathIndex getPersistedDiskBasedIndex()
    {
        throw new UnsupportedOperationException( "The disk backed index is not available." );
    }
}
