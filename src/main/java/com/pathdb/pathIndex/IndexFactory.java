package com.pathdb.pathIndex;

public interface IndexFactory
{
    PathIndex getEphemeralIndex();

    PathIndex getPersistedDiskBasedIndex();
}
