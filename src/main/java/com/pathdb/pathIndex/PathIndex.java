package com.pathdb.pathIndex;

import java.io.IOException;

public interface PathIndex
{
    Iterable<Path> getPaths( PathPrefix pathPrefix ) throws IOException;

    void insert( Path path );
}
