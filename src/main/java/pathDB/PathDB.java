package pathDB;

import pathIndex.tree.IndexTree;

import java.io.IOException;

public class PathDB
{
    private final IndexTree pathIndex;

    public PathDB( IndexTree pathIndex )
    {
        this.pathIndex = pathIndex;
    }

    public Iterable<Path> getPaths( PathPrefix pathPrefix ) throws IOException
    {
        return pathIndex.find( pathPrefix );
    }

    public void insert( Path path )
    {

    }
}
