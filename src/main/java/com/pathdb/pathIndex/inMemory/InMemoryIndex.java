package com.pathdb.pathIndex.inMemory;

import pathIndex.AbstractPath;
import pathIndex.Node;
import pathIndex.Path;
import pathIndex.PathIndex;
import pathIndex.PathPrefix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class InMemoryIndex implements PathIndex
{
    private TreeMap<AbstractPath,AbstractPath> treeMap = new TreeMap<>();

    @Override
    public Iterable<Path> getPaths( PathPrefix pathPrefix ) throws IOException
    {
        return treeMap.subMap( pathPrefix, nextIncrementalPath( pathPrefix ) ).values().stream()
                .map( p -> (Path) p )::iterator;
    }

    @Override
    public void insert( Path path )
    {
        treeMap.put( path, path );
    }

    private PathPrefix nextIncrementalPath( PathPrefix pathPrefix )
    {
        long pathId = pathPrefix.pathId;
        int pathLength = pathPrefix.length;
        List<Node> nodes = new ArrayList<>( pathPrefix.nodes.size() );
        for ( Node node : pathPrefix.nodes )
        {
            nodes.add( new Node( node.getId() ) );
        }

        if ( pathPrefix.nodes.size() == 0 )
        {
            return new PathPrefix( pathId + 1, pathLength, nodes );
        }
        else
        {
            Node remove = nodes.remove( nodes.size() - 1 );
            nodes.add( new Node( remove.getId() + 1 ) );
        }
        return new PathPrefix( pathId, pathLength, nodes );
    }
}
