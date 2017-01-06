package com.pathdb.pathIndex.inMemory;

import org.junit.Test;
import pathIndex.Node;
import pathIndex.Path;
import pathIndex.PathPrefix;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class InMemoryIndexTest
{
    @Test
    public void simpleInMemoryTest() throws Exception
    {
        // given
        InMemoryIndex inMemoryIndex = new InMemoryIndex();
        List<Node> nodes = new ArrayList<>();
        nodes.add( new Node( 1 ) );
        nodes.add( new Node( 2 ) );
        nodes.add( new Node( 3 ) );
        Path path = new Path( 42, nodes );

        // when
        inMemoryIndex.insert( path );

        // then
        Iterable<Path> paths = inMemoryIndex.getPaths( new PathPrefix( 42, 3, emptyList() ) );
        Path next = paths.iterator().next();

        assertEquals( path, next );
    }

    @Test
    public void returnsAllPathsOfSameId() throws Exception
    {
        // given
        InMemoryIndex index = createTestDatabaseA();

        // when
        Iterable<Path> pathsId0 = index.getPaths( new PathPrefix( 0, 3 ) );
        Iterable<Path> pathsId42 = index.getPaths( new PathPrefix( 0, 3 ) );
        Iterable<Path> pathsId56 = index.getPaths( new PathPrefix( 0, 3 ) );
        Iterable<Path> pathsId99 = index.getPaths( new PathPrefix( 0, 3 ) );

        // then
        ArrayList<Path> results = new ArrayList<>();
        pathsId0.forEach( results::add );
        assertEquals( 100, results.size() );

        results.clear();
        pathsId42.forEach( results::add );
        assertEquals( 100, results.size() );

        results.clear();
        pathsId56.forEach( results::add );
        assertEquals( 100, results.size() );

        results.clear();
        pathsId99.forEach( results::add );
        assertEquals( 100, results.size() );
    }

    @Test
    public void returnsAllPathsOfSamePathPrefix() throws Exception
    {
        // given
        InMemoryIndex index = createTestDatabaseB();
        List<Node> nodes = new ArrayList<>( 1 );
        nodes.add( new Node( 7 ) );

        // when
        Iterable<Path> pathsId0 = index.getPaths( new PathPrefix( 0, 3, nodes ) );
        Iterable<Path> pathsId42 = index.getPaths( new PathPrefix( 0, 3, nodes ) );
        Iterable<Path> pathsId56 = index.getPaths( new PathPrefix( 0, 3, nodes ) );
        Iterable<Path> pathsId99 = index.getPaths( new PathPrefix( 0, 3, nodes ) );

        // then
        ArrayList<Path> results = new ArrayList<>();
        pathsId0.forEach( results::add );
        assertEquals( 10, results.size() );

        results.clear();
        pathsId42.forEach( results::add );
        assertEquals( 10, results.size() );

        results.clear();
        pathsId56.forEach( results::add );
        assertEquals( 10, results.size() );

        results.clear();
        pathsId99.forEach( results::add );
        assertEquals( 10, results.size() );
    }

    public InMemoryIndex createTestDatabaseA()
    {
        InMemoryIndex inMemoryIndex = new InMemoryIndex();
        for ( int i = 0; i < 1000; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                List<Node> nodes = new ArrayList<>();
                nodes.add( new Node( j ) );
                nodes.add( new Node( j ) );
                nodes.add( new Node( j ) );
                inMemoryIndex.insert( new Path( i, nodes ) );
            }
        }
        return inMemoryIndex;
    }

    public InMemoryIndex createTestDatabaseB()
    {
        InMemoryIndex inMemoryIndex = new InMemoryIndex();
        for ( int i = 0; i < 1000; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                List<Node> nodes = new ArrayList<>();
                nodes.add( new Node( j % 10 ) );
                nodes.add( new Node( j ) );
                nodes.add( new Node( j ) );
                inMemoryIndex.insert( new Path( i, nodes ) );
            }
        }
        return inMemoryIndex;
    }
}
