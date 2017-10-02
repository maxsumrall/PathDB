/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathDB;

import com.pathdb.pathIndex.models.ImmutablePathPrefix;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.models.PathPrefix;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.pathdb.pathDB.PathTest.equalNodes;
import static com.pathdb.pathDB.PathTest.incrementingNodes;
import static com.pathdb.pathDB.PathTest.simplePath;
import static java.util.Collections.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PathPrefixTest
{
    private static final Random random = new Random();

    @Test
    public void samePathPrefixPrefixesEqualEachOtherTest() throws Exception
    {
        // given
        PathPrefix a =
                ImmutablePathPrefix.of( 42, equalNodes( 4, 42 ) );
        PathPrefix b =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 42 ) ).build();

        // then
        assertEquals( a, a );
        assertEquals( a, b );
    }

    @Test
    public void differentPathPrefixPrefixesAreNotEqualsTest() throws Exception
    {
        // given
        PathPrefix a =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 42 ) ).build();
        PathPrefix b =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 24 ) ).build();
        PathPrefix c =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( equalNodes( 3, 42 ) ).build();

        List<Node> differentNodes = equalNodes( 3, 42 );
        differentNodes.remove( differentNodes.size() - 1 );
        differentNodes.add( new Node( 43 ) );

        PathPrefix d = ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( differentNodes ).build();

        // then
        assertFalse( a.equals( b ) );
        assertFalse( b.equals( a ) );
        assertFalse( a.equals( c ) );
        assertFalse( c.equals( a ) );
        assertFalse( c.equals( d ) );
        assertFalse( d.equals( c ) );
    }

    @Test
    public void pathPrefixOrderingOnFirstNodeTest() throws Exception
    {
        // given
        PathPrefix a =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 1 ) ).build();
        PathPrefix b =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 2 ) ).build();
        PathPrefix c =
                ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 3 ) ).build();

        // then
        assertEquals( -1, a.compareTo( b ) );
        assertEquals( 1, b.compareTo( a ) );

        assertEquals( -1, b.compareTo( c ) );
        assertEquals( 1, c.compareTo( b ) );

        assertEquals( -1, a.compareTo( c ) );
        assertEquals( 1, c.compareTo( a ) );
    }

    @Test
    public void pathOrderingOnLastNodeTest() throws Exception
    {
        List<Node> nodesA = equalNodes( 3, 1 );
        nodesA.add( new Node( 2 ) );

        List<Node> nodesB = equalNodes( 3, 1 );
        nodesB.add( new Node( 3 ) );

        List<Node> nodesC = equalNodes( 3, 1 );
        nodesC.add( new Node( 4 ) );

        // given
        PathPrefix a = ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( nodesA ).build();
        PathPrefix b = ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( nodesB ).build();
        PathPrefix c = ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( nodesC ).build();

        // then
        assertEquals( -1, a.compareTo( b ) );
        assertEquals( 1, b.compareTo( a ) );

        assertEquals( -1, b.compareTo( c ) );
        assertEquals( 1, c.compareTo( b ) );

        assertEquals( -1, a.compareTo( c ) );
        assertEquals( 1, c.compareTo( a ) );
    }

    @Test
    public void sequentialPathPrefixesComparisionTest() throws Exception
    {
        // given
        List<PathPrefix> paths = generateSequentialPathPrefixes( 42, 6, 4, 10 );

        //then
        assertTrue( pathPrefixesAreSorted( paths ) );
    }

    @Test
    public void pathSortingTest() throws Exception
    {
        // given
        List<PathPrefix> paths = generateRandomPathPrefixes( 6, 4, 10 );

        // when
        sort( paths );

        // then
        assertTrue( pathPrefixesAreSorted( paths ) );

    }

    @Test
    public void validPrefixTest()
    {
        Path path = simplePath( 42L, 4, 9999L );
        for ( int i = 0; i < 3; i++ )
        {
            PathPrefix pathPrefix = simplePathPrefix( 42L, 4, i, 9999L );
            assertTrue( pathPrefix.validPrefix( path ) );
        }
    }

    private boolean pathPrefixesAreSorted( List<PathPrefix> pathPrefixes )
    {
        for ( int i = 0; i < pathPrefixes.size(); i++ )
        {
            if ( i < pathPrefixes.size() - 1 )
            {
                //then
                if ( pathPrefixes.get( i ).compareTo( pathPrefixes.get( i + 1 ) ) != -1 )
                {
                    return false;
                }
            }
        }
        return true;
    }

    public static PathPrefix simplePathPrefix( long pathId, int actualLength, int numberOfNodes, Long value )
    {
        List<Node> nodes = IntStream.range( 0, numberOfNodes ).mapToObj( i -> new Node( value ) )
                .collect( Collectors.toCollection( () -> new ArrayList<>( numberOfNodes + 1 ) ) );

        return ImmutablePathPrefix.builder().pathId( pathId ).addAllNodes( nodes ).build();
    }

    public static PathPrefix randomPathPrefix( int prefixLength, int length )
    {
        List<Node> nodes = new ArrayList<>( length );
        for ( int i = 0; i < length; i++ )
        {
            nodes.add( new Node( random.nextLong() ) );
        }
        return ImmutablePathPrefix.builder().pathId( 42 ).addAllNodes( nodes ).build();
    }


    public static List<PathPrefix> generateRandomPathPrefixes( int prefixLength, int length, int amount )
    {
        List<PathPrefix> pathPrefixes = new ArrayList<>( amount );
        for ( long i = 0; i < amount; i++ )
        {
            pathPrefixes.add( randomPathPrefix( prefixLength, length ) );
        }
        return pathPrefixes;
    }

    public static List<PathPrefix> generateSequentialPathPrefixes( long pathId, int prefixLength, int length,
            int amount )
    {
        List<PathPrefix> pathPrefixes = new ArrayList<>( amount );
        for ( long i = 0; i < amount; i++ )
        {
            pathPrefixes.add( simplePathPrefix( pathId, prefixLength, length, i ) );
        }
        return pathPrefixes;
    }

}
