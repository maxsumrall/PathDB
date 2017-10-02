/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathDB;

import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Collections.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PathTest
{
    private static final Random random = new Random();

    @Test
    public void samePathsEqualEachOtherTest() throws Exception
    {
        // given
        Path a = ImmutablePath.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 42 ) ).build();
        Path b = ImmutablePath.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 42 ) ).build();

        // then
        assertEquals( a, a );
        assertEquals( a, b );
    }

    @Test
    public void differentPathsAreNotEqualsTest() throws Exception
    {
        // given
        Path a = ImmutablePath.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 42 ) ).build();
        Path b = ImmutablePath.builder().pathId( 42 ).addAllNodes( equalNodes( 4, 24 ) ).build();
        Path c = ImmutablePath.builder().pathId( 42 ).addAllNodes( equalNodes( 3, 42 ) ).build();

        List<Node> differentNodes = equalNodes( 3, 42 );
        differentNodes.remove( differentNodes.size() - 1 );
        differentNodes.add( new Node( 43 ) );

        Path d = ImmutablePath.builder().pathId( 42 ).addAllNodes( differentNodes ).build();

        // then
        assertFalse( a.equals( b ) );
        assertFalse( b.equals( a ) );
        assertFalse( a.equals( c ) );
        assertFalse( c.equals( a ) );
        assertFalse( c.equals( d ) );
        assertFalse( d.equals( c ) );
    }

    @Test
    public void pathOrderingOnFirstNodeTest() throws Exception
    {
        // given
        Path a = ImmutablePath.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 1 ) ).build();
        Path b = ImmutablePath.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 2 ) ).build();
        Path c = ImmutablePath.builder().pathId( 42 ).addAllNodes( incrementingNodes( 4, 3 ) ).build();

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
        Path a = ImmutablePath.builder().pathId( 42 ).addAllNodes( nodesA ).build();
        Path b = ImmutablePath.builder().pathId( 42 ).addAllNodes( nodesB ).build();
        Path c = ImmutablePath.builder().pathId( 42 ).addAllNodes( nodesC ).build();

        // then
        assertEquals( -1, a.compareTo( b ) );
        assertEquals( 1, b.compareTo( a ) );

        assertEquals( -1, b.compareTo( c ) );
        assertEquals( 1, c.compareTo( b ) );

        assertEquals( -1, a.compareTo( c ) );
        assertEquals( 1, c.compareTo( a ) );
    }

    @Test
    public void sequentialPathsComparisionTest() throws Exception
    {
        // given
        List<Path> paths = generateSequentialPaths( 42, 4, 10 );

        //then
        assertTrue( pathsAreSorted( paths ) );
    }

    @Test
    public void pathSortingTest() throws Exception
    {
        // given
        List<Path> paths = generateRandomPaths( 4, 10 );

        // when
        sort( paths );

        // then
        assertTrue( pathsAreSorted( paths ) );

    }

    private boolean pathsAreSorted( List<Path> paths )
    {
        for ( int i = 0; i < paths.size(); i++ )
        {
            if ( i < paths.size() - 1 )
            {
                //then
                if ( paths.get( i ).compareTo( paths.get( i + 1 ) ) != -1 )
                {
                    return false;
                }
            }
        }
        return true;
    }

    public static List<Node> equalNodes( int count, long id )
    {
        List<Node> nodes = new LinkedList<>();
        IntStream.range( 0, count ).forEach( ( i ) -> nodes.add( new Node( id ) ) );
        return nodes;
    }

    public static List<Node> incrementingNodes( int count, long startingId )
    {
        List<Node> nodes = new LinkedList<>();
        LongStream.range( startingId, startingId + count ).forEach( ( i ) -> nodes.add( new Node( i ) ) );
        return nodes;
    }

    public static Path simplePath( long pathID, int length, Long value )
    {
        List<Node> nodes = new ArrayList<>( length );

        for ( int i = 0; i < length; i++ )
        {
            nodes.add( new Node( value ) );
        }

        return ImmutablePath.builder().pathId( pathID ).addAllNodes( nodes ).build();
    }

    public static Path randomPath( int length )
    {
        List<Node> nodes = new ArrayList<>( length );
        for ( int i = 0; i < length; i++ )
        {
            nodes.add( new Node( random.nextLong() ) );
        }
        return ImmutablePath.builder().pathId( 42 ).addAllNodes( nodes ).build();
    }

    public static List<Path> generateRandomPaths( int length, int amount )
    {
        List<Path> paths = new ArrayList<>( amount );
        for ( long i = 0; i < amount; i++ )
        {
            paths.add( randomPath( length ) );
        }
        return paths;
    }

    public static List<Path> generateSequentialPaths( long pathId, int length, int amount )
    {
        List<Path> paths = new ArrayList<>( amount );
        for ( long i = 0; i < amount; i++ )
        {
            paths.add( simplePath( pathId, length, i ) );
        }
        return paths;
    }

}
