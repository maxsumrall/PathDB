package pathDB;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.Collections.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static pathDB.PathTest.equalNodes;
import static pathDB.PathTest.incrementingNodes;

public class PathPrefixTest
{
    private static final Random random = new Random();

    @Test
    public void samePathPrefixPrefixesEqualEachOtherTest() throws Exception
    {
        // given
        PathPrefix a = new PathPrefix( 6, equalNodes( 4, 42 ) );
        PathPrefix b = new PathPrefix( 6, equalNodes( 4, 42 ) );

        // then
        assertEquals( a, a );
        assertEquals( a, b );
    }

    @Test
    public void differentPathPrefixPrefixesAreNotEqualsTest() throws Exception
    {
        // given
        PathPrefix a = new PathPrefix( 6, equalNodes( 4, 42 ) );
        PathPrefix b = new PathPrefix( 6, equalNodes( 4, 24 ) );
        PathPrefix c = new PathPrefix( 6, equalNodes( 3, 42 ) );

        List<Node> differentNodes = equalNodes( 3, 42 );
        differentNodes.remove( differentNodes.size() - 1 );
        differentNodes.add( new Node( 43 ) );

        PathPrefix d = new PathPrefix( 6, differentNodes );

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
        PathPrefix a = new PathPrefix( 6, incrementingNodes( 4, 1 ) );
        PathPrefix b = new PathPrefix( 6, incrementingNodes( 4, 2 ) );
        PathPrefix c = new PathPrefix( 6, incrementingNodes( 4, 3 ) );

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
        PathPrefix a = new PathPrefix( 6, nodesA );
        PathPrefix b = new PathPrefix( 6, nodesB );
        PathPrefix c = new PathPrefix( 6, nodesC );

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
        List<PathPrefix> paths = generateSequentialPathPrefixes( 6, 4, 10 );

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

    public static PathPrefix simplePathPrefix( int prefixLength, int length, Long value )
    {
        List<Node> nodes = new ArrayList<>( length + 1 );

        for ( int i = 0; i < length + 1; i++ )
        {
            nodes.add( new Node( value ) );
        }

        return new PathPrefix( prefixLength, nodes );
    }

    public static PathPrefix randomPathPrefix( int prefixLength, int length )
    {
        List<Node> nodes = new ArrayList<>( length );
        for ( int i = 0; i < length; i++ )
        {
            nodes.add( new Node( random.nextLong() ) );
        }
        return new PathPrefix( prefixLength, nodes );
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

    public static List<PathPrefix> generateSequentialPathPrefixes( int prefixLength, int length, int amount )
    {
        List<PathPrefix> pathPrefixes = new ArrayList<>( amount );
        for ( long i = 0; i < amount; i++ )
        {
            pathPrefixes.add( simplePathPrefix( prefixLength, length, i ) );
        }
        return pathPrefixes;
    }

}
