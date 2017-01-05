package com.pathdb.pathDB;

import org.junit.Test;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static com.pathdb.pathDB.PathPrefixTest.simplePathPrefix;
import static com.pathdb.pathDB.PathTest.simplePath;

public class AbstractPathComparisionTest
{

    @Test
    public void prefixLessThanLongerPath() throws Exception
    {
        // given
        PathPrefix prefix = simplePathPrefix( 3, 2, 2L );
        Path path = simplePath( 4, 1L );

        // then
        assertThat( path, greaterThan( prefix ) );
    }

    @Test
    public void prefixEqualToPath() throws Exception
    {
        // given
        PathPrefix prefix = simplePathPrefix( 4, 2, 2L );
        Path path = simplePath( 4, 2L );

        // then
        assertThat( prefix, comparesEqualTo( path ) );
    }

    @Test
    public void prefixLessThanPath() throws Exception
    {
        // given
        PathPrefix prefix = simplePathPrefix( 4, 2, 2L );
        Path path = simplePath( 4, 3L );

        // then
        assertThat( prefix, lessThan( path ) );
    }
}
