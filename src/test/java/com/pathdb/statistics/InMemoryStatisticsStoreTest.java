package com.pathdb.statistics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InMemoryStatisticsStoreTest
{
    @Test
    public void shouldReportCorrectStatistics() throws Exception
    {
        // given
        InMemoryStatisticsStore inMemoryStatisticsStore = new InMemoryStatisticsStore();

        // when
        inMemoryStatisticsStore.incrementCardinality( 42, 99 );
        inMemoryStatisticsStore.incrementCardinality( 99, 42 );

        inMemoryStatisticsStore.decrementCardinality( 42, 9 );
        inMemoryStatisticsStore.decrementCardinality( 99, 2 );
        // then

        assertEquals( 90, inMemoryStatisticsStore.getCardinality( 42 ) );
        assertEquals( 40, inMemoryStatisticsStore.getCardinality( 99 ) );
    }

    @Test
    public void shouldProtectFromNegativeInput() throws Exception
    {
        // given
        InMemoryStatisticsStore inMemoryStatisticsStore = new InMemoryStatisticsStore();

        // when
        try
        {
            inMemoryStatisticsStore.incrementCardinality( 42, -33 );
            fail( "Should not allow negative values" );
        }

        // then
        catch ( IllegalArgumentException e )
        {
            //expected.
        }

        //Testing the other method.
        // when
        try
        {
            inMemoryStatisticsStore.decrementCardinality( 42, -33 );
            fail( "Should not allow negative values" );
        }

        // then
        catch ( IllegalArgumentException e )
        {
            //expected.
        }
    }

    @Test
    public void shouldNotAllowNegativeCardinality() throws Exception
    {
        // given
        InMemoryStatisticsStore inMemoryStatisticsStore = new InMemoryStatisticsStore();

        // when
        try
        {
            inMemoryStatisticsStore.decrementCardinality( 42, 1 );
            fail( "Should not have allowed the cardinality to be negative." );
        }
        catch ( IllegalStateException e )
        {
            //expected
        }
        // then
    }
}
