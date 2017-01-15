package com.pathdb.statistics;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStatisticsStore implements StatisticsStoreReader, StatisticsStoreWriter
{
    ConcurrentHashMap<Long,Long> store;

    public InMemoryStatisticsStore()
    {
        store = new ConcurrentHashMap<>();
    }

    @Override
    public long getCardinality( long pathId )
    {
        return store.get( pathId );
    }

    @Override
    public void setCardinality( long pathId, long newValue )
    {
        store.put( pathId, newValue );
    }

    @Override
    public void incrementCardinality( long pathId, long amount )
    {
        validatePositive( amount );
        store.compute( pathId, ( key, val ) ->
        {
            if ( val != null )
            {
                return val + amount;
            }
            else
            {
                return amount;
            }
        } );
    }

    @Override
    public void decrementCardinality( long pathId, long amount )
    {
        validatePositive( amount );
        store.compute( pathId, ( key, val ) ->
        {
            if ( val != null && val >= amount )
            {
                return val - amount;
            }
            else
            {
                throw new IllegalStateException( String.format(
                        "Attempted to decrement cardinality of pathid %s by " + "amount %s " +
                                "when it's reported cardinailty is only %s.", pathId, amount, val ) );
            }
        } );
    }

    private void validatePositive( long amount )
    {
        if ( amount < 0 )
        {
            throw new IllegalArgumentException( "Cardinality modifications must be positive." );
        }
    }
}
