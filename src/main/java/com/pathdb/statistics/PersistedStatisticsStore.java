/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.statistics;

import java.util.concurrent.ConcurrentHashMap;


//todo persist this, probably in lmdb also
public class PersistedStatisticsStore implements StatisticsStoreReader, StatisticsStoreWriter
{
    ConcurrentHashMap<Long,Long> store;

    public PersistedStatisticsStore()
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
