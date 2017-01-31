/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.persisted;

import com.pathdb.pathIndex.Path;
import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.PathPrefix;
import com.pathdb.pathIndex.PathSerializer;
import com.pathdb.statistics.PersistedStatisticsStore;
import com.pathdb.statistics.StatisticsStoreReader;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class LMDB implements PathIndex
{
    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final PersistedStatisticsStore statisticsStore;

    public LMDB( Env<ByteBuffer> env, Dbi<ByteBuffer> db, PersistedStatisticsStore statistictsStore )
    {
        this.env = env;
        this.db = db;
        this.statisticsStore = statistictsStore;
    }

    @Override
    public Iterable<Path> getPaths( PathPrefix pathPrefix ) throws IOException
    {
        ByteBuffer key = PathSerializer.serialize( pathPrefix );
        ArrayList<Path> resultList = new ArrayList<>();
        try ( Txn<ByteBuffer> txn = env.txnRead() )
        {
            Cursor<ByteBuffer> byteBufferCursor = db.openCursor( txn );
            boolean results = byteBufferCursor.get( key, GetOp.MDB_SET_RANGE );

            while ( results )
            {
                ByteBuffer val = byteBufferCursor.val();
                Path path = PathSerializer.deserialize( val );
                if ( pathPrefix.validPrefix( path ) )
                {
                    resultList.add( path );
                }
                else
                {
                    break;
                }
                results = byteBufferCursor.next();
            }
        }
        return resultList;
    }

    @Override
    public void insert( Path path )
    {
        ByteBuffer buffer = PathSerializer.serialize( path );
        db.put( buffer, buffer );
        statisticsStore.incrementCardinality( path.pathId, 1 );
    }

    @Override
    public StatisticsStoreReader getStatisticsStore()
    {
        return null;
    }

    public void close()
    {
        env.close();
    }
}
