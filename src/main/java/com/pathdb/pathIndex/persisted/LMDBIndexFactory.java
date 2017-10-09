/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.persisted;

import static com.jakewharton.byteunits.BinaryByteUnit.GIBIBYTES;
import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

import com.jakewharton.byteunits.BinaryByteUnit;
import com.pathdb.pathIndex.PathIndex;
import com.pathdb.statistics.PersistedStatisticsStore;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileSystemException;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;

public class LMDBIndexFactory {
    protected File dbDir;
    protected File path;
    protected Env<ByteBuffer> env;
    protected Dbi<ByteBuffer> db;
    protected long val;
    protected BinaryByteUnit unit;

    public LMDBIndexFactory(File dbDir) {
        this.dbDir = dbDir;
        if (!this.dbDir.isDirectory()) {
            throw new UnsupportedOperationException("Must provide folder/directory of store.");
        }
        val = 1;
        unit = GIBIBYTES;
        db = null;
    }

    public LMDBIndexFactory withMaxDBSize(long val, BinaryByteUnit unit) {
        this.val = val;
        this.unit = unit;
        return this;
    }

    public PathIndex build() throws FileSystemException
    {
        env = create().setMapSize( unit.toBytes( val ) ).setMaxDbs( 1 ).open( dbDir );
        db = env.openDbi( "pathdb.db", MDB_CREATE );

        return new LMDB(env, db, new PersistedStatisticsStore());
    }
}
