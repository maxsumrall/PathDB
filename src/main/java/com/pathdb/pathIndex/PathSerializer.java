/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PathSerializer
{
    public static ByteBuffer serialize( Path path )
    {
        int nodes = path.length * Long.BYTES;
        int pathId = Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocateDirect( nodes + pathId );
        buffer.putLong( path.pathId );
        for ( Node node : path.nodes )
        {
            buffer.putLong( node.getId() );
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serialize( AbstractPath path )
    {
        int nodes = path.length * Long.BYTES;
        int pathId = Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocateDirect( nodes + pathId );
        buffer.putLong( path.pathId );
        for ( Node node : path.nodes )
        {
            buffer.putLong( node.getId() );
        }
        buffer.flip();
        return buffer;
    }

    public static Path deserialize( ByteBuffer buffer )
    {
        long pathId = buffer.getLong();
        int size = buffer.remaining() / Long.BYTES;
        List<Node> nodes = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            nodes.add( new Node( buffer.getLong() ) );
        }
        return new Path( pathId, nodes );

    }
}
