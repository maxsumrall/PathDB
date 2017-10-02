/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.models.PathInterface;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PathSerializer
{
    public static ByteBuffer serialize( PathInterface path )
    {
        int nodes = path.getLength() * Long.BYTES;
        int pathId = Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocateDirect( nodes + pathId );
        buffer.putLong( path.getPathId() );
        for ( Node node : path.getNodes() )
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
        return ImmutablePath.builder().addAllNodes(
                IntStream.range( 0, size ).mapToObj( i -> new Node( buffer.getLong() ) )
                        .collect( Collectors.toCollection( () -> new ArrayList<>( size ) ) ) )
                .pathId( pathId ).build();
    }
}
