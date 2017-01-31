/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class PathPrefix extends AbstractPath
{
    final int prefixLength;

    public PathPrefix( long pathId, int length )
    {
        super( pathId, length, emptyList() );
        this.prefixLength = 0;
    }

    public PathPrefix( long pathId, int length, List<Node> nodes )
    {
        super( pathId, length, nodes );
        this.prefixLength = nodes.size();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PathPrefix that = (PathPrefix) o;
        return pathId == that.pathId && length == that.length && Objects.equals( super.nodes, that.nodes );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pathId, length, nodes );
    }

    @Override
    public String toString()
    {
        return "PathPrefix{" + "pathId=" + pathId + ", length=" + length + ", nodes=" + nodes + "}\n";
    }

    public boolean validPrefix( Path path )
    {
        if(pathId != path.pathId)
        {
            return false;
        }

        for ( int i = 0; i < nodes.size(); i++ )
        {
            if ( !(nodes.get( i ).getId() == (path.nodes.get( i ).getId())) )
            {
                return false;
            }
        }
        return true;
    }
}
