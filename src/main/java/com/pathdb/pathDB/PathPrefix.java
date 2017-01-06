/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathDB;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class PathPrefix extends AbstractPath
{
    public final int length;
    public final List<Node> nodes;
    final int prefixLength;

    public PathPrefix( long pathId, int length )
    {
        super(pathId);
        this.length = length;
        this.nodes = emptyList();
        this.prefixLength = 0;
    }

    public PathPrefix( long pathId, int length, List<Node> nodes )
    {
        super( pathId );
        this.length = length;
        this.nodes = nodes;
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
        return pathId == that.pathId && length == that.length && Objects.equals( nodes, that.nodes );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( length, nodes );
    }

    @Override
    public String toString()
    {
        return "PathPrefix{" + "length=" + length + ", nodes=" + nodes + '}';
    }
}
