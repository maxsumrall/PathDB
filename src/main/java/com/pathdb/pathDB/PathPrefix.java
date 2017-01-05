package com.pathdb.pathDB;

import java.util.List;
import java.util.Objects;

public class PathPrefix extends AbstractPath
{
    public final int length;
    public final List<Node> nodes;
    public final int prefixLength;


    public PathPrefix( int pathLength, List<Node> nodes )
    {
        this.length = pathLength;
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
        return length == that.length && Objects.equals( nodes, that.nodes );
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
