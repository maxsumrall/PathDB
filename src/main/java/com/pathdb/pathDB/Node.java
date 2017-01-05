package com.pathdb.pathDB;

import java.util.Objects;

public class Node
{
    private final long id;

    public Node( long id )
    {

        this.id = id;
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
        Node node = (Node) o;
        return Objects.equals( id, node.id );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id );
    }

    @Override
    public String toString()
    {
        return "Node{" + "id=" + id + '}';
    }

    public long getId()
    {
        return id;
    }
}
