package pathDB;

import java.util.List;
import java.util.Objects;


public class Path implements Comparable<Path>
{
    private final int length;
    private List<Node> nodes;

    public Path( List<Node> nodes )
    {
        if ( nodes == null )
        {
            throw new IllegalArgumentException( "nodes must not be null" );
        }
        this.length = nodes.size();
        this.nodes = nodes;
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
        Path path = (Path) o;
        return length == path.length && Objects.equals( nodes, path.nodes );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( length, nodes );
    }

    @Override
    public String toString()
    {
        return "Path{" + "length=" + length + ", nodes=" + nodes + "}\n";
    }

    @Override
    public int compareTo( Path o )
    {
        if ( this == o )
        {
            return 0;
        }
        if ( length != o.length )
        {
            return length - o.length;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( nodes.get( i ).getId() - o.nodes.get( i ).getId() != 0 )
            {
                return Long.compare( nodes.get( i ).getId(), o.nodes.get( i ).getId() );
            }
        }
        return 0;
    }
}

