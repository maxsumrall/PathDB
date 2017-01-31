/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

import java.util.List;

/**
 * Exists for abstracting this comparision logic between path prefixes and paths.
 */
public class AbstractPath implements Comparable<AbstractPath>
{
    public final long pathId;
    public final int length;
    public final List<Node> nodes;

    AbstractPath( long pathId, int length, List<Node> nodes)
    {
        this.pathId = pathId;
        this.length = length;
        this.nodes = nodes;
    }

    @Override
    public int compareTo( AbstractPath o )
    {
        if ( this instanceof PathPrefix )
        {
            if ( o instanceof Path )
            {
                return comparePathPrefixToPath( (PathPrefix) this, (Path) o );
            }
            else if ( o instanceof PathPrefix )
            {
                return comparePathPrefixToPathPrefix( (PathPrefix) this, (PathPrefix) o );
            }
        }
        else if ( this instanceof Path )
        {
            if ( o instanceof Path )
            {
                return comparePathToPath( (Path) this, (Path) o );
            }
            else if ( o instanceof PathPrefix )
            {
                // The comparision gets reversed here, so we need to invert the result.
                return -comparePathPrefixToPath( (PathPrefix) o, (Path) this );
            }
        }
        throw new UnsupportedOperationException(
                String.format( "Attempted comparision of unsupported types. Supported types are %s and %s.",
                        Path.class.getName(), PathPrefix.class.getName() ) );
    }

    private int comparePathPrefixToPath( PathPrefix a, Path b )
    {
        if ( a.pathId != b.pathId )
        {
            return a.pathId > b.pathId ? 1 : -1;
        }
        if ( a.length != b.length )
        {
            return a.length - b.length;
        }
        for ( int i = 0; i < a.prefixLength; i++ )
        {
            if ( a.nodes.get( i ).getId() - b.nodes.get( i ).getId() != 0 )
            {
                return Long.compare( a.nodes.get( i ).getId(), b.nodes.get( i ).getId() );
            }
        }
        if ( a.prefixLength != b.length )
        {
            return a.prefixLength - b.length;
        }
        return 0;
    }

    private int comparePathPrefixToPathPrefix( PathPrefix a, PathPrefix b )
    {
        if ( a == b )
        {
            return 0;
        }
        if ( a.pathId != b.pathId )
        {
            return a.pathId > b.pathId ? 1 : -1;
        }
        if ( a.length != b.length )
        {
            return a.length - b.length;
        }
        if ( a.prefixLength != b.prefixLength )
        {
            return a.length - b.length;
        }
        for ( int i = 0; i < a.prefixLength; i++ )
        {
            if ( a.nodes.get( i ).getId() - b.nodes.get( i ).getId() != 0 )
            {
                return Long.compare( a.nodes.get( i ).getId(), b.nodes.get( i ).getId() );
            }
        }
        return 0;
    }

    private int comparePathToPath( Path a, Path b )
    {
        if ( a == b )
        {
            return 0;
        }
        if ( a.pathId != b.pathId )
        {
            return a.pathId > b.pathId ? 1 : -1;
        }
        if ( a.length != b.length )
        {
            return a.length - b.length;
        }
        for ( int i = 0; i < a.length; i++ )
        {
            if ( a.nodes.get( i ).getId() - b.nodes.get( i ).getId() != 0 )
            {
                return Long.compare( a.nodes.get( i ).getId(), b.nodes.get( i ).getId() );
            }
        }
        return 0;
    }
}
