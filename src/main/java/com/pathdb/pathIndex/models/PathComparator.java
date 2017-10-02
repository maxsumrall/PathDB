/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.models;

public final class PathComparator
{
    public static int comparePathPrefixToPath( PathPrefix a, Path b )
    {
        if ( a.getPathId() != b.getPathId() )
        {
            return a.getPathId() > b.getPathId() ? 1 : -1;
        }
        if ( a.getLength() != b.getLength() )
        {
            return a.getLength() - b.getLength();
        }
        for ( int i = 0; i < a.getPrefixLength(); i++ )
        {
            if ( a.getNodes().get( i ).getId() - b.getNodes().get( i ).getId() != 0 )
            {
                return Long.compare( a.getNodes().get( i ).getId(), b.getNodes().get( i ).getId() );
            }
        }
        if ( a.getPrefixLength() != b.getLength() )
        {
            return a.getPrefixLength() - b.getLength();
        }
        return 0;
    }

    public static int comparePathPrefixToPathPrefix( PathPrefix a, PathPrefix b )
    {
        if ( a == b )
        {
            return 0;
        }
        if ( a.getPathId() != b.getPathId() )
        {
            return a.getPathId() > b.getPathId() ? 1 : -1;
        }
        if ( a.getLength() != b.getLength() )
        {
            return a.getLength() - b.getLength();
        }
        if ( a.getPrefixLength() != b.getPrefixLength() )
        {
            return a.getLength() - b.getLength();
        }
        for ( int i = 0; i < a.getPrefixLength(); i++ )
        {
            if ( a.getNodes().get( i ).getId() - b.getNodes().get( i ).getId() != 0 )
            {
                return Long.compare( a.getNodes().get( i ).getId(), b.getNodes().get( i ).getId() );
            }
        }
        return 0;
    }

    public static int comparePathToPath( Path a, Path b )
    {
        if ( a == b )
        {
            return 0;
        }
        if ( a.getPathId() != b.getPathId() )
        {
            return a.getPathId() > b.getPathId() ? 1 : -1;
        }
        if ( a.getLength() != b.getLength() )
        {
            return a.getLength() - b.getLength();
        }
        for ( int i = 0; i < a.getLength(); i++ )
        {
            if ( a.getNodes().get( i ).getId() - b.getNodes().get( i ).getId() != 0 )
            {
                return Long.compare( a.getNodes().get( i ).getId(), b.getNodes().get( i ).getId() );
            }
        }
        return 0;
    }
}
