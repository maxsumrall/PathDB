/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.models;

import java.util.List;

import static com.pathdb.pathIndex.models.PathComparator.comparePathPrefixToPath;
import static com.pathdb.pathIndex.models.PathComparator.comparePathPrefixToPathPrefix;
import static com.pathdb.pathIndex.models.PathComparator.comparePathToPath;

public interface PathInterface extends Comparable<PathInterface>
{
    long getPathId();

    List<Node> getNodes();

    int getLength();

    default int compareTo( PathInterface o )
    {
        if ( this instanceof PathPrefix )
        {
            if ( o instanceof Path)
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
}
