/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.models;

import org.immutables.value.Value;

@Value.Immutable
public abstract class PathPrefix implements PathInterface
{
    public int getPrefixLength(){
        return getNodes().size();
    }

    public boolean validPrefix( Path path )
    {
        if(getPathId() != path.getPathId())
        {
            return false;
        }

        for ( int i = 0; i < getNodes().size(); i++ )
        {
            if ( !(getNodes().get( i ).getId() == (path.getNodes().get( i ).getId())) )
            {
                return false;
            }
        }
        return true;
    }
}
