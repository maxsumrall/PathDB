/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.tree;

import java.util.LinkedList;

public class TreeNodeIDManager
{
    private static LinkedList<Long> pool = null;
    public static long currentID = 0;

    public TreeNodeIDManager()
    {
        if ( pool == null )
        {
            pool = new LinkedList<>();
        }
    }

    public static Long acquire()
    {
        if ( pool == null )
        {
            pool = new LinkedList<>();
        }
        if ( pool.size() > 0 )
        {
            return pool.pop();
        }
        else
        {
            return currentID++;
        }
    }

    public static void release( Long id )
    {
        pool.push( id );
    }

    public boolean isNodeIdInFreePool( Long search_id )
    {
        for ( Long id : pool )
        {
            if ( search_id.equals( id ) )
            {
                return true;
            }
        }
        return false;
    }
}
