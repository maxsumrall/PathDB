/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathDB;

import java.util.List;
import java.util.Objects;


public class Path extends AbstractPath
{
    public final int length;
    public List<Node> nodes;

    public Path( List<Node> nodes )
    {
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


}

