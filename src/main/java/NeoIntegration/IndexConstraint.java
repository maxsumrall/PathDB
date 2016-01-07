/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

public class IndexConstraint{
    final public int indexInResultSet;
    final public String property;
    final public String value;

    public IndexConstraint(int indexInResultSet, String property, String value) {
        this.indexInResultSet = indexInResultSet;
        this.property = property;
        this.value = value;
    }
}
