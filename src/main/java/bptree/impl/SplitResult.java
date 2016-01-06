/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

public class SplitResult {
    public Long[] key = null;
    public long[] primkey = null;
    public long left;
    public long right;
    public SplitResult(Long[] k, long l, long r){key = k; left = l; right = r;}
    public SplitResult(long[] k, long l, long r){primkey = k; left = l; right = r;}
    public SplitResult(){}
}
