/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;


public class PrimitiveLongArray {
    private long[] tmp_array = new long[20]; //temporary array much longer than ever necessary.
    private int index = 0;
    public void put(long item){
        tmp_array[index++] = item;
    }
    public long[] get(){
        long[] ret = new long[index];
        System.arraycopy(tmp_array, 0, ret, 0, index);
        index = 0;
        Arrays.fill(tmp_array, 0);
        return ret;
    }
    public byte[] getAsBytes(){
        byte[] ret = new byte[index * 8];
        ByteBuffer bb = ByteBuffer.wrap(ret);
        for(int i = 0; i < index; i++){
            bb.putLong(tmp_array[i]);
        }
        index = 0;
        Arrays.fill(tmp_array, 0);
        return ret;
    }
}
