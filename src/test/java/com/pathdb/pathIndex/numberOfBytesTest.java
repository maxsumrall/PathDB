/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex;

/**
 * Created by max on 6/15/15.
 */
public class numberOfBytesTest {
    public static void main(String[] args) {
        long runs = 100000000l;
        long time = System.nanoTime();
        for (long i = 0; i < runs; i++) {
            explicit(i);
        }
        time = System.nanoTime() - time;
        System.out.println("Explicit total: " + time + " avg: " + (time+ 0.0)/runs);


        time = System.nanoTime();
        for (long i = 0; i < runs; i++) {
            math(i);
        }
        time = System.nanoTime() - time;
        System.out.println("Math.ceil + log2nlz, total: " + time + " avg: " + (time+ 0.0)/runs);

        time = System.nanoTime();
        for (long i = 0; i < runs; i++) {
            shift(i);
        }
        time = System.nanoTime() - time;
        System.out.println("Math.ceil + log2nlz, total: " + time + " avg: " + (time+ 0.0)/runs);

    }

    public static int explicit(long value){
        long abs = Math.abs(value);
        int minBytes = 8;
        if(abs <= 127){
            minBytes = 1;
        }
        else if(abs <= 32768){
            minBytes = 2;
        }
        else if(abs <= 8388608){
            minBytes = 3;
        }
        else if(abs <= 2147483648l){
            minBytes = 4;
        }
        else if(abs <= 549755813888l){
            minBytes = 5;
        }
        else if(abs <= 140737488355328l){
            minBytes = 6;
        }
        else if(abs <= 36028797018963968l){
            minBytes = 7;
        }
        return minBytes;
    }

    public static int math(long value){
        return (int) Math.ceil(log2nlz(value) / 8.0);
    }

    public static int log2nlz( long bits )
    {
        if( bits == 0 )
            return 0; // or throw exception
        return 31 - Long.numberOfLeadingZeros( bits );
    }


    public static int shift (long value){
        int n = 0;
        while (value != 0) {
            value = value >> 8;
            n ++;
        }
        return n;
    }
}
