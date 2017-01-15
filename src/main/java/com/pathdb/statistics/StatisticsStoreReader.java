package com.pathdb.statistics;

/**
 * Provides statistics about different paths.
 */
public interface StatisticsStoreReader
{
    long getCardinality(long pathId);
}
