package com.pathdb.statistics;

public interface StatisticsStoreWriter
{
    /**
     * Sets the cardinality of this path to the specific amount specified.
     * @param pathId
     * @param newValue
     */
    void setCardinality(long pathId, long newValue);

    /**
     * Increases the cardinality of this path by the amount specified.
     * @param pathId The path to increase the cardinality of.
     * @param amount The amount to increase the cardinality by.
     */
    void incrementCardinality(long pathId, long amount);

    /**
     * Decrements the cardinality of this path by the amount specified.
     * @param pathId The path to decrease the cardinality of.
     * @param amount The amount to decrease the cardinality by.
     */
    void decrementCardinality(long pathId, long amount);
}
