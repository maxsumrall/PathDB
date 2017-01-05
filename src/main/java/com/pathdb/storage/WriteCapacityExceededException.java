package com.pathdb.storage;


/**
 * When you try to write to the page but the compressed file version was too large to fit.
 * Uncompressed pages will result in a no-op for this.
 */
public class WriteCapacityExceededException extends Exception
{
}
