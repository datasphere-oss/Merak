package io.merak.etl.executor.spark.exceptions;

public class SnowFlakeExecutionException extends Exception
{
    public SnowFlakeExecutionException() {
    }
    
    public SnowFlakeExecutionException(final String message) {
        super(message);
    }
    
    public SnowFlakeExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public SnowFlakeExecutionException(final Throwable cause) {
        super(cause);
    }
    
    public SnowFlakeExecutionException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
