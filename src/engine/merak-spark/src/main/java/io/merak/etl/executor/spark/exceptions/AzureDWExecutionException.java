package io.merak.etl.executor.spark.exceptions;

public class AzureDWExecutionException extends Exception
{
    public AzureDWExecutionException() {
    }
    
    public AzureDWExecutionException(final String message) {
        super(message);
    }
    
    public AzureDWExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public AzureDWExecutionException(final Throwable cause) {
        super(cause);
    }
    
    public AzureDWExecutionException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
