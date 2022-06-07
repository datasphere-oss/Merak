package io.merak.etl.executor.hive.translator;

import io.merak.etl.sdk.translator.*;

public class SqlTranslatorState implements TranslatorState<StringBuilder>
{
    StringBuilder builder;
    
    public SqlTranslatorState() {
        this.builder = new StringBuilder();
    }
    
    public StringBuilder get() {
        return this.builder;
    }
}
