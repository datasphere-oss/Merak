package io.merak.etl.executor.spark.utils;

import java.util.*;
import scala.collection.*;

public class ScalaUtils
{
    public static <T> Seq<T> toSeq(final List<T> items) {
        return (Seq<T>)JavaConversions.asScalaBuffer((List)items);
    }
}
