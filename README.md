
### CustomSparkWindowOperator

This is an example how to implement a custom SQL operator over a Spark SQL window, such as rank, ntile.

This implementation detects changes in a number of columns with respect to the previous row, within a window group. If a change in any of the columns is detected, the aggregate column ```hasChanged``` will be set to true, otherwise it will be set to false.

The custom SQL operator ```changed``` takes a number of column names over which a change needs to be detected. The result will be stored in the column ```hasChanged```.
```code
 val rowsWithChanged = df.withColumn("hasChanged", changed("status", "title").over(Window.partitionBy("id").orderBy("date")))
 val changedRows = rowsWithChanged.filter("hasChanged == true")
```

### Details

The implementation of the example is inspired by the [rank operator](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala#L632). 
The important difference is that the rank function uses the ```orderBy``` expressions for the aggregation, which are already resolved.
The initial calls (plan analysis, code generation phase) to ```ChangedOverPreviousRow``` contains non resolved children (the datatype is not yet known), therefore the child expressions are checked if resolved or not.  
    
The case class ```ChangesOverPreviousRow``` implements the [DeclarativeAggregate](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Expression-DeclarativeAggregate.html), which implements the [AggregrateFunction contract](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Expression-AggregateFunction.html).

 