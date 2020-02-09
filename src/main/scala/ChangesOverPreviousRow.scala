package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, And, AttributeReference, EqualNullSafe, Expression, If, Literal}
import org.apache.spark.sql.types.{BooleanType, DataType}

// children is the list of relevant columns (columns used in the aggregation function)
// Example: the children in the expression ds.withColumn("rank", changed("status", "title").over(window)) are the columns "status" and "title"

case class ChangedOverPreviousRow(children: Seq[Expression]) extends AggregateWindowFunction {
  // define the type of the aggregate column
  override def dataType: DataType = BooleanType
  protected val zero = Literal(false)
  protected val one  = Literal(true)
  protected val changedOverPrevious = AttributeReference("changedOverPrev", dataType, nullable = false)()
  // initial values for each of the input columns (filter out non resolved expressions)
  protected val initAttrs   = children.filter(e => e.resolved).map(e => Literal.create(null, e.dataType))
  // keep the previous values of the relevant columns
  protected val prevAttrs   = children.filter(e => e.resolved).map(e => AttributeReference(e.sql, e.dataType)())
  // safely compare current values with previous values
  protected val isIdentical = children.zip(prevAttrs).map(EqualNullSafe.tupled).reduceOption(And).getOrElse(Literal(true))
  protected val getChanged  = If(isIdentical, Literal(false), Literal(true))

  override val initialValues: Seq[Expression] = one +: initAttrs
  override val updateExpressions : Seq[Expression] = getChanged +: children
  // buffer to hold partial aggregate
  override val aggBufferAttributes: Seq[AttributeReference] = changedOverPrevious +: prevAttrs
  // final value for aggregate function
  override val evaluateExpression: Expression = changedOverPrevious
  // used in explain
  override def prettyName: String = "changed_over_prev"
}
object functions2 {
  def changed(colName: String, colNames: String*): Column = Column(new ChangedOverPreviousRow((Seq(colName) ++ colNames).map(name => Column(name).expr)))
}