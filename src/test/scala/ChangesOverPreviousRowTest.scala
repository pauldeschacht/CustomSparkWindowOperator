package io.nomad47

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.TimeZone

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions2.changed //import the custom operator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.FunSuite

trait AbstractStatusRecord {def groupId: Integer; def date: Timestamp; def status: Integer; }
case class TestRecord(groupId:Integer, date: Timestamp, status:Integer, title: String) extends AbstractStatusRecord
case class TestRecordChanged(groupId:Integer, date: Timestamp, status:Integer, title: String, changed: Boolean) extends AbstractStatusRecord

object TestRecord {
  def getTimestamp(s: String) : Timestamp = {
    Timestamp.valueOf(LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TimeZone.getTimeZone("UTC").toZoneId)))
  }
  def create(groupId:Integer, s: String, status:Integer) = new TestRecord(groupId, getTimestamp(s), status, "")
  def create(groupId:Integer, s: String, status:Integer, title: String) = new TestRecord(groupId, getTimestamp(s), status, title)

  def create(groupId:Integer, s: String, status:Integer, changed: Boolean) = new TestRecordChanged(groupId, getTimestamp(s), status, "", changed)
  def create(groupId:Integer, s: String, status:Integer, title: String,changed: Boolean) = new TestRecordChanged(groupId, getTimestamp(s), status, title, changed)
}

class ChangesOverPreviousRowTest extends FunSuite {

  lazy implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ThothSnapshotTest")
      .getOrCreate()
  }

  import spark.implicits._
  implicit val encoder: Encoder[TestRecord] = Encoders.product[TestRecord]

  val base : Dataset[TestRecord] = List(
    TestRecord.create(1, "2020-01-10 00:00:00", 2),
    TestRecord.create(1, "2020-01-01 00:00:00", 0),
    TestRecord.create(1, "2020-01-02 00:00:00", 0, "title"),
    TestRecord.create(1, "2020-01-05 00:00:00", 0),
    TestRecord.create(2, "2020-01-08 00:00:00", 2),
    TestRecord.create(1, "2020-01-05 00:45:00", 0),
    TestRecord.create(2, "2020-01-06 00:00:00", 3),
    TestRecord.create(1, "2020-01-06 00:00:00", 1),
    TestRecord.create(1, "2020-01-07 00:00:00", 1),
    TestRecord.create(1, "2020-01-03 00:00:00", 0),
    TestRecord.create(1, "2020-01-04 00:00:00", 1),
    TestRecord.create(1, "2020-01-08 00:00:00", 0),
    TestRecord.create(1, "2020-01-09 00:00:00", 0),
    TestRecord.create(2, "2020-01-04 00:00:00", 2),
    TestRecord.create(2, "2020-01-01 00:00:00", 2)
  ).toDS

  val expected : DataFrame = List(
    TestRecord.create(1, "2020-01-01 00:00:00", 0, "", true),
    TestRecord.create(1, "2020-01-02 00:00:00", 0, "title", true),
    TestRecord.create(1, "2020-01-03 00:00:00", 0, "", true),
    TestRecord.create(1, "2020-01-04 00:00:00", 1, "", true),
    TestRecord.create(1, "2020-01-05 00:00:00", 0, "", true),
    TestRecord.create(1, "2020-01-05 00:45:00", 0, "", false),
    TestRecord.create(1, "2020-01-06 00:00:00", 1, "", true),
    TestRecord.create(1, "2020-01-07 00:00:00", 1, "", false),
    TestRecord.create(1, "2020-01-08 00:00:00", 0, "", true),
    TestRecord.create(1, "2020-01-09 00:00:00", 0, "", false),
    TestRecord.create(1, "2020-01-10 00:00:00", 2, "", true),
    TestRecord.create(2, "2020-01-01 00:00:00", 2, "", true),
    TestRecord.create(2, "2020-01-04 00:00:00", 2, "", false),
    TestRecord.create(2, "2020-01-06 00:00:00", 3, "", true),
    TestRecord.create(2, "2020-01-08 00:00:00", 2, "", true)
  ).toDF

  test("Select changing status") {
    // GroupBy the column "groupid" and within each group sort by "date"
    // Inside each sorted group, detect if the columns "status" or "title" have changed compared to the previous row
    // The first line of each group is considered to be changed.
    val result = base.withColumn("hasChanged", changed("status", "title").over(Window.partitionBy("groupId").orderBy("date")))
    assert(result.collect === expected.collect)

    val changedRows = result.filter("hasChanged == true")
    assert(changedRows.count == 11)
  }
}

