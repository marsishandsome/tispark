package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

/**
 * SRTING type include:
 * 1. CHAR
 * 2. VARCHAR
 * 3. TINYTEXT
 * 4. TEXT
 * 5. MEDIUMTEXT
 * 6. LONGTEXT
 */
class StringOverflowSuite extends BaseDataSourceTest("test_data_type_string_overflow") {

  test("Test CHAR Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testCharOverflow(false)
  }

  test("Test CHAR as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testCharOverflow(true)
  }

  private def testCharOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 CHAR(8) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 CHAR(8))"
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test VARCHAR Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testVarcharOverflow(false)
  }

  test("Test VARCHAR as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testVarcharOverflow(true)
  }

  private def testVarcharOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 VARCHAR(8) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 VARCHAR(8))"
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test TINYTEXT Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTinyTextOverflow(false)
  }

  test("Test TINYTEXT as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTinyTextOverflow(true)
  }

  private def testTinyTextOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYTEXT, primary key (c1(4)))"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYTEXT)"
      )
    }

    val base = "0123456789"
    var str = ""
    for (i <- 1 to 30) {
      str = str + base
    }
    val row = Row(str)
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = s"value $str length > max length 255"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test TEXT Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTextOverflow(false)
  }

  test("Test TEXT as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTextOverflow(true)
  }

  private def testTextOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TEXT(8), primary key (c1(4)))"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TEXT(8))"
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
