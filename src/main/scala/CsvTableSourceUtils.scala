import java.io.File

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object CsvTableSourceUtils {

  def genWordCountSource: CsvTableSource = {
    val csvRecords = Seq(
      "words",
      "Hello Flink",
      "Hi, Apache Flink",
      "Apache FlinkBook"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("words"),
      Array(
        Types.STRING
      ),
      "#",
      CommonUtils.line,
      '`',
      true,
      "%",
      true
    )
  }


  def genRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "rowtime ,currency   ,rate",
      "1   ,US Dollar  , 102",
      "1   ,Euro       , 114",
      "1  ,Yen        ,   1",
      "5   ,Euro       , 116",
      "7   ,Euro       , 119"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("rowtime","currency","rate"),
      Array(
        Types.SQL_TIMESTAMP,Types.STRING,Types.LONG
      ),
      ",",
      CommonUtils.line,
      '`',
      true,
      "%",
      true
    )
  }

  def genEventRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "ts#currency#rate",
      "1#US Dollar#102",
      "1#Euro#114",
      "1#Yen#1",
      "3#Euro#116",
      "5#Euro#119",
      "7#Pounds#108"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString(CommonUtils.line), "csv_source_rate", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("ts","currency","rate"),
      Array(
        Types.LONG,Types.STRING,Types.LONG
      ),
      "#",
      CommonUtils.line,
      '`',
      true,
      "%",
      true
    )
  }

  def genRatesOrderSource: CsvTableSource = {

    val csvRecords = Seq(
      "rowtime#currency#amount",
      "2#Euro#2",
      "3#US Dollar#1",
      "4#Yen#50",
      "5#Euro#3"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString(CommonUtils.line), "csv_source_order", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("rowtime", "currency", "amount"),
      Array(
        Types.SQL_TIMESTAMP,Types.STRING,Types.LONG
      ),
      "#",
      CommonUtils.line,
      '`',
      true,
      "%",
      true
    )
  }


  /**
    * Example:
    * genCsvSink(
    *   Array[String]("word", "count"),
    *   Array[TypeInformation[_] ](Types.STRING, Types.LONG))
    */
  def genCsvSink(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    val tempFile = File.createTempFile("csv_sink_", "tem")
    if (tempFile.exists()) {
      tempFile.delete()
    }
    new CsvTableSink(tempFile.getAbsolutePath).configure(fieldNames, fieldTypes)
  }

}