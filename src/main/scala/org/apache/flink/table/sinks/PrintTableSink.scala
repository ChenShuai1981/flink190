//package org.apache.flink.table.sinks
//
//import java.lang.{Boolean => JBool}
//import java.sql.{Date, Time, Timestamp}
//import java.util.{Date => JDate}
//
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
//import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
//import org.apache.flink.table.runtime.functions.DateTimeFunctions
//import org.apache.flink.types.Row
//import org.apache.flink.util.StringUtils
//
///**
//  * A simple [[org.apache.flink.table.sinks.TableSink]] to output data to console.
//  *
//  */
//class PrintTableSink()
//  extends TableSinkBase[JTuple2[JBool, Row]]
//    with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
//    with UpsertStreamTableSink[Row] {
//
//  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]) = {
//    val sink: PrintSinkFunction = new PrintSinkFunction()
//    dataStream.addSink(sink).name(sink.toString)
//  }
//
//  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = new PrintTableSink()
//
//  override def setKeyFields(keys: Array[String]): Unit = {}
//
//  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}
//
//  //  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)
//
//  override def getRecordType: TypeInformation[Row] = {
//    new RowTypeInfo(getFieldTypes, getFieldNames)
//  }
//
//  /** Emits the DataStream. */
//  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]) = {
//    val sink: PrintSinkFunction = new PrintSinkFunction()
//    boundedStream.addSink(sink).name(sink.toString)
//  }
//}
//
///**
//  * Implementation of the SinkFunction writing every tuple to the standard output.
//  *
//  */
//class PrintSinkFunction() extends RichSinkFunction[JTuple2[JBool, Row]] {
//  private var prefix: String = _
//
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    val context = getRuntimeContext.asInstanceOf[StreamingRuntimeContext]
//    prefix = "task-" + (context.getIndexOfThisSubtask + 1) + "> "
//  }
//
//  override def invoke(in: JTuple2[JBool, Row]): Unit = {
//    val sb = new StringBuilder
//    val row = in.f1
//    for (i <- 0 until row.getArity) {
//      if (i > 0) sb.append(",")
//      val f = row.getField(i)
//      if (f.isInstanceOf[Date]) {
//        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime, "yyyy-MM-dd"))
//      } else if (f.isInstanceOf[Time]) {
//        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime, "HH:mm:ss"))
//      } else if (f.isInstanceOf[Timestamp]) {
//        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime,
//          "yyyy-MM-dd HH:mm:ss.SSS"))
//      } else {
//        sb.append(StringUtils.arrayAwareToString(f))
//      }
//    }
//
//    if (in.f0) {
//      System.out.println(prefix + "(+)" + sb.toString())
//    } else {
//      System.out.println(prefix + "(-)" + sb.toString())
//    }
//  }
//
//  override def close(): Unit = {
//    this.prefix = ""
//  }
//
//  override def toString: String = "Print to System.out"
//}