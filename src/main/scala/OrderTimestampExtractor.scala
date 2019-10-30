import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class OrderTimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}