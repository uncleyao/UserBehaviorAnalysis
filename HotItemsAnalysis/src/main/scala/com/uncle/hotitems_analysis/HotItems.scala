package com.uncle.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/** 定义输入数据样例类 */
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
/** 定义窗口聚合结果样例类*/
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {
    /** 1. 创建执行环境 */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /** 设置时间语义 */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    /** 2. Source */
    // Kafka版本
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 自动更新Kafka版本，在pom里面已经设置
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
    // dataStream2是文件版本
      // val dataStream2 = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
    /** 提取字段作为时间戳，此时是升序 */
      .assignAscendingTimestamps( _.timestamp * 1000L )

    /** 3. Transform处理数据*/
    val processedStream = dataStream.filter(_.behavior=="pv")
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate( new CountAgg(), new WindowResult() ) // CountAgg的Out就是WindowResult的IN
        .keyBy(_.windowEnd) //按照窗口分组
        .process(new TopNHotItems(3) )
    /** 4. Sink，控制台输出 */
    processedStream.print()

    env.execute("hot items job")

  }
}

/** 自定义预聚合函数[In, Agg, Out] */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc
  //如果有重分区的结果
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
/** 另一个例子：自定义预聚合函数计算平均 */
class AverageAgg() extends AggregateFunction[UserBehavior, (Long,Int) , Double]{
  override def createAccumulator(): (Long, Int) = (0L, 0)
  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2+1 )
  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2
  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

/** 自定义窗口函数，输出ItemViewCount
 * 做keyby时，要用tuple????????????????
 * 052, 18:00分钟
 * */
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/** 自定义处理函数 */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(i)
    // 注册一个定时器
    context.timerService().registerEventTimeTimer( i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get() ){
      allItems += item
    }
    // 按照Count大小排序，sortBy默认升序，改为降序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse)
      // 取得前N个
      .take(topSize)

    // 清空状态
    itemState.clear()

    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间： ").append(new Timestamp( timestamp - 1 )).append("\n")
    // 输出每一个商品信息
    for (i <-  sortedItems.indices ){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":").append(" 商品ID=")
        .append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("================")
    // 控制输出频率
    Thread.sleep(1000)
    /** 需要out做collect */
    out.collect(result.toString())
  }


}