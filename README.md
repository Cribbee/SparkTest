# SparkTest
A project for learning spark.

进阶笔记

RDD算子：

sc.parallelize：通过并行化scala集合创建RDD
val rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8))
sc.makeRDD：调用的sc.parallelize 故而同理
val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
rdd.partitions.length：查看该rdd的分区数量
rdd1.partitions.length
Ps.查看源码sec.textFile def defaultMinPartitions: Int = math.min(defaultParallelism, 2) 发现默认最小分区为2
问题：是否是有多少个输入切片，就有多少个分区，看一下源代码
解答：
1、首先明确切片属于逻辑切分，而非真正的hdfs进行物理切分，参考文件实际大小和blockSize比较，而是记录了偏移量（从多少至多少为一个片）
2、idea领着读源码
3、调用方法逻辑 抽象出来 理想大小输入切片 HDFS源码中 与 spark设定的分区数产生影响

rdd.flatMap 与 rdd.map  RDD的api已经讲解较为清楚，map是将每个元素对应执行f函数，而flatMap对应的是将每个元素执行f函数后将其扁平化

RDD api源码解释
mapPartitionsWithIndex  一次拿出一个分区（分区中并没有数据，而是记录要读取哪些数据，真正生成的Task会读取多条数据），并且可以将分区的编号取出来
功能：取分区中对应的数据时，还可以将分区的编号取出来，这样就可以知道数据是属于哪个分区的（哪个区分对应的Task的数据）
//例1
//定义函数func 该函数的功能是将对应分区中的数据取出来，并且带上分区编号
val func = (index: Int, it: Iterator[Int]) => {
it.map(x => s"part: $index, val: $x")} 
val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9),2) //看2分区中哪部分在前，在后的归属情况
val rdd2 = rdd.mapPartitionsWithIndex(func)
rdd2.collect
#res12: Array[String] = Array(part:0,ele:1, part:0,ele:2, part:0,ele:3, part:0,ele:4, part:1,ele:5, part:1,ele:6, part:1,ele:7, part:1,ele:8, part:1,ele:9)
Ps.如果分区数量超过元素数量例如：sc.parallelize(List(1,2,3,4,5),8) 那么空的分区，该方法就会忽略不显示，但hdfs中还是有剩余空切片
aggregate 使用给定的组合函数和中性“初始值”，聚合每个分区的元素，然后聚合所有分区的结果。 此函数可以返回与此RDD，T的类型不同的结果类型U。因此，我们需要一个操作将T合并到U中和一个合并两个U的操作，如scala.TraversableOnce。 允许这两个函数修改并返回其第一个参数，而不是创建新的U以避免内存分配。局部先分组处理，再全局处理
//例1
val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3)
rdd1.aggregate(5)(math.max(_, _), _ + _)  //123 456 789 每个分区与参数（5）相比较，留下最大值后，聚合结果+（5）
Int = 25 // max(1,2,3,5)+max(4,5,6,5)+max(7,8,9,5)+5 =25
//例2
val rdd2 = sc.parallelize(List("a","b","c","d","e","f"),2)
rdd2.aggregate("")(_ + _, _ + _)
res17: String = abcdef
scala> rdd2.aggregate("")(_ + _, _ + _)
res18: String = defabc
rdd2.aggregate("=")(_ + _, _ + _)
res20: String = ==def=abc
 rdd2.aggregate("=")(_ + _, _ + _)
res21: String = ==abc=def
注：结果不同，由于_ + _方法相当于分区内拼接字符，_ + _总体聚合，2个分区故而下发2个task，但是2个并行task执行顺序是不一定的，先执行结束的就会先返回在前。 "=" 会在前后两个方法中都参与进来，故而有此结果
aggregateByKey 是根据key值来先局部分组处理再全局处理，与aggregate 不同的是，初始值只有在局部处理的时候才参与进来，全局处理不参加，原理涉及shuffle过程
//例1
val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
//求和
pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
res23: Array[(String, Int)] = Array((dog,12), (cat,17), (mouse,6))
//初始值特性
pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
res22: Array[(String, Int)] = Array((dog,100), (cat,200), (mouse,200))
collectAsMap 属于阻塞方法，就是会等待所有计算结果收集回来才会展示，将收集的结果以Map的形式返回。
注：这不会返回多重映射（因此，如果您对同一个键只有多个值，那么每个键的一个值保留在返回的map中）
//例1
val rdd = sc.parallelize(List(("a", 1), ("b", 2)))
rdd.collectAsMap
res24: scala.collection.Map[String,Int] = Map(b -> 2, a -> 1)

Executor是个进程，task根据分区下发到Worker下的Executor，上面记录着要执行的业务逻辑，task相当于一个实例，好比写程序写了个类，类中定义方法，方法就可以计算逻辑，然后通过类就可以创建一些实例（对象）就可以对数据进行计算了
实际工况下不要collect到Driver端：
1、Driver端无法承受大量收集来的数据，会崩溃
2、这种只有一个连接，网络延迟大
3、如果直接从Worker写入数据库不仅减少一步收集步骤，还是多个连接同时传输，提升效率

countByKey 是统计看Key出现了几次，与Value无关
countByValue 返回此RDD中每个唯一值(元组是一个整体)的计数，作为(Value，计数) 对的本地映射。只有在结果Map预期很小时，才应使用此方法整个东西被加载到驱动程序的内存中。要处理非常大的结果，请考虑使用
//例1
val rdd1 = sc.parallelize(List(("a", 1), ("b", 2), ("b", 2), ("c", 2), ("c", 1)))
rdd1.countByKey
res27: scala.collection.Map[String,Long] = Map(b -> 2, a -> 1, c -> 2)
rdd1.countByValue
res28: scala.collection.Map[(String, Int),Long] = Map((b,2) -> 2, (c,2) -> 1, (a,1) -> 1, (c,1) -> 1)
filterByRange 包含边界，将指定范围内的对象过滤出来
//例1
val rdd1 = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("a", 1)))
val rdd2 = rdd1.filterByRange("b", "d")
rdd2.collect
res26: Array[(String, Int)] = Array((c,3), (d,4), (c,2)) //由于没有 "b"
flatMapValues 就是将Values进行切分扁平（flatMap）操作
//例1
val a = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
rdd3.flatMapValues(_.split(" “)).collect
res33: Array[(String, String)] = Array((a,1), (a,2), (b,3), (b,4))
foldByKey 折叠操作，使用关联函数和中性“初始值”合并每个键的值，（面试可用）与reduceByKey、aggregateByKey、combineByKey非常类似，看源码理解原因，三者均是在底层调用了 combineByKeyWithClassTag 方法进行实现的，只不过在参数方面稍作调整修改
combineByKeyWithClassTag
源码解释：
使用自定义聚合集合组合每个键的元素的通用函数 功能：对于“组合类型”C，将RDD [(K，V)] 转换为RDD [(K，C)]类型的结果
   用户提供三种功能： 
- `createCombiner`，它将V变成C（例如，创建一个单元素列表）
- `mergeValue`，将V合并为C（例如，将其添加到列表的末尾）
- `mergeCombiners`，将两个C组合成一个。
//例1
val rdd1 = sc.parallelize(List("dog", "wolf", "cat", "bear"), 2)
val rdd2 = rdd1.map(x => (x.length, x))
val rdd3 = rdd2.foldByKey(“0")(_+_)
rdd3: Array[(Int, String)] = Array((4,0wolf0bear), (3,0dog0cat))

foreach 将函数f应用于此RDD的所有元素。
foreachPartition 将函数f应用于此RDD的每个分区，与scala集合中foreach结合使用
val rdd1 = sc.parallelize(List(1,2,3,4,5,6),2)
rdd1.foreach(e => println(e * 100))
rdd1.foreachPartition(it => it.foreach(x => println(x*1000)))
注：应用场景问题：如果要将计算数据存放到数据库当中，应该选择两种方法中的哪一个？
答：写入MySQL的业务逻辑需要放到foreach xx（中）而foreach需要写一条就要创建一个链接，频繁创建链接不好，而 foreachPartition 创建一个链接可以把一个分区的数据全部写完，更加高效
任务在执行的时候，由于是在Executor里执行的，除非收集到Driver端才会在Driver端展示，一般工况下都不会收集的（例外：count、take、topN…收集后比较少的数据操作可以）所以foreachPartition（）虽然是action但不会展示在Driver，而是展示在Worker端UI可以查看

combineByKey 通常传入三个函数完成计算，三个函数功能见图，第一个可以改变Value类型进而方便处理
//例1
val rdd1 = = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
val rdd2 = rdd1.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
//由于方法毕竟底层，所以需要制定变量类型，不可以直接(_+_,_+_)
rdd2.collect
//例2
val rdd4 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val rdd5 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
val rdd6 = rdd5.zip(rdd4).collect // 拉链操作
res49: Array[(Int, String)] = Array((1,dog), (1,cat), (2,gnu), (2,salmon), (2,rabbit), (1,turkey), (2,wolf), (2,bear), (2,bee))
val rdd7 = rdd6.combineByKey(x => ListBuffer(x), (m: ListBuffer[String], n: String) => m += n, (a: ListBuffer[String], b: ListBuffer[String]) => a ++= b)
//第2个函数是将分区中的key相同的元组放到一个笼子里即在List中添加元素，第3个函数式在全局合并所有的ListBuffer
rdd7.collect
res50: Array[(Int, scala.collection.mutable.ListBuffer[String])] = Array((1,ListBuffer(dog, cat, turkey)), (2,ListBuffer(gnu, salmon, rabbit, wolf, bear, bee)))

