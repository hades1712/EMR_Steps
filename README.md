# EMR_Steps
## 背景介绍
IDC客户上云也会伴随将IDC中的一些大数据的工作负载迁移到云上来，和大多数线下Hadoop集群相比，亚马逊云科技也提供了更为适合云上的Hadoop服务：Amazon EMR。本文对如何对接线下的调度系统，通过脚本 API 进行集群动态启停，依赖作业提交，并通过spot instance的支持，来进行动态的负载支撑，从而降低成本
并能够兼容线下的调度方式，在IDC中支持SQL和Shell两种提交方式；
最终结果数据可以继续和线下数据无缝交互。

## 一、方案选择
考虑到对线下调度系统的兼容性，选择了EMR：EMR本身和线下Hadoop较为接近，客户上手更快；EMR可以通过多种方式直接提交Spark SQL，不需要经过转换或者封装；以及给客户提供了Spark，Hive，甚至Presto等更多的选择；同时还提供了对Spot Instance的支持，降低成本。
## 二、方案架构
架构说明：我们将整个架构无缝集成入客户的调度系统中，包含了数据，代码，流程三部分。
![avatar](https://github.com/hades1712/EMR_Steps/blob/main/migrating-spark-sql-tasks-to-amazon-emr1.png)
所有的数据和代码都存放在S3上，流程存放在客户的调度系统里，所以EMR是一个无状态的计算服务，每日动态启动，完成当天的ETL工作之后，再终止掉该集群，最大化发挥云上的弹性优势；
和客户的调度系统集成EMR的启动脚本，封装成Shell，并记录下集群id；附录1提供了EMR提交任务的几种方式供参考；
通过客户调度系统，往该集群id提交Steps的方式提交Spark SQL脚本，并给调度系统返回Steps的执行日志；这里我们选择了更云原生的Steps提交方式；
最终确认所有的任务执行完毕，通过客户调度关闭集群。
## 三、方案实施
存储：新建S3 Bucket和prefix，将所有的数据和依赖包和输出放在上面；
元数据管理：IDC中一般使用MySQL作为后端的Hive Metastore，但我们云上优先选择托管的Glue元数据目录服务，主要好处有两点：免于管理运维；成本较MySQL更为低廉；
计算：考虑到性能，优先选择EMR 5.32之后的版本，亚马逊云科技的服务团队为我们带来了大量的Spark和Hive性能调优；但考虑到兼容性，此次并没有选择使用了Hadoop 3/Hive 3/Spark 3的EMR 6.2.0，降低了迁移成本，最终选择了EMR 5.33.0作为本次上云的最终版本。当然， 由于在第一步和第二步已经做到了存算分离，后续我们可以随时同步跟随EMR升级的步伐，获取更好的性能和特性而不需要任何额外的负担和成本；
机型选择：根据到客户的任务性质和数据量，选择单Master方案，Master Node为r5d.2xlarge一台，Core Node为r5d.12xlarge两台；最终每天所有ETL任务跑完的费用只需要3-5美元。
集群建立：shell封装；
任务提交：EMR Steps提交方式，使用shell封装
动态开启集群并获取相应的集群信息和状态，传递给调度系统 参考start_cluster.py
根据依赖提交相应的作业：boto3_hive1.py
根据调度系统检查作业状态，并关闭集群：close_cluster.py
UDF嵌入：一般用户会在提交查询语句的时候注册临时UDF，临时的UDF并不能在集群起停整个生命周期保留，所以在集群创建后将UDF统一上传到S3，进行统一注册：
create function if not EXISTS xxx as "UDF_name" USING JAR 's3://s3_UDF.jar_URL'

