package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickupData")
  //pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  // YOU NEED TO CHANGE THIS PART

  pickupInfo = pickupInfo.where(col("x") >= minX and col("x") <= maxX and col("y") >= minY and col("y") <= maxY and col("z") >= minZ and col("z") <= maxZ)
  pickupInfo = pickupInfo.groupBy("x", "y", "z").count()

  val mean: Double = pickupInfo.agg(sum("count") / numCells).first.getDouble(0)
  val std: Double = math.sqrt(pickupInfo.agg(sum(pow("count", 2.0)) / numCells - math.pow(mean, 2.0)).first.getDouble(0))


  val givenData = spark.sql("select x,y,z from pickupData where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ).persist();
  givenData.createOrReplaceTempView("givenData")

  val findHotness = spark.sql("select x,y,z,count(*) as hotness from givenData group by z,y,x").persist();
  findHotness.createOrReplaceTempView("findHotness")


  spark.udf.register("neighborsCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
  => ((HotcellUtils.neighborsCount(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))))
  val Neighbours = spark.sql("select neighborsCount("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a.x,a.y,a.z) as neighbourCount, a.x as x,a.y as y,a.z as z, sum(b.hotness) as totalHotness from findHotness as a, findHotness as b where (b.x = a.x+1 or b.x = a.x or b.x = a.x-1) and (b.y = a.y+1 or b.y = a.y or b.y =a.y-1) and (b.z = a.z+1 or b.z = a.z or b.z =a.z-1) group by a.z,a.y,a.x ").persist()
  Neighbours.createOrReplaceTempView("totalHotness");
  Neighbours.show()

  spark.udf.register("ZScore", (x: Int, y: Int, z: Int, mean:Double, sd: Double, neighbour: Int, hotness: Int, numcells: Int) => ((
    HotcellUtils.zScore(x, y, z, mean, sd, neighbour, hotness, numcells))))

  val ZScore= spark.sql("select ZScore(x,y,z,"+mean+","+std+",neighbourCount,totalHotness,"+numCells+") as z_score,x, y, z from totalHotness order by z_score desc");
  ZScore.createOrReplaceTempView("z_score")

  val finalOutput = spark.sql("select x,y,z from z_score")
  finalOutput.createOrReplaceTempView("final")
  return finalOutput.repartition(1)
}
}
