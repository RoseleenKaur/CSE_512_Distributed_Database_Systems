package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
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
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val num_of_Cells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  pickupInfo.createOrReplaceTempView("pickupinfo")
  val points_in_the_cube = spark.sql("SELECT x, y, z FROM pickupinfo WHERE x <= " + maxX + " and y <= " + maxY +  " and z <= " + maxZ +" and x >= " + minX + " and y >= " + minY  + " and z >= " + minZ + " ORDER BY z, y, x").persist()
  points_in_the_cube.createOrReplaceTempView("points_in_the_cube")
  points_in_the_cube.show()


  val points_count = spark.sql("SELECT x, y, z, count(*) as pointValues FROM points_in_the_cube GROUP BY z, y, x order by z, y, x").persist()
  points_count.createOrReplaceTempView("points_count")
  points_count.show()

  val dfForPointNeighbors = spark.sql("SELECT t1.x, t1.y, t1.z, COUNT(t2.pointValues) as no_of_Neighbors, SUM(t2.pointValues) as sum_pv FROM points_count t1, points_count t2 WHERE  (ABS(t1.y-t2.y) = 0 OR ABS(t1.y-t2.y) = 1) AND ((ABS(t1.x-t2.x) = 0 OR ABS(t1.x-t2.x) = 1) AND (ABS(t1.z-t2.z) = 0 OR ABS(t1.z-t2.z) = 1)) GROUP BY t1.x, t1.y, t1.z")
  dfForPointNeighbors.show()
  dfForPointNeighbors.createOrReplaceTempView("dfForPointNeighbors")

  val sum_of_X = spark.sql("SELECT SUM(points_count.pointValues) as sum_of_X FROM points_count").first().getLong(0).toDouble
  val sum_of_X_sq = spark.sql("SELECT SUM(points_count.pointValues * points_count.pointValues) as sum_of_X_sq FROM points_count").first().getLong(0).toDouble
  val mean_of_X = (sum_of_X / num_of_Cells.toDouble).toDouble
  val standardDeviation_of_X = math.sqrt(sum_of_X_sq / num_of_Cells.toDouble - mean_of_X * mean_of_X)

  spark.udf.register("zScore", (x: Int, y: Int, z: Int, sum_pv: Double, no_of_Neighbors: Int) => ((HotcellUtils.zScore(x, y, z, minX, maxX, minY, maxY, minZ, maxZ, num_of_Cells.toDouble, sum_pv.toDouble, no_of_Neighbors, mean_of_X.toDouble, standardDeviation_of_X.toDouble))))
  val result_DF = spark.sql("SELECT table.x, table.y, table.z FROM (SELECT x, y, z, zScore(dfForPointNeighbors.x, dfForPointNeighbors.y, dfForPointNeighbors.z, dfForPointNeighbors.sum_pv, dfForPointNeighbors.no_of_Neighbors) as zscore FROM dfForPointNeighbors ORDER BY zscore desc) table")
  result_DF.show()
  return result_DF


  

  //return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
