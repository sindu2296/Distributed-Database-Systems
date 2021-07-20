package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }


  def neighborsCount(min_x: Double, min_y: Double, min_z: Double, max_x: Double, max_y: Double, max_z: Double, x: Double, y: Double, z: Double):Int=
  {
    var out=0;
    var a=7;
    var b=11;
    var c=17;
    var d=26;
    if (x==min_x || x==max_x)
    {
      out =out + 1;
    }
    if (y==min_y || y==max_y)
    {
      out =out + 1;
    }
    if (z==min_z || z==max_z)
    {
      out =out + 1;
    }
    if(out==1)
      return c
    else if(out==2)
      return b
    else if(out==3)
      return a
    else
      return d
  }

  def zScore(x: Int, y: Int, z: Int, mean:Double, sd: Double, neighbour: Int, hotness: Int, numcells: Int): Double =
  {
    val num = (hotness.toDouble - (mean*neighbour.toDouble))
    val den = sd*math.sqrt((((numcells.toDouble*neighbour.toDouble) -(neighbour.toDouble*neighbour.toDouble))/(numcells.toDouble-1.0).toDouble).toDouble).toDouble
    return (num/den).toDouble
  }



  // YOU NEED TO CHANGE THIS PART
}
