package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    {
      val temp = queryRectangle.split(",")
      val x1 = temp(0).toDouble
      val y1 = temp(1).toDouble
      val x2 = temp(2).toDouble
      val y2 = temp(3).toDouble
      val temp1 = pointString.split(",");
      val x = temp1(0).toDouble
      val y = temp1(1).toDouble
      if (y >= y1 && y <= y2 && x >= x1 && x <= x2) {
        (true)
      }
      else {
        (false)
      }
    }

    // YOU NEED TO CHANGE THIS PART
  }

}
