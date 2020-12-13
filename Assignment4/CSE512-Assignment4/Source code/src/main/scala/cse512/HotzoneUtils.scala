package cse512

object HotzoneUtils {

  // YOU NEED TO CHANGE THIS PART
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    val point = pointString.split(',').map(x => x.trim.toDouble)
    val rectangle = queryRectangle.split(',').map(x => x.trim.toDouble)
    var rectangleX_start = 0.0
    var rectangleY_start = 0.0
    var rectangleX_end = 0.0
    var rectangleY_end = 0.0
    val pointX = point(0)
    val pointY = point(1)


    if (rectangle(1) > rectangle(3)) {
      rectangleY_start = rectangle(3)
      rectangleY_end = rectangle(1)
    }
    else if (rectangle(1) < rectangle(3)) {
      rectangleY_start = rectangle(1)
      rectangleY_end = rectangle(3)
    }


    if (rectangle(0) > rectangle(2)) {
      rectangleX_start = rectangle(2)
      rectangleX_end = rectangle(0)
    }
    else if (rectangle(0) < rectangle(2)) {
      rectangleX_start = rectangle(0)
      rectangleX_end = rectangle(2)
    }
    if (pointX >= rectangleX_start && pointX <= rectangleX_end && pointY >= rectangleY_start && pointY <= rectangleY_end)
      true
    else
      false

   // return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
