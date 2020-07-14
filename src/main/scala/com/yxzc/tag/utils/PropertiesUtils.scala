package com.yxzc.tag.utils

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.JavaConverters._

object PropertiesUtils {

  def readPropertiesFile(path: String, isDebug: Boolean): Map[String, String] = {
    var in: InputStream = null
    if (isDebug) {
      in = getClass.getResourceAsStream(path)
    } else {
      in = new FileInputStream(path)
    }
    //    val in : InputStream = getClass.getResourceAsStream(path)
    val properties: Properties = new Properties()
    properties.load(in)
    in.close()
    properties.asScala.toMap
  }
}

