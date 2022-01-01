package com.MusicObject

import ujson.Value
import scala.collection.mutable.ArrayBuffer

object Helper {
  def quote(inputStr: Option[String]): String = {
    quote(inputStr.getOrElse(""))
  }

  def quote(inputStr: String): String = {
    "\"" + sanitize(inputStr) + "\""
  }

  def arrToSet(arrOpt: Option[ArrayBuffer[Value]]): Set[String] = {
    arrOpt.getOrElse(ArrayBuffer.empty).map(_.str).map(sanitize).toSet
  }

  def parseArtistField(artists: Option[ArrayBuffer[Value]]): Set[String] = {
    artists.getOrElse(List.empty)
      .map(_("id").strOpt)
      .filter(_.isDefined)
      .map(_.get)
      .map(sanitize)
      .toSet
  }

  def parseNumOpt(numOpt: Option[Double]): Int = {
    if (numOpt.isDefined)
      numOpt.get.toInt
    else
      -1
  }

  def sanitize(str: String) = str.trim.replace("\"", "'").replace('|', ':')

  def sanitize(str: Option[String]): String = {
    sanitize(str.getOrElse(""))
  }
}
