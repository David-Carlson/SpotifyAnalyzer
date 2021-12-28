package MusicObject

import ujson.Value

case class Artist(id: String, name: String, genres: Set[String], popularity: Int, followers: Int){
  override def equals(o: Any) = o match {
    case that: Artist => that.id.equalsIgnoreCase(this.id)
    case id: String => id.equalsIgnoreCase(this.id)
    case _ => false
  }
  override def hashCode = id.hashCode
}

object Artist {
  def toCSV(artist: Artist): String = {
    artist match {
      case Artist(id, name, genres, popularity, followers) =>
        s"$id|$name|$popularity|$followers"
    }
  }
  def getSchema(): String = "$id|$name|$popularity|$followers"

  def parseArtist(i: Value): Option[Artist] = {
    try {
      val id = i("id").strOpt
      val name = i("name").strOpt
      val genres = i("genres").arrOpt
      val popularity = i("popularity").numOpt
      val followers = i("followers")("total").numOpt
      val all = List(id, name, genres, popularity, followers)
      if (all.exists(_.isEmpty)) {
        println("Artist fields not obtained: ")
        println(all.map(_.getOrElse("%")).mkString(" | "))
        return None
      }

      Some(Artist(id.get, name.get.replace('|', ':'), genres.get.map(_.str).toSet, popularity.get.toInt, followers.get.toInt))
    } catch {
      case _: RuntimeException => None
    }
  }
}
