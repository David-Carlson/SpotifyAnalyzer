package MusicObject

import ujson.Value

import scala.collection.mutable.ArrayBuffer

case class Track(id: String, name: String, artists: Set[String],
                 album_id: Option[String], added_at: Option[String],  duration: Int,
                 track_number: Int, explicit: Boolean, popularity: Option[Int]){
  override def equals(o: Any) = o match {
    case that: Track => that.id.equalsIgnoreCase(this.id)
    case _ => false
  }
  override def hashCode = id.hashCode
}

object Track {
  def toCSV(track: Track): String = {
    track match {
      case Track(id, name, artists, album_id, added_at, duration, track_number, explicit, popularity) =>
        val added = added_at.getOrElse("")
        val pop = popularity.getOrElse("")

        s"$id|$name|$added|$duration|$track_number|$explicit|$pop"
    }
  }
  def getSchema(): String = "$id|$name|$added|$duration|$track_number|$explicit|$pop"

  def parsePlaylistTrack(i: Value): Option[Track] = {
    try {
      val id = i("track")("id").strOpt
      val name = i("track")("name").strOpt
      val artists = i("track")("artists").arrOpt
      val album_id = i("track")("album")("id").strOpt
      val added_at = i("added_at").strOpt
      val duration = i("track")("duration_ms").numOpt
      val track_number = i("track")("track_number").numOpt
      val explicit = i("track")("explicit").boolOpt
      val popularity = i("track")("popularity").numOpt

      val all = List(id, name, added_at, duration, track_number, explicit, popularity)
      if (all.exists(_.isEmpty)) {
        //        println("Track fields from playlist not obtained: ")
        //        println(all.map(_.getOrElse("%")).mkString(" | "))
        return None
      }

      val managedArtists = parseArtistField(artists)
      Some(Track(id.get, name.get, managedArtists, album_id, added_at, duration.get.toInt, track_number.get.toInt, explicit.get, Some(popularity.get.toInt)))
    } catch {
      case ex: ujson.ParsingFailedException =>
        println("parsing exception", ex)
        None
      case ex: RuntimeException =>
        println("Track runtime Exception", ex)
        None
    }
  }
  def parseAlbumTrack(i: Value, parent_album_id: String): Option[Track] = {
    try {
      val id = i("id").strOpt
      val name = i("name").strOpt
      val artists = i("artists").arrOpt
      val album_id = Some(parent_album_id)
      val added_at = None
      val duration = i("duration_ms").numOpt
      val track_number = i("track_number").numOpt
      val explicit = i("explicit").boolOpt
      val popularity = None
      val all = List(id, name, artists, album_id, duration, track_number, explicit)
      if (all.exists(_.isEmpty)) {
        println("Track fields from album not obtained: ")
        println(all.map(_.getOrElse("%")).mkString(" | "))
        return None
      }
      val managedArtists = parseArtistField(artists)
      Some(Track(id.get, name.get, managedArtists, album_id, added_at, duration.get.toInt, track_number.get.toInt, explicit.get, popularity))
    } catch {
      case ex: ujson.ParsingFailedException =>
        println("parsing exception", ex)
        None
      case ex: RuntimeException =>
        println("Track runtime Exception", ex)
        None
    }
  }
  def parseArtistField(artists: Option[ArrayBuffer[Value]]): Set[String] = {
    artists.getOrElse(List.empty).map(_("id").strOpt).filter(_.isDefined).map(_.get).toSet
  }
}

