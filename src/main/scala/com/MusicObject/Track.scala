package com.MusicObject

import com.MusicObject.Helper.{parseArtistField, parseNumOpt, quote, sanitize}
import ujson.Value

case class Track(id: String, name: String, artists: Set[String],
                 album_id: String, added_at: String,  duration: Int,
                 track_number: Int, explicit: Boolean, popularity: Int){
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
        val idS = quote(id)
        val nameS = quote(name)
        val album_idS = quote(album_id)
        val added_atS = quote(added_at)
        s"$idS|$nameS|$album_idS|$added_atS|$duration|$track_number|$explicit|$popularity"
    }
  }
  def getSchema(): String = "$idS|$nameS|$album_idS|$added_atS|$duration|$track_number|$explicit|$popularity"

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

      val managedArtists = Helper.parseArtistField(artists)
      Some(Track(sanitize(id), sanitize(name), managedArtists, sanitize(album_id), sanitize(added_at),
        parseNumOpt(duration), parseNumOpt(track_number), explicit.get, parseNumOpt(popularity)))
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
      val managedArtists = Helper.parseArtistField(artists)
      Some(Track(sanitize(id), sanitize(name), managedArtists, sanitize(album_id), sanitize(added_at),
        parseNumOpt(duration), parseNumOpt(track_number), explicit.get, parseNumOpt(popularity)))
    } catch {
      case ex: ujson.ParsingFailedException =>
        println("parsing exception", ex)
        None
      case ex: RuntimeException =>
        println("Track runtime Exception", ex)
        None
    }
  }



}

