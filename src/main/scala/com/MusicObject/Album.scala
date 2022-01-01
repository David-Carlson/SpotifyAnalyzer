package com.MusicObject
import ujson.Value
import com.MusicObject.Helper.{parseNumOpt, quote, sanitize}

case class Album(id: String, name: String, artists: Set[String],
                 tracks: Int, popularity: Int, var track_ids: Set[String] = Set[String]()){
  override def equals(o: Any) = o match {
    case that: Album => that.id.equalsIgnoreCase(this.id)
    case _ => false
  }
  override def hashCode = id.hashCode
}

object Album {
  def toCSV(album: Album): String = {
    album match {
      case Album(id, name, artists, tracks, popularity, track_ids) =>
        val idstr = quote(id)
        val namestr = quote(name)
        s"$idstr|$namestr|$tracks|$popularity"
    }
  }
  def getSchema(): String = "$idstr|$namestr|$tracks|$popularity"

  def parseAlbum(i: Value): Option[Album] = {
    try {
      val id = i("id").strOpt
      val name = i("name").strOpt
      val artists = i("artists").arrOpt
      val tracks = i("total_tracks").numOpt
      val popularity = i("popularity").numOpt
      val all = List(id, name, tracks, popularity)
      if (all.exists(_.isEmpty)) {
        println("Album fields not obtained: ")
        println(all.map(_.getOrElse("%")).mkString(" | "))
        return None
      }
      val managedArtists = Helper.parseArtistField(artists)
      Some(Album(sanitize(id), sanitize(name), managedArtists, parseNumOpt(tracks), parseNumOpt(popularity)))
    } catch {
      case _: RuntimeException => None
    }
  }
}


