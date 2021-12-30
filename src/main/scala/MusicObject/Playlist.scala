package MusicObject

import ujson.Value
import Helper.{parseNumOpt, quote, sanitize}

case class Playlist(id: String, name: String, desc: String, owner_id: String,
                    owner_name: String, public: Boolean, followers: Int, track_total: Int, var track_ids: Set[String] = Set[String]()){
  override def equals(o: Any) = o match {
    case that: Playlist => that.id.equalsIgnoreCase(this.id)
    case _ => false
  }
  override def hashCode = id.hashCode
}

object Playlist {
  def toCSV(playlist: Playlist): String = {
    playlist match {
      case Playlist(id, name, desc, owner_id, owner_name, public, followers, total_tracks, _) =>
        val idS = quote(id)
        val nameS = quote(name)
        val descS = quote(desc)
        val owner_idS = quote(owner_id)
        s"$idS|$nameS|$descS|$owner_idS|$public|$followers|$total_tracks"
    }
  }
  def getSchema(): String = "$idS|$nameS|$descS|$owner_idS|$public|$followers|$total_tracks"

  def parsePlaylist(i: Value): Option[Playlist] = {
    try {
      val id = i("id").strOpt
      val name = i("name").strOpt
      val desc = i("description").strOpt
      val owner_id = i("owner")("id").strOpt
      val owner_name = i("owner")("display_name").strOpt
      val public = i("public").boolOpt
      val followers = i("followers")("total").numOpt
      val track_total = i("tracks")("total").numOpt

      val all = List(id, name, desc, owner_id, owner_name, public, followers, track_total)
      if (all.exists(_.isEmpty)) {
        println("Playlist fields not obtained: ")
        println(all.map(_.getOrElse("%")).mkString(" | "))
        return None
      }

      Some(Playlist(sanitize(id), sanitize(name), sanitize(desc), sanitize(owner_id), sanitize(owner_name),
        public.get, parseNumOpt(followers), parseNumOpt(track_total)))
    } catch {
      case ex: RuntimeException =>
        println(s"Runtime Exception parsing: $ex")
        None
    }
  }

  def parsePlaylistID(i: Value, username: String, minSize: Int): Option[String] = {
    try {
      val owner_id = i("owner")("id").strOpt.getOrElse("").trim
      val total_tracks = i("tracks")("total").numOpt.getOrElse(0.0)
      val id = i("id").strOpt.getOrElse("").trim
      if (id.nonEmpty && owner_id == username && total_tracks >= minSize) {
        Some(id)
      } else {
        None
      }
    } catch {
      case ex: RuntimeException =>
        println(s"Runtime Exception parsing: $ex")
        None
    }
  }
}
