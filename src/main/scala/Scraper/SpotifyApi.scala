package Scraper

import MusicObject.Album.parseAlbum
import MusicObject.Artist.parseArtist
import MusicObject.Playlist.{parsePlaylist, parsePlaylistID}
import MusicObject.Track.{parseAlbumTrack, parsePlaylistTrack}
import MusicObject.{Album, Artist, Playlist, Track}
import ujson.Value



object SpotifyApi {
  // https://developer.spotify.com/console/get-playlists/?user_id=doctorsalt&limit=&offset=
  val baseUrl = "https://api.spotify.com/v1"
  val bearer = sys.env("spotifytoken")

  var requestCount = 0

  def main(args: Array[String]): Unit = {

  }

  def getHeaders(): Map[String, String] = {
    Map(
      "Accept" -> "application/json",
      "Content-Type" -> "application/json",
      "Authorization" -> ("Bearer " + bearer)
    )
  }

  private def getJsonDataWithLink(link: String): Value.Value = {
    requestCount += 1

    ujson.read(requests.get(
      link,
      headers = getHeaders()
    ).text)
  }
  def printException(ex: requests.RequestFailedException, title: String): Unit = {
    try {
      val res = ujson.read(ex.response)
      val status = res("error")("status")
      val msg = res("error")("message")
      println(title + "\n")
      println(s"Status: $status")
      println(s"Message: $msg")
    } catch {
      case ex: Throwable => println(title)
    }
  }

  def getGenreSeeds(): Option[List[String]] = {
    try {
      val link = baseUrl + "/recommendations/available-genre-seeds"
      val json = getJsonDataWithLink(link)
      Some(json("genres").arr.map(_.toString()).map(s => s.substring(1, s.length - 1)).toList)
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "GetGenreSeeds failed")
        None
      case ex => println(s"An unexpected error occured in getGenreSeeds: $ex")
        None
    }
  }

  def getUserPlaylistIDS(username: String, minSize: Int): List[String] = {
    try {
      var nextLink: Option[String] = Some(baseUrl + s"/users/$username/playlists?limit=50")
      Iterator
        .continually(nextLink.isDefined)
        .takeWhile(identity)
        .flatMap { _ =>
          val json = getJsonDataWithLink(nextLink.get)
          nextLink = json("next").strOpt
//          json("items").arr.map(_("id").strOpt)
          json("items").arr
            .map(i => parsePlaylistID(i, username, minSize))
        }
        .filter(_.isDefined)
        .map(_.get)
        .toList
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "getUserPlaylists failed")
        List.empty
      case ex => println(s"An unexpected error occurred in getUserplaylists: $ex")
        List.empty
    }
  }

  def getPlaylist(id: String): Option[Playlist] = {
    try {
      val link = baseUrl + s"/playlists/${id}"
      parsePlaylist(getJsonDataWithLink(link))
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "getPlaylist failed")
        None
      case ex => println(s"An unexpected error occurred in getPlaylist: $ex")
        None
    }
  }

  def getSeveralArtists(ids: Iterable[String]): Option[Set[Artist]] = {
    if (ids.size > 50)
      throw new Exception("More than 50 ids given to getSeveralArtists!")

    val idQuery = "?ids=" + ids.mkString("%2C")
    val link: String = baseUrl + "/artists" + idQuery
    val json = getJsonDataWithLink(link)
    val artists = getJsonDataWithLink(link)("artists")
      .arr
      .map(parseArtist(_))
      .toSet
    if (artists.forall(_.isDefined)) Some(artists.map(_.get)) else None
  }

  def getSeveralAlbums(ids: Iterable[String]): Set[Album] = {
    if (ids.size > 20)
      throw new Exception("More than 20 ids given to getSeveralAlbums!")

    try {
      val idQuery = "?ids=" + ids.mkString("%2C")
      val link: String = baseUrl + "/albums" + idQuery
      val json = getJsonDataWithLink(link)
      getJsonDataWithLink(link)("albums")
        .arr
        .map(parseAlbum(_))
        .filter(_.isDefined)
        .map(_.get)
        .toSet
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "getPlaylistTracks failed")
        Set.empty
      case ex => println(s"An unexpected error occurred in getPlaylistTracks: $ex")
        Set.empty
    }
  }



  def getPlaylistTracks(id: String): List[Track] = {
    try {
      var nextLink: Option[String] = Some(baseUrl + s"/playlists/$id/tracks?limit=50")

      Iterator
        .continually(nextLink.isDefined)
        .takeWhile(identity)
        .flatMap { _ =>
          var json = getJsonDataWithLink(nextLink.get)
          nextLink = json("next").strOpt
          json("items").arr.map(parsePlaylistTrack)
        }
        .filter(_.isDefined)
        .map(_.get)
        .toList
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "getPlaylistTracks failed")
        List.empty
      case ex => println(s"An unexpected error occurred in getPlaylistTracks: $ex")
        List.empty
    }

  }
  def getAlbumTracks(id: String): List[Track] = {
    try {
      var nextLink: Option[String] = Some(baseUrl + s"/albums/$id/tracks?limit=50")
      Iterator
        .continually(nextLink.isDefined)
        .takeWhile(identity)
        .flatMap { _ =>
          var json = getJsonDataWithLink(nextLink.get)
          nextLink = json("next").strOpt
          json("items").arr.map(parseAlbumTrack(_, id))
        }
        .filter(_.isDefined)
        .map(_.get)
        .toList
    } catch {
      case ex: requests.RequestFailedException => printException(ex, "getSeveralAlbums failed")
        List.empty
      case ex => println(s"An unexpected error occurred in getSeveralAlbums: $ex")
        List.empty
    }
  }
}
