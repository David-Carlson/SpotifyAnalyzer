package Scraper

import MusicObject.{Album, Artist, Playlist, Track}
import scala.collection.mutable

object DataCollector {
  var genres: mutable.Set[String] = mutable.Set[String]()
  val albums: mutable.Set[Album] = mutable.Set[Album]()
  val artists: mutable.Set[Artist] = mutable.Set[Artist]()
  val playlists: mutable.Set[Playlist] = mutable.Set[Playlist]()
  val tracks: mutable.Set[Track] = mutable.Set[Track]()

  val albumIDSToAdd: mutable.Set[String] = mutable.Set[String]()
  val artistIDSToAdd: mutable.Set[String] = mutable.Set[String]()
  val playlistIDSForTracks: mutable.Set[String] = mutable.Set[String]()
  val trackIDSToAdd: mutable.Set[String] = mutable.Set[String]()

  val singleSongPlaylist = "2G8eBVtQL1Hvtz9mWIJHaR"
  val weddingPlaylist = "3XDM55F826gwInezuCa2KL"
  val top2019Playlist = "37i9dQZF1EtjEY9J2W3ro3"
  val derpPlaylist = "5ssrGR3JGkazd6Um7kRS4N"
  val katamariPlaylist = "0SoaXpELMHcEvhBE5IIbNz"
  val notDepressingPlaylist = "3995PqMcENZ9EOAEuIPTkp"

  def main(args: Array[String]): Unit = {

  }

  def filterPlaylists(plists: List[Playlist], users: List[String], playlistsPerUser: Int,
                      minPlaylistSize: Int, maxPlaylistSize: Int): List[Playlist] = {
    users.flatMap(u => plists
      .filter(p => p.owner_id == u && p.track_total >= minPlaylistSize && p.track_total <= maxPlaylistSize)
      .take(playlistsPerUser))
  }

  def startCollection(users: List[String], playlistsPerUser: Int = 12, minPlaylistSize: Int = 5, maxPlaylistSize: Int = 50): (Map[String, Int], mutable.Set[Album], mutable.Set[Artist], mutable.Set[Playlist], mutable.Set[Track]) = {
    println("Starting Collection\n")
    // Get normal genres to start genre collection

    println("Getting genre seeds\n")
    addGenres(SpotifyApi.getGenreSeeds().getOrElse(List.empty))
    if (genres.size == 0)
      throw new Exception("Collection failed, likely token error")

    println("Getting user playlists, then filtering them\n")
    val initialPlaylists = users.flatMap(SpotifyApi.getUserPlaylistIDS)
      .map(SpotifyApi.getPlaylist)
      .filter(_.isDefined)
      .map(_.get)
      .filter(p => users.contains(p.owner_id))
    addPlaylists(filterPlaylists(initialPlaylists, users, playlistsPerUser, minPlaylistSize, maxPlaylistSize))

//    println()
//    println(playlists.map(_.track_total).sum)
//    throw new Exception("remove")

    //    addPlaylists(SpotifyApi.getUserPlaylistIDS(username)
    //      .take(2)
    //      .map(SpotifyApi.getPlaylist(_))
    //      .filter(_.isDefined)
    //      .map(_.get)
    //      .filter(_.owner_id == username))

    //    addPlaylists(List(weddingPlaylist)
    //      .map(SpotifyApi.getPlaylist(_))
    //      .filter(_.isDefined)
    //      .map(_.get))

    println(s"Getting tracks from ${playlists.size} playlists\n")
    // For each playlist, get tracks, add tracks to playlist. Return all tracks for further use
    playlists.foreach(playlist => {
      val tracks = SpotifyApi.getPlaylistTracks(playlist.id)
      playlist.track_ids ++= tracks.map(_.id)
      addArtistIDS(tracks.flatMap(_.artists).toList)
      addAlbumIDS(tracks.map(_.album_id).filter(_.isDefined).map(_.get))
      addTracks(tracks)
      if (tracks.length == 0) {
        playlists -= playlist
      }
    })

    if (albumIDSToAdd.isEmpty) {
      println("Found 0 albums to retrieve")
    } else {
      println("Retrieving Albums... ")
    }
    // get albums, adding new tracks to id list.
    val newAlbums = albumIDSToAdd
      .grouped(20)
      .zipWithIndex
      .map{ case (grp: mutable.Set[String], idx: Int) => {
        println(s"Retrieving ${idx * 20}/${albumIDSToAdd.size} albums")
        grp
      }}
      .flatMap(SpotifyApi.getSeveralAlbums(_))
    addAlbums(newAlbums.toList)
    addArtistIDS((newAlbums.flatMap(_.artists).toList))

    val track_count = albums.map(_.tracks).sum
    println(s"\nGetting approximately ${track_count} tracks")
    val albumTracks = albumIDSToAdd
      .grouped(50)
      .zipWithIndex
      .map{ case (grp: mutable.Set[String], idx: Int) => {
        println(s"Retrieving tracks from Albums ${idx * 50}/${albumIDSToAdd.size}")
        grp
      }}
      .flatMap(group => group.flatMap(a => SpotifyApi.getAlbumTracks(a)))
      .toList
    addTracks(albumTracks)

    println(s"\nGetting ${artistIDSToAdd.size} artists")
    val newArtists = artistIDSToAdd.grouped(20)
      .zipWithIndex
      .map{ case (grp: mutable.Set[String], idx: Int) => {
        println(s"Retrieving Artists ${idx * 20}/${artistIDSToAdd.size}")
        grp
      }}
      .map(SpotifyApi.getSeveralArtists(_))
      .filter(_.isDefined)
      .flatMap(_.get)
      .toList

    addArtists(newArtists)
    addGenres(newArtists.flatMap(_.genres))

    printAllData()

    val genreMap = genres.zipWithIndex.map{ case (g, i) => (g, i+1)}.toMap
    if (genreMap.isEmpty || albums.isEmpty || artists.isEmpty || playlists.isEmpty || tracks.isEmpty)
      println("One of the data structures is empty!")
    (genreMap, albums, artists, playlists, tracks)
  }
  def printAllData(showData: Boolean = false): Unit = {
    println("\nWeb scraping complete!")
    println("----------------------")
    println(s"Playlists: ${playlists.size}")
    if (showData) {
      playlists.foreach(println)
    }
    println()
    println(s"Artists: ${artists.size}")
    if (showData) {
      artists.foreach(println)
    }
    println()
    println(s"Albums: ${albums.size}")
    if (showData) {
      albums.foreach(println)
    }
    println()
    println(s"Tracks: ${tracks.size}")
    if (showData) {
      tracks.foreach(println)
    }
    println()
    println(s"Genres: ${genres.size}")
    if (showData) {
      genres.foreach(println)
    }
    println()
  }

  def addGenres(newGenres: List[String]): Unit = {
    genres ++= newGenres
  }

  def addPlaylists(newPlaylists: List[Playlist]): Unit = {
//    newPlaylists.foreach(p => println(Playlist.toCSV(p)))
    playlists ++= newPlaylists
  }

  def addPlaylistIDS(newPlaylistIDS: List[String]): Unit = {
    playlistIDSForTracks ++= newPlaylistIDS.distinct.filter(id => !(playlists.exists(_.id == id)))
  }
  def addTracks(newTracks: List[Track]): Unit = {
    tracks ++= newTracks
  }

  def addTrackIDS(newTrackIDS: List[String]): Unit = {
    trackIDSToAdd ++= newTrackIDS.distinct.filter(id => !(tracks.exists(_.id == id)))
  }

  def addAlbums(newAlbums: List[Album]): Unit = {
    albums ++= newAlbums
  }

  def addAlbumIDS(newAlbumIDS: List[String]): Unit = {
    albumIDSToAdd ++= newAlbumIDS.distinct.filter(id => !(albums.exists(_.id == id)))
  }

  def addArtists(newArtists: List[Artist]): Unit = {
    artists ++= newArtists
  }

  def addArtistIDS(newArtistIDS: List[String]): Unit = {
    artistIDSToAdd ++= newArtistIDS.distinct.filter(id => !(artists.exists(_.id == id)))
  }

}
