package com.Crawler

import com.MusicObject.Helper.quote
import com.MusicObject.{Album, Artist, Playlist, Track}
import com.Platform.PasswordHash
import com.Platform.PasswordHash.simpleHash

import scala.collection.mutable

object DataWriter {
  // Update this whenever the output format changes
  val version = "2.1.0"
  // TODO: Change to env var, use throughout app
  val verbose = false

  def main(args: Array[String]): Unit = {
    val usernames = "doctorsalt|1249049206|tchheou".split("\\|").toList

    collectAndWriteAllData(usernames, "small-test", 1, 6, 30)

//    val scraperName = "wall-e"
//    collectAndWriteAllData(usernames, scraperName, 6, 5, 30)

    //    val usernames = sys.env("users").split("\\|").toList
//    val usernames = "doctorsalt|1249049206".split("\\|").toList
//    val scraperName = "scrappy_joe"
//    collectAndWriteAllData(usernames, scraperName)
  }
  // doctorsalt|1249049206|tchheou

  def collectAndWriteAllData(users: List[String], crawlerName: String,
                             playlistsPerUser: Int, minPlaylistSize: Int, maxPlaylistSize: Int, token: String = sys.env("spotifytoken")): Unit = {
    val startTime = System.nanoTime()
    val (genreMap, albums, artists, playlists, tracks) = DataCollector.startCollection(users, playlistsPerUser, minPlaylistSize, maxPlaylistSize, token)
    if (albums.isEmpty || artists.isEmpty || playlists.isEmpty || tracks.isEmpty) {
      println("No data returning, aborting write procedure")
      return
    }
    val collTimeStamp = System.nanoTime()
    val collTime  = (collTimeStamp- startTime) / 1e9d

    println(s"Collection took $collTime seconds")
    println(s"Writing music data for ${users.mkString(", ")}")

    writeGenreMap(genreMap, crawlerName)

    writeAlbums(albums, crawlerName)
    writeAlbumToArtists(albums, crawlerName)
    writeAlbumToTracks(tracks, crawlerName)

    writeArtists(artists, crawlerName)
    writeArtistsToGenres(artists, genreMap, crawlerName)

    writePlaylists(playlists, crawlerName)
    writePlaylistsToTracks(playlists, crawlerName)
    writePlaylistOwnerToID(playlists, crawlerName)
    writeAccountPasswords(playlists, crawlerName)

    writeTracks(tracks, crawlerName)
    writeTracksToArtists(tracks, crawlerName)

    val writeTime = (System.nanoTime() - collTimeStamp) / 1e9d
    println(s"Writing to file took $writeTime seconds")

    val crawlerPath = os.pwd/"spotifydata"/crawlerName
    val timestamp = java.time.LocalDateTime.now
    val schemas = List(Album.getSchema(), Artist.getSchema(), Playlist.getSchema(), Track.getSchema(), version)
    val versionHash = simpleHash(schemas.mkString)
    // TODO: Change to multiple usernames
    val manifestTxt = List(
      s"Music supplied by users  : ${users.mkString(", ")}",
      s"Crawler Version          : ${version}-$versionHash",
      s"Crawled by robot         : $crawlerName",
      s"Number of API Requests   : ${SpotifyApi.requestCount}\n",

      s"Number of Playlists      : ${playlists.size}",
      s"Number of Albums         : ${albums.size}",
      s"Number of Artists        : ${artists.size}",
      s"Number of Tracks         : ${tracks.size}",
      s"Number of Genres         : ${genreMap.size}\n",

      s"Allowed playlist sizes   : Between $minPlaylistSize and $maxPlaylistSize",
      s"Playlists taken per user : $playlistsPerUser",
      s"Time to scrape data      : ${collTime} seconds",
      s"Time to write data       : ${writeTime} seconds",
      s"Created                  : ${timestamp}\n"
    )
    os.write.over(crawlerPath/"manifest.txt", manifestTxt.mkString("\n"), createFolders = true)
    os.write.over(crawlerPath/"schemas.txt", getSchemas(), createFolders = true)
  }

  def writeGenreMap(genreMap: Map[String, Int], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"genre.txt"
      os.write.over(path, genreMap.map { case (g,i) => s"$i|${quote(g)}"}.mkString("\n"), createFolders = true)
      if(verbose) println(s"Genres written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing genres")
      case ex: NullPointerException => println(ex, " Null path occurred when writing genres")
    }
  }

  def writeArtists(artists: mutable.Set[Artist], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"artist.txt"
      os.write.over(path, artists.map(Artist.toCSV).mkString("\n"), createFolders = true)
      if(verbose) println(s"Artists written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing artists")
      case ex: NullPointerException => println(ex, " Null path occurred when writing artists")
    }
  }
  def writeArtistsToGenres(artists: mutable.Set[Artist], genreMap: Map[String, Int], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"artist_genres.txt"
      val artistGenres = artists.flatMap(a => a.genres.map(g => s"${quote(a.id)}|${genreMap(g)}"))
      os.write.over(path, artistGenres.mkString("\n"), createFolders = true)
      if(verbose) println(s"Artists/Genres written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Artists/Genres")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Artists/Genres")
    }
  }

  def writeAlbums(albums: mutable.Set[Album], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"album.txt"
      os.write.over(path, albums.map(Album.toCSV).mkString("\n"), createFolders = true)
      if(verbose) println(s"Albums written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing albums")
      case ex: NullPointerException => println(ex, " Null path occurred when writing albums")
    }
  }

  def writeAlbumToArtists(albums: mutable.Set[Album], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"album_artists.txt"
      val albArt = albums.flatMap(alb => alb.artists.map(art => s"${quote(alb.id)}|${quote(art)}"))
      os.write.over(path, albArt.mkString("\n"), createFolders = true)
      if(verbose) println(s"Album/Artists written to $path")

    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Album/Artists")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Album/Artists")
    }
  }

  def writeAlbumToTracks(tracks: mutable.Set[Track], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"album_tracks.txt"
      val tracksWithAlbum = tracks.map(t => s"${quote(t.album_id)}|${quote(t.id)}")
      os.write.over(path, tracksWithAlbum.mkString("\n"), createFolders = true)
      if(verbose) println(s"Album/Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Album/Tracks")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Album/Tracks")
    }
  }

  def writePlaylists(playlists: mutable.Set[Playlist], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"playlist.txt"
      os.write.over(path, playlists.map(Playlist.toCSV).mkString("\n"), createFolders = true)
      if(verbose) println(s"Playlists written to $path")

    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Playlists")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Playlists")
    }
  }
  def writePlaylistOwnerToID(playlists: mutable.Set[Playlist], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"owner.txt"
      val owners = playlists.map(p => s"${quote(p.owner_id)}|${quote(p.owner_name)}")
      os.write.over(path, owners.mkString("\n"), createFolders = true)

      if(verbose) println(s"Owners/Names written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Owners/Names")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Owners/Names")
    }
  }
  def writeAccountPasswords(playlists: mutable.Set[Playlist], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"user_password.txt"
      val owner_passwords = playlists.map(p => (p.owner_id, false))
      val admin = ("admin", true)
      val all = (owner_passwords + admin).map(pair => s"${quote(pair._1)}|${quote(PasswordHash.createSaltedHash(pair._1))}|${pair._2}")
      os.write.over(path, all.mkString("\n"), createFolders = true)

      if(verbose) println(s"User/Password written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing User/Password")
      case ex: NullPointerException => println(ex, " Null path occurred when writing User/Password")
    }
  }
  def writePlaylistsToTracks(playlists: mutable.Set[Playlist], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"playlist_tracks.txt"
      val allRows = playlists.map(p => p.track_ids.map(tr => s"${quote(p.id)}|${quote(tr)}").mkString("\n"))
      os.write.over(path, allRows.mkString("\n"), createFolders = true)
      if(verbose) println(s"Playlists/Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Playlists/Tracks")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Playlists/Tracks")
    }
  }
  def writeTracks(tracks: mutable.Set[Track], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"track.txt"
      os.write.over(path, tracks.map(Track.toCSV).mkString("\n"), createFolders = true)
      if(verbose) println(s"Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Tracks")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Tracks")
    }
  }
  def writeTracksToArtists(tracks: mutable.Set[Track], crawlerName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/"track_artists.txt"
      val trackArtists = tracks.flatMap(t => t.artists.map(a => s"${quote(t.id)}|${quote(a)}"))
      os.write.over(path, trackArtists.mkString("\n"), createFolders = true)
      if(verbose) println(s"Tracks/Artists written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occurred when writing Tracks/Artists")
      case ex: NullPointerException => println(ex, " Null path occurred when writing Tracks/Artists")
    }
  }

  def getSchemas(): String = {
    """Table Schemas:
      |
      |----------------------------------------------
      |GENRE
      |
      |CREATE TABLE IF NOT EXISTS spotify.genre (
      |   id INT,
      |   name VARCHAR(150))
      |COMMENT 'Gives the full name of a genre'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/genre.txt' OVERWRITE INTO TABLE genre;
      |
      |----------------------------------------------
      |ALBUM
      |
      |CREATE TABLE IF NOT EXISTS spotify.album (
      |   id VARCHAR(50),
      |   name VARCHAR(200),
      |   tracks INT,
      |   popularity INT)
      |COMMENT 'Broad details for an album'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/album.txt' INTO TABLE album;
      |
      |----------------------------------------------
      |ARTIST
      |
      |CREATE TABLE IF NOT EXISTS spotify.artist (
      |   id VARCHAR(50),
      |   name VARCHAR(200),
      |   popularity INT,
      |   followers INT)
      |COMMENT 'Broad details for an artist'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/artist.txt' INTO TABLE artist;
      |
      |----------------------------------------------
      |PLAYLIST
      |
      |CREATE TABLE IF NOT EXISTS spotify.playlist (
      |   id VARCHAR(50),
      |   name VARCHAR(200),
      |   desc VARCHAR(300),
      |   owner_id VARCHAR(50),
      |   public BOOLEAN,
      |   followers INT,
      |   total_tracks INT)
      |COMMENT 'Broad details for a playlist'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/playlist.txt' INTO TABLE playlist;
      |
      |----------------------------------------------
      |TRACK TWO FIELDS MOSTLY NULL, added/popularity
      |
      |CREATE TABLE IF NOT EXISTS spotify.track (
      |   id VARCHAR(50),
      |   name VARCHAR(200),
      |   added TIMESTAMP,
      |   duration_ms INT,
      |   track_number INT,
      |   explicit BOOLEAN,
      |   popularity INT)
      |COMMENT 'Broad details for a track'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/track.txt' INTO TABLE track;
      |
      |----------------------------------------------
      |ALBUM_ARTISTS Check multiple artists per album
      |
      |CREATE TABLE IF NOT EXISTS spotify.album_artists (
      |   album_id VARCHAR(50),
      |   artist VARCHAR(50))
      |COMMENT 'Relates a single album to its Artists'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/album_artists.txt' INTO TABLE album_artists;
      |
      |----------------------------------------------
      |ALBUM_TRACKS
      |
      |CREATE TABLE IF NOT EXISTS spotify.album_tracks (
      |   album_id VARCHAR(50),
      |   track_id VARCHAR(50))
      |COMMENT 'Relates a single album to its Tracks'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/album_tracks.txt' INTO TABLE album_tracks;
      |
      |----------------------------------------------
      |ARTIST_GENRES
      |
      |CREATE TABLE IF NOT EXISTS spotify.artist_genres (
      |   artist_id VARCHAR(50),
      |   genre_id VARCHAR(50))
      |COMMENT 'Relates a single Artist to their genres'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/artist_genres.txt' INTO TABLE spotify.artist_genres;
      |
      |----------------------------------------------
      |OWNER
      |
      |CREATE TABLE IF NOT EXISTS spotify.owner (
      |   id VARCHAR(50),
      |   name VARCHAR(50))
      |COMMENT 'Lists an owners name given their ID'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/owner.txt' INTO TABLE spotify.owner;
      |----------------------------------------------
      |PLAYLIST_TRACKS
      |
      |CREATE TABLE IF NOT EXISTS spotify.playlist_tracks (
      |   playlist_id VARCHAR(50),
      |   track_id VARCHAR(50))
      |COMMENT 'Relates a single playlist to its tracks'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/playlist_tracks.txt' INTO TABLE spotify.playlist_tracks;
      |
      |----------------------------------------------
      |TRACK_ARTISTS CHECK
      |
      |CREATE TABLE IF NOT EXISTS spotify.track_artists (
      |   track_id VARCHAR(50),
      |   artist_id VARCHAR(50))
      |COMMENT 'Relates a single track to its artists'
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      |LINES TERMINATED BY '\n';
      |
      |LOAD DATA INPATH '/user/daweiwei/scrappy_joe/music_data/track_artists.txt' INTO TABLE spotify.track_artists;""".stripMargin
  }
}
