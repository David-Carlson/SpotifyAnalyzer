package Scraper
import MusicObject.Track.toCSV
import MusicObject.{Album, Artist, Playlist, Track}

import scala.collection.mutable

object DataWriter {
  val verbose = false
  // Update this whenever the output format changes
  val version = "1.0.0"

  def main(args: Array[String]): Unit = {
    val usernames = sys.env("users").split("\\|").toList
    collectAndWriteAllData(usernames, "scrappy_joe")
  }
  // doctorsalt|1249049206|tchheou

  def collectAndWriteAllData(users: List[String], scraperName: String): Unit = {
    val playlistsPerUser = 6
    val minPlaylistSize = 5
    val maxPlaylistSize = 30

    val startTime = System.nanoTime()
    val (genreMap, albums, artists, playlists, tracks) = DataCollector.startCollection(users, playlistsPerUser, minPlaylistSize, maxPlaylistSize)
    if (albums.isEmpty || artists.isEmpty || playlists.isEmpty || tracks.isEmpty) {
      println("No data returning, aborting write procedure")
      return
    }
    val collTimeStamp = System.nanoTime()
    val collTime  = (collTimeStamp- startTime) / 1e9d

    println(s"Collection took $collTime seconds")
    println(s"Writing music data for ${users.mkString(", ")}")

    writeGenreMap(genreMap, scraperName)

    writeAlbums(albums, scraperName)
    writeAlbumToArtists(albums, scraperName)
    writeAlbumToTracks(tracks, scraperName)

    writeArtists(artists, scraperName)
    writeArtistsToGenres(artists, genreMap, scraperName)

    writePlaylists(playlists, scraperName)
    writePlaylistsToTracks(playlists, scraperName)
    writePlaylistOwnerToID(playlists, scraperName)

    writeTracks(tracks, scraperName)
    writeTracksToArtists(tracks, scraperName)

    val writeTime = (System.nanoTime() - collTimeStamp) / 1e9d
    println(s"Writing to file took $writeTime seconds")

    val scraperPath = os.pwd/"spotifydata"/scraperName/"scraper_data"
    val timestamp = java.time.LocalDateTime.now
    val schemas = List(Album.getSchema(), Artist.getSchema(), Playlist.getSchema(), Track.getSchema(), version)
    val versionHash = schemas.mkString.hashCode
    // TODO: Change to multiple usernames
    val manifestTxt = List(
      s"Music supplied by users: ${users.mkString(", ")}",
      s"Scraper Format: #$versionHash",
      s"Scraped by robot: $scraperName",
      s"Number of API Requests: ${SpotifyApi.requestCount}\n",
      s"Number of Playlists: ${playlists.size}",
      s"Number of Albums: ${albums.size}",
      s"Number of Artists: ${artists.size}",
      s"Number of Tracks: ${tracks.size}",
      s"Number of Genres: ${genreMap.size}\n",

      s"Allowed playlist sizes: Between $minPlaylistSize and $maxPlaylistSize",
      s"Playlists taken per user: $playlistsPerUser",
      s"Created ${timestamp}",
      s"Time to Scrape data: ${collTime} seconds",
      s"Time to Write data: ${writeTime} seconds\n"
    )
    os.write.over(scraperPath/"manifest.txt", manifestTxt.mkString("\n"), createFolders = true)
    os.write.over(scraperPath/"schemas.txt", getSchemas(), createFolders = true)
  }

  def writeGenreMap(genreMap: Map[String, Int], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"genre.txt"
      os.write.over(path, genreMap.map { case (g,i) => s"$i|$g"}.mkString("\n"),createFolders = true)
      if(verbose) println(s"Genres written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing genres")
      case ex: NullPointerException => println(ex, " Null path occured when writing genres")
    }
  }

  def writeArtists(artists: mutable.Set[Artist], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"artist.txt"
      os.write.over(path, artists.map(Artist.toCSV).mkString("\n"),createFolders = true)
      if(verbose) println(s"Artists written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing artists")
      case ex: NullPointerException => println(ex, " Null path occured when writing artists")
    }
  }
  def writeArtistsToGenres(artists: mutable.Set[Artist], genreMap: Map[String, Int], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"artist_genres.txt"
      os.write.over(path, artists.flatMap(a => a.genres.map(g => s"${a.id}|${genreMap(g)}")).mkString("\n"),createFolders = true)
      if(verbose) println(s"Artists/Genres written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Artists/Genres")
      case ex: NullPointerException => println(ex, " Null path occured when writing Artists/Genres")
    }
  }

  def writeAlbums(albums: mutable.Set[Album], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"album.txt"
      os.write.over(path, albums.map(Album.toCSV).mkString("\n"),createFolders = true)
      if(verbose) println(s"Albums written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing albums")
      case ex: NullPointerException => println(ex, " Null path occured when writing albums")
    }
  }

  def writeAlbumToArtists(albums: mutable.Set[Album], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"album_artists.txt"
      os.write.over(path, albums.flatMap(alb => alb.artists.map(art => s"${alb.id}|$art")).mkString("\n"),createFolders = true)
      if(verbose) println(s"Album/Artists written to $path")

    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Album/Artists")
      case ex: NullPointerException => println(ex, " Null path occured when writing Album/Artists")
    }
  }

  def writeAlbumToTracks(tracks: mutable.Set[Track], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"album_tracks.txt"
      val without = tracks.filter(_.album_id.isEmpty)

      val tracksWithAlbum = tracks.filter(_.album_id.isDefined).map(t => s"${t.album_id.get}|${t.id}")
      os.write.over(path, tracksWithAlbum.mkString("\n"),createFolders = true)
      if(verbose) println(s"Album/Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Album/Tracks")
      case ex: NullPointerException => println(ex, " Null path occured when writing Album/Tracks")
    }
  }

  def writePlaylists(playlists: mutable.Set[Playlist], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"playlist.txt"
      os.write.over(path, playlists.map(Playlist.toCSV).mkString("\n"),createFolders = true)
      if(verbose) println(s"Playlists written to $path")

    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Playlists")
      case ex: NullPointerException => println(ex, " Null path occured when writing Playlists")
    }
  }
  def writePlaylistOwnerToID(playlists: mutable.Set[Playlist], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"owner.txt"
      os.write.over(path, playlists.map(p => s"${p.owner_id}|${p.owner_name}").mkString("\n"),createFolders = true)
      if(verbose) println(s"Owners/Names written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Owners/Names")
      case ex: NullPointerException => println(ex, " Null path occured when writing Owners/Names")
    }
  }
  def writePlaylistsToTracks(playlists: mutable.Set[Playlist], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"playlist_tracks.txt"
      val allRows = playlists.map(p => p.track_ids.map(tr => s"${p.id}|${tr}").mkString("\n"))
      os.write.over(path, allRows.mkString("\n"),createFolders = true)
      if(verbose) println(s"Playlists/Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Playlists/Tracks")
      case ex: NullPointerException => println(ex, " Null path occured when writing Playlists/Tracks")
    }
  }
  def writeTracks(tracks: mutable.Set[Track], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"track.txt"
      os.write.over(path, tracks.map(Track.toCSV).mkString("\n"),createFolders = true)
      if(verbose) println(s"Tracks written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Tracks")
      case ex: NullPointerException => println(ex, " Null path occured when writing Tracks")
    }
  }
  def writeTracksToArtists(tracks: mutable.Set[Track], scraperName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/scraperName/"music_data"/"track_artists.txt"
      os.write.over(path, tracks.flatMap(t => t.artists.map(a => s"${t.id}|$a")).mkString("\n"),createFolders = true)
      if(verbose) println(s"Tracks/Artists written to $path")
    } catch {
      case ex: java.io.IOException => println(ex, " Occured when writing Tracks/Artists")
      case ex: NullPointerException => println(ex, " Null path occured when writing Tracks/Artists")
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
