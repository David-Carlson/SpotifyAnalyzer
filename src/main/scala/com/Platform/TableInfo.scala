package com.Platform

object TableInfo {
  val tableNames = List("genre", "album", "artist", "owner", "playlist", "track", "album_artists",
    "album_tracks", "artist_genres", "playlist_tracks", "track_artists", "user_password")

  val simpleTables = List("genre", "album", "artist", "owner", "track", "album_artists",
    "album_tracks", "artist_genres", "playlist_tracks", "track_artists", "user_password")

  val partitionTables = List("playlist")
  val partitionName = Map("playlist" -> "owner_id")
  val partitionIdx = Map("playlist" -> 3)

  // TODO: Store as ORC
  // Add partitions and clusters
  val tableSchemas = List(
    // Genre
    "CREATE TABLE IF NOT EXISTS genre (" +
    "id INT," +
    "name VARCHAR(150)) " +

    "COMMENT 'Gives the full name of a genre' " +
    "STORED AS orc",

  // Album
    "CREATE TABLE IF NOT EXISTS album (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "tracks INT," +
      "popularity INT)" +

      "COMMENT 'Broad details for an album' " +
      "STORED AS orc",

    // Artist
    "CREATE TABLE IF NOT EXISTS artist (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "popularity INT," +
      "followers INT)" +

      "COMMENT 'Broad details for an artist' " +
      "STORED AS orc",

    // Playlist
    "CREATE TABLE IF NOT EXISTS playlist (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "desc VARCHAR(300)," +
      "public BOOLEAN," +
      "followers INT," +
      "total_tracks INT)" +
      "PARTITIONED BY (owner_id VARCHAR(50)) " +
      "COMMENT 'Broad details for a playlist' " +
      "STORED AS orc",

    // Track
    "CREATE TABLE IF NOT EXISTS track (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "album_id VARCHAR(50)," +
      "added TIMESTAMP," +
      "duration_ms INT," +
      "track_number INT," +
      "explicit BOOLEAN," +
      "popularity INT)" +
      "clustered by (id) into 20 buckets " +

      "COMMENT 'Broad details for a track' " +
      "STORED AS orc",

    // Album Artists
    "CREATE TABLE IF NOT EXISTS album_artists (" +
      "id VARCHAR(50)," +
      "artist_id VARCHAR(50))" +

      "COMMENT 'Relates a single album to its Artists' " +
      "STORED AS orc",

    // Album Tracks
    "CREATE TABLE IF NOT EXISTS album_tracks (" +
      "id VARCHAR(50)," +
      "track_id VARCHAR(50))" +
      "clustered by (track_id) into 20 buckets " +

      "COMMENT 'Relates a single album to its Tracks' " +
      "STORED AS orc",

    // Artist Genres
    "CREATE TABLE IF NOT EXISTS artist_genres (" +
      "id VARCHAR(50)," +
      "genre_id VARCHAR(50))" +

      "COMMENT 'Relates a single Artist to their genres' " +
      "STORED AS orc",

    // Owner
    "CREATE TABLE IF NOT EXISTS owner (" +
      "id VARCHAR(50)," +
      "name VARCHAR(50))" +

      "COMMENT 'Lists an owners name given their ID' " +
      "STORED AS orc",

    // User Password
    "CREATE TABLE IF NOT EXISTS user_password (" +
      "id VARCHAR(50)," +
      "password VARCHAR(200), " +
      "is_admin BOOLEAN)" +

      "COMMENT 'Lists a user name, password, and admin status' " +
      "STORED AS orc",

    // Playlist Tracks
    "CREATE TABLE IF NOT EXISTS playlist_tracks (" +
      "id VARCHAR(50)," +
      "track_id VARCHAR(50))" +
      "clustered by (track_id) into 20 buckets " +

      "COMMENT 'Relates a single playlist to its tracks' " +
      "STORED AS orc",

    // Track Artists
    "CREATE TABLE IF NOT EXISTS track_artists (" +
      "id VARCHAR(50)," +
      "artist_id VARCHAR(50))" +

      "COMMENT 'Relates a single track to its artists' " +
      "STORED AS orc"
  )
}
