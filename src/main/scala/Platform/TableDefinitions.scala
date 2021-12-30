package Platform

object TableDefinitions {
  // TODO: Store as ORC
  // Add partitions and clusters
  val tableSchemas = List(
    // Genre
    "CREATE TABLE IF NOT EXISTS genre (" +
    "id INT," +
    "name VARCHAR(150)) " +

    "COMMENT 'Gives the full name of a genre' " +
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
    "LINES TERMINATED BY '\\n'",

  // Album
    "CREATE TABLE IF NOT EXISTS album (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "tracks INT," +
      "popularity INT)" +

      "COMMENT 'Broad details for an album' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Artist
    "CREATE TABLE IF NOT EXISTS artist (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "popularity INT," +
      "followers INT)" +

      "COMMENT 'Broad details for an artist' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Playlist
    // Partition by owner_id?
    "CREATE TABLE IF NOT EXISTS playlist (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "desc VARCHAR(300)," +
      "owner_id VARCHAR(50)," +
      "public BOOLEAN," +
      "followers INT," +
      "total_tracks INT)" +

      "COMMENT 'Broad details for a playlist' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Track
    "CREATE TABLE IF NOT EXISTS track (" +
      "id VARCHAR(50)," +
      "name VARCHAR(200)," +
      "added TIMESTAMP," +
      "duration_ms INT," +
      "track_number INT," +
      "explicit BOOLEAN," +
      "popularity INT)" +

      "COMMENT 'Broad details for a track' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Album Artists
    "CREATE TABLE IF NOT EXISTS album_artists (" +
      "id VARCHAR(50)," +
      "artist_id VARCHAR(50))" +

      "COMMENT 'Relates a single album to its Artists' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Album Tracks
    "CREATE TABLE IF NOT EXISTS album_tracks (" +
      "id VARCHAR(50)," +
      "track_id VARCHAR(50))" +

      "COMMENT 'Relates a single album to its Tracks' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Artist Genres
    "CREATE TABLE IF NOT EXISTS artist_genres (" +
      "id VARCHAR(50)," +
      "genre_id VARCHAR(50))" +

      "COMMENT 'Relates a single Artist to their genres' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Owner
    "CREATE TABLE IF NOT EXISTS owner (" +
      "id VARCHAR(50)," +
      "name VARCHAR(50))" +

      "COMMENT 'Lists an owners name given their ID' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Playlist Tracks
    "CREATE TABLE IF NOT EXISTS playlist_tracks (" +
      "id VARCHAR(50)," +
      "track_id VARCHAR(50))" +

      "COMMENT 'Relates a single playlist to its tracks' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'",

    // Track Artists
    "CREATE TABLE IF NOT EXISTS track_artists (" +
      "id VARCHAR(50)," +
      "artist_id VARCHAR(50))" +

      "COMMENT 'Relates a single track to its artists' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'"
  )
}
