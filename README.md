# Spotify Analyzer
This project is built with two main parts: 
- A Spotify API scraper that gets all my user playlists, all songs inside, albums/artists linked by those songs, and finally the other tracks on Albums I have some songs for.
- Analysis using spark to find general trends among all users/playlists, as well as contrasting statistics. This includes a user lacking genre types in their playlists, which allows me so suggest songs from other users.

The API Scraper will build a local data structure to represent all relevant information with an output similar to the following:
```
Starting Collection

Getting genre seeds

Getting playlists from 2 users
There are 199 playlists before filtering
Retrieving 0/199 playlists
Retrieving 10/199 playlists
Retrieving 20/199 playlists
Retrieving 30/199 playlists
Retrieving 40/199 playlists
Retrieving 50/199 playlists
Retrieving 60/199 playlists
Retrieving 70/199 playlists
Retrieving 80/199 playlists
Retrieving 90/199 playlists
Retrieving 100/199 playlists
Retrieving 110/199 playlists
Retrieving 120/199 playlists
Retrieving 130/199 playlists
Retrieving 140/199 playlists
Retrieving 150/199 playlists
Retrieving 160/199 playlists
Retrieving 170/199 playlists
Retrieving 180/199 playlists
Retrieving 190/199 playlists

Getting tracks from 12 playlists...

Retrieving Albums... 
Retrieving 0/176 albums
Retrieving 20/176 albums
Retrieving 40/176 albums
Retrieving 60/176 albums
Retrieving 80/176 albums
Retrieving 100/176 albums
Retrieving 120/176 albums
Retrieving 140/176 albums
Retrieving 160/176 albums

Getting approximately 531 tracks
Retrieving tracks from Albums 0/176
Retrieving tracks from Albums 50/176
Retrieving tracks from Albums 100/176
Retrieving tracks from Albums 150/176

Getting 187 artists
Retrieving Artists 0/187
Retrieving Artists 20/187
Retrieving Artists 40/187
Retrieving Artists 60/187
Retrieving Artists 80/187
Retrieving Artists 100/187
Retrieving Artists 120/187
Retrieving Artists 140/187
Retrieving Artists 160/187
Retrieving Artists 180/187

Web scraping complete!
----------------------
Playlists: 12

Artists: 187

Albums: 176

Tracks: 2024

Genres: 372

Collection took 116.3168971 seconds
Writing music data for doctorsalt, 1249049206
Writing to file took 0.3326463 seconds
```
## Technologies
- Scala
- Requests for Scala
- Hive
- Spark
- SBT

## Setup
### Directories
First off, set up a working directory to allow my program to put data into. I use C:\\input

Setup winutils in C:\\winutils
### Environment Vars
- username : Your Spotify id 
- spotifyToken : A Spotify token granting permission for reading user playlists



