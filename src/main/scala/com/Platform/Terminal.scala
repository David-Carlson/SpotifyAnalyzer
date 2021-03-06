package com.Platform
import com.Crawler.DataWriter
import com.Platform.Analysis._
import com.Platform.DB.{createUser, getSparkSession, setupDatabase, usernameIsFree}
import com.Platform.RowObjects.UserInfo

import scala.io.StdIn
import sys.exit


object Terminal {
  var logged_in_user: Option[UserInfo] = None
  def getUserID = (logged_in_user.get).id
  def getHash = (logged_in_user.get).password
  def isAdmin = (logged_in_user.get).is_admin

  def main(args: Array[String]): Unit = {
    if (!DB.allTablesExist())
      loadDatabase()
    mainMenu()
  }


  def mainMenu(): Unit = {
    getSparkSession()
    while(true) {
      if (logged_in_user.isDefined) {
        if (isAdmin)
          runAdminMenu()
        else
          runUserMenu()
      } else {
        runDefaultMenu()
      }
    }
  }
  def runDefaultMenu(): Unit = {
    while(true) {
      println(s"Spotify Analyzer - ${DataWriter.version}")
      println()
      println("1) Find general patterns in the music archive")
      println("2) Login to your account")
      println("0) Quit the program")
      IO.readInt(0, 2) match {
        case 1 => runGeneralAnalysis()
        case 2 => loginOrLogout(); return
        case 3 => createUserAccount()
        case 0 => exit
        case _ => println("Didn't understand command")
      }
    }
  }

  def runUserMenu(): Unit = {
    while(true) {
      println(s"Spotify Analyzer - User Portal - ${DataWriter.version}")
      println()
      println("1) Find personal patterns in the music archive")
      println("2) Change password")
      println("3) Logout")
      println("0) Quit the program")
      IO.readInt(0, 3) match {
        case 1 => runSpecificAnalysis()
        case 2 => changePassword()
        case 3 => loginOrLogout(); return
        case 0 => exit
        case _ => println("Didn't understand command")
      }
    }
  }

  def runAdminMenu(): Unit = {
    while(true) {
      println(s"Admin Portal - ${DataWriter.version}")
      IO.printShortBreak()
      println("1) Crawl more playlists")
      println("2) Load data into database")
      println("3) Create a new admin")
      println("4) Change your password")
      println("5) Logout")
      println("0) Quit program")
      println()
      val ans = IO.readInt(0, 5)
      ans match {
        case 1 => crawlNewPlaylist()
        case 2 => loadDatabase()
        case 4 => changePassword()
        case 5 => loginOrLogout(); return
        case 0 => exit
      }
    }
  }

  def runGeneralAnalysis(): Unit = {
    while(true) {
      println(s"General Analysis")
      println()
      println("1) List Archive information")
      println("2) Song length by album")
      println("3) Track popularity by user")
      println("4) Track popularity by playlist")
      println("5) Profanity usage by user")
      println("6) Print simple schemas")
      println("7) Return to previous menu")
      println("0) Quit the program")
      IO.readInt(0, 7) match {
        case 1 => getArchiveStats()
        case 2 => averageAlbumTrackLength();
        case 3 => getAvgTrackPopularityByUser()
        case 4 => getAvgTrackPopularityByPlaylist()
        case 5 => profanityByUser()
        case 6 => printSimpleSchema()
        case 7 => return
        case 0 => exit
        case _ => println("Didn't understand command")
      }
    }
  }

  def runSpecificAnalysis(): Unit = {
    while(true) {
      println(s"User Analysis")
      println()
      println("1) List Archive information")
      println("2) Get my unique genres")
      println("3) Find genres I'm lacking")
      println("4) Suggest tracks to expand my musical vocabulary")
      println("5) List my most popular playlists")
      println("6) Return to previous menu")
      println("0) Quit the program")
      IO.readInt(0, 6) match {
        case 1 => getArchiveStats()
        case 2 => getUserGenres(getUserID);
        case 3 => getMissingUserGenres(getUserID)
        case 4 => getTrackSuggestions(getUserID)
        case 5 => getAvgTrackPopularityByPlaylist(getUserID)
        case 6 => return
        case 0 => exit
        case _ => println("Didn't understand command")
      }
    }
  }

  def createUserAccount(): Unit = {
    val (username, password) = IO.readUsernameAndPassword()
    if (usernameIsFree(username)) {
      println("Creating user...")
      createUser(username, password)
    }
  }

  def crawlNewPlaylist(): Unit = {
    println("Music Archive Creator: ")
    val crawlerName = IO.getCrawlerName()
    val users = IO.getUsernames()
    println("Enter how many playlists will be stored per user (1-20 playlists)")
    val playlists = IO.readInt(1, 20)
    println("What's the smallest playlist size to store? (1-100 songs in a playlist)")
    val minSize = IO.readInt(1, 20)
    val atLeast = math.max(minSize, 5)
    println(s"What's the largest playlist size to store? ($atLeast-100 songs in a playlist)")
    val maxSize = IO.readInt(5, 100)
    println("What's your AUTH token?")
    val token = StdIn.readLine()
    DataWriter.collectAndWriteAllData(users, crawlerName, playlists, minSize, maxSize, token)
  }

  def loadDatabase(): Unit = {
    println("Here are the available achives to load: ")
    val choices = os.list(os.pwd / "spotifydata").filter(os.isDir(_)).map(_.baseName)
    choices.foreach(println)
    var choice = ""
    do {
      println("Enter the archive name to load")
      choice = StdIn.readLine()
    } while(!choices.contains(choice))

    println(s"Loading $choice...")
    setupDatabase(choice)
    println("Done")
  }

  def printBestOfN(n: Int): Unit = {
    //    println(s"Best scores for $n questions: ")
    //    println("User    Score   Ratio")
    //    IO.printShortBreak()
    //    getBestOfN(n).foreach(res => {
    //      val percent = res._3.toFloat/(res._3 + res._4)
    //      println(f"${res._1}%8s    ${res._2}%4d    $percent%.2f%%")
    //    })
    //    println()
  }

  def printUserBestOfN(n: Int, id:Int): Unit = {
    //    val (_, score, correct, incorrect) = getBestOfNByUser(id, n).get
    //    val best = s"Best over $n:"
    //    val scoreStr: String = f"$score/$n"
    //    println(f"$best%14s  $scoreStr%6s")
    //    println()
  }

  def loginOrLogout(): Unit = {
    logged_in_user match {
      case Some(user) =>
        println(s"Bye bye, ${user.id}!")
        logged_in_user = None
      case None => logInUser()
    }
  }
  def logInUser(): Unit = {
    do {
      val (user, password) = IO.readUsernameAndPassword()
      logged_in_user = DB.validateLogin(user, password)
    } while (logged_in_user.isEmpty)
    println("Logged in")
    IO.pressEnter()
  }
  def changePassword(): Unit = {
    val newPassword = IO.getNewPassword(getHash)
    logged_in_user = DB.updatePassword(logged_in_user.get, newPassword)
    if (logged_in_user.isDefined)
      println("Password change successful")

  }

}
