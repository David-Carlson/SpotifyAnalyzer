package com.Platform
import com.Crawler.DataWriter
import com.Platform.DB.usernameIsFree
import com.Platform.RowObjects.UserInfo

import scala.io.StdIn
import sys.exit


object Terminal {
  var logged_in_user: Option[UserInfo] = None
  def getUserID = (logged_in_user.get).id
  def isAdmin = (logged_in_user.get).is_admin

  def main(args: Array[String]): Unit = {
    mainMenu()
  }


  def mainMenu(): Unit = {
    while(true) {
      if (logged_in_user.isDefined) {

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
      println("3) Create an account")
      println("0) Quit the program")
      IO.readInt(0, 3) match {
        case 1 => generalAnalysis()
        case 2 => loginOrLogout()
        case 3 => createUserAccount()
        case 0 => exit
        case _ => println("Didn't understand command")
      }
    }
  }

  def generalAnalysis(): Unit = {

  }

  def createUserAccount(): Unit = {
    val (username, password) = IO.readUsernameAndPassword()
    if (usernameIsFree(username)) {
      println("Creating user...")
    }
  }

  def adminMode(): Unit = {
    while(true) {
      println("Admin Portal")
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
        case 1 =>
          adminLogin()
          return
        case 2 => return
        case 0 => exit
      }
    }
  }

  def crawlNewPlaylist(): Unit = {
    println("Music Archive Creator: ")
    val crawlerName = IO.getCrawlerName()
    val users = IO.getUsernames()


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

}
