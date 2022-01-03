package com.Platform
import scala.io.StdIn

object IO {
  def printBreak(): Unit = {
    println("---------------------------------")
  }
  def printShortBreak(): Unit = {
    println("---------------------")
  }
  def pressEnter(): Unit = {
//    println("Press Enter to continue: ")
//    StdIn.readLine()
  }
  def readInt(min: Int = 1, max: Int = 4): Int = {
    var num = -1
    do {
      println(s"Enter a number from $min to $max: ")
      try {
        num = StdIn.readInt()
      } catch {
        case _: NumberFormatException => println("That wasn't a number...")
        case _ => println("An error occurred...")
      }
    } while (num > max || num < min)
    num
  }

  def readUsernameAndPassword(): (String, String) = {
    var (user, password) = ("", "")
    do {
      println("Enter your user id and password in one line: ")
      val line = StdIn.readLine().split(" ")
      if (line.length != 2) {
        println("Format: 'Username password', e.g 'Hunter fDj3ml'")
      } else {
        user = line(0)
        password = line(1)
        if (user.length < 4 || password.length < 4) {
//          println(s"$user|$password")
          println("Username and password must be at least 4 characters")
        }
      }
    } while(user.length < 4 || password.length < 4)
    (user, password)
  }
  def readFirstAndLastname(): (String, String) = {
    var (first, last) = ("", "")
    do {
      println("Enter your first and last name in one line: ")
      val line = StdIn.readLine().split(" ")
      if (line.length != 2) {
        println("Format: 'First Last', e.g 'Keanu Reeves'")
      } else {
        println()
        first = line(0)
        last = line(1)
        if (first.length < 3 || last.length < 3)
          println("First and Last name must be at least 3 characters")
      }
    } while(first.length < 3 || last.length < 3)
    (first, last)
  }

  def getCrawlerName(): String = {
    var name = ""
    do {
      println("What should your archive be called?")
      name = StdIn.readLine()
      if (!name.matches("\\S+")) {
        println("This name will become a directory, no white-space characters allowed!")
        name = ""
      } else {
        if (os.exists(os.pwd / "spotifydata"/ name)) {
          println("This name is already taken")
          name = ""
        }
      }
    } while(name.isEmpty)
    name
  }

  def getUsernames(): List[String] = {
    var users: List[String] = List.empty
    do {
      println("Enter all your users separated by spaces")
      users = StdIn.readLine().split(" ").toList
      if (users.isEmpty)
        println("Didn't find any user names")
      else if (!users.forall(_.nonEmpty)) {
        println("One of the user names are empty")
        users = List.empty
      }
    } while(users.isEmpty)
    users
  }

  def getNewPassword(oldHash: String): String = {
    var newPass = ""
    do {
      println("Enter a new password: ")
      newPass = StdIn.readLine()
      if (PasswordHash.validatePassword(newPass, oldHash)) {
        println("Password matches the old hash, choose a new password")
        newPass = ""
      } else if (newPass.length < 4) {
        println("Password must be at least 4 characters")
        newPass = ""
      }
    } while(newPass.isEmpty)
    newPass
  }
}

