package com.Platform.RowObjects

case class UserInfo(id: String, password: String, is_admin: Boolean)

object UserInfo{
  def toInsertString(userInfo: UserInfo): String = {
    s"('${userInfo.id}','${userInfo.password}', ${userInfo.is_admin})"
  }
}
