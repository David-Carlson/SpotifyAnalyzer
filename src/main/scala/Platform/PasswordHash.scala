package Platform
import java.security.SecureRandom;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.SecretKeyFactory;
import com.roundeights.hasher.Implicits._
import scala.language.postfixOps

// https://github.com/dholbrook/scala-password-hash/blob/master/src/main/scala/scalapasswordhash/PasswordHash.scala
object PasswordHash {

  val PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA1"

  val SALT_BYTE_SIZE = 24
  val HASH_BYTE_SIZE = 24
  val PBKDF2_ITERATIONS = 1000

  val ITERATION_INDEX = 0
  val SALT_INDEX = 1
  val PBKDF2_INDEX = 2

  def main(args: Array[String]): Unit = {
    val password = "secrets"
    val hash = createSaltedHash(password)
    println(createSaltedHash(password))
    println(validatePassword(password, createSaltedHash(password)))
  }


  def createSaltedHash(password: String): String = {
    createSaltedHash(password.toCharArray())
  }

  private def createSaltedHash(password: Array[Char]): String = {
    val salt = nextRandomSalt
    val hash = pbkdf2(password, salt, PBKDF2_ITERATIONS, HASH_BYTE_SIZE)
    PBKDF2_ITERATIONS + ":" + toHex(salt) + ":" + toHex(hash)
  }

  def validatePassword(password: String, correctHash: String): Boolean = {
    validatePassword(password.toCharArray(), correctHash)
  }

  def validatePassword(password: Array[Char], correctHash: String): Boolean = {
    if (!correctHash.contains(":")) return false
    val params = correctHash.split(":")
    val iterations = Integer.parseInt(params(ITERATION_INDEX))
    val salt = fromHex(params(SALT_INDEX))
    val hash = fromHex(params(PBKDF2_INDEX))
    val testHash = pbkdf2(password, salt, iterations, hash.length)
    slowEquals(hash, testHash)
  }

  private def slowEquals(a: Array[Byte], b: Array[Byte]): Boolean = {
    val range = 0 until scala.math.min(a.length, b.length)
    val diff = range.foldLeft(a.length ^ b.length) {
      case (acc, i) => acc | a(i) ^ b(i)
    }
    diff == 0
  }

  private def nextRandomSalt(): Array[Byte] = {
    val random = new SecureRandom()
    val salt = Array.ofDim[Byte](SALT_BYTE_SIZE)
    random.nextBytes(salt)
    salt
  }

  private def pbkdf2(password: Array[Char], salt: Array[Byte], iterations: Int, bytes: Int): Array[Byte] = {
    val spec = new PBEKeySpec(password, salt, iterations, bytes * 8)
    val skf = SecretKeyFactory.getInstance(PBKDF2_ALGORITHM)
    skf.generateSecret(spec).getEncoded()
  }

  private def toHex(bytes: Array[Byte]): String =
    bytes.map("%02X" format _).mkString

  private def fromHex(hex: String): Array[Byte] =
    hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)

}