package music

import java.io.{PrintWriter, File}
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.Ordering
import scala.util.Try

object MusicRunner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Music App").setMaster("local")
    val context = new SparkContext(conf)
    val rawData = context.textFile(new File(args(0)).toURI.getPath.toString)
    val records = rawData.map(line => Try(RecordOfPlay(line))).filter(_.isSuccess).map(_.get)
    println(s"successfully mapped approximately ${records.countApprox(10).getFinalValue()} records")
    val userSongCount = toUserSongCount(records)
    println("got user song count")
    writeSongCount(userSongCount)
    val top100Songs = toTop100Songs(records)
    println("got top 100 songs")
    writeMostPlayed(top100Songs)
    val longestSessions = to10LongestSessions(records)
    println("got longest sessions")
    writeLongestSessions(longestSessions)
    context.stop()
  }

  def writeSongCount(songCounts: Array[(String, Int)]): Future[Unit] = {
    Future {
      val file = new File("song counts.txt")
      print(file){p => songCounts.foreach{case (userId, count) => p.println(s"user id:$userId  songs played:$count")}}
    }
  }

  def writeMostPlayed(songs: Array[(Int, Song)]): Future[Unit] = {
    Future{
      val file = new File("most played.txt")
      print(file){p => songs.foreach{case (count, song) => p.println(s"artist: ${song.artist} name: ${song.name} plays:$count")}}

    }
  }

  def writeLongestSessions(sessions: Array[Session]): Future[Unit] = {
    Future {
      val file = new File("longest sessions.txt")
      print(file){p => sessions.foreach(session => {
        p.println(s"user: ${session.userId}, start: ${new DateTime(session.start).toString}, finish: ${new DateTime(session.finish.getOrElse(-1)).toString}")
        p.println(s"tracks: ${session.songs.mkString(", ")}")
      })}
    }
  }
  
  def toUserSongCount(records: RDD[RecordOfPlay]): Array[(String, Int)] = {
    records
      .groupBy(_.userId)
      .map{case(userId, userPlays) => (userId,userPlays.map(_.trackId).toSet.size)}
      .sortBy(_._2)
      .collect()
  }

  def toTop100Songs(records: RDD[RecordOfPlay]): Array[(Int, Song)] = {
    implicit val ordering = Ordering.by{playCountAndSong: (Int, Song) => playCountAndSong._1}
    records
      .map(Song(_))
      .groupBy(_.trackId)
      .map{case (_, songs) => (songs.size, songs.head)}
      .sortBy(_._1, false)
      .take(100)
  }

  def to10LongestSessions(records: RDD[RecordOfPlay]): Array[Session] = {
    val gap = 10 * 60 * 1000
    val isSameSession: (Session, RecordOfPlay) => Boolean = (session, record) => session.isPartOfSession(record, gap)
    implicit val recordOrdering = Ordering.by {record: RecordOfPlay => record.timestamp}
    records
      .groupBy(_.userId)
      .flatMap{case (id, records) => toSessions(records.toList, isSameSession)}
      .sortBy(_.duration, false)
      .take(10)
  }

  def toSessions(records: List[RecordOfPlay], isSameSession: (Session, RecordOfPlay) => Boolean)(implicit ordering: Ordering[RecordOfPlay]): List[Session] = {
    def toSessions(records: List[RecordOfPlay], sessions: List[Session]): List[Session] = {
        records match {
          case Nil => sessions
          case record::recordsTail => sessions match {
            case Nil => toSessions(recordsTail, List(Session(record)))
            case current::tail =>
              if(isSameSession(current, record)) toSessions(recordsTail, Session(record, current)::tail)
              else toSessions(recordsTail, Session(record)::sessions)
          }
        }
    }
    toSessions(records sorted, Nil)
  }

  def print(file: File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(file)
    try { op(p) } finally { p.close() }
  }

}
case class Session(userId: String, start: Long, finish: Option[Long], songs: List[String]) {

   def isPartOfSession(record: RecordOfPlay, gap: Long): Boolean = {
     finish match {
       case None => (record.timestamp - start) < gap
       case Some(time) => (record.timestamp - time) < gap
     }
   }

   def duration: Long = finish match {
     case None => 0
     case Some(time) => time - start
   }
}

object Session {

  def apply(recordOfPlay: RecordOfPlay): Session = Session(recordOfPlay userId, recordOfPlay timestamp, None, List(recordOfPlay trackName))

  def apply(record: RecordOfPlay, session: Session): Session = Session(session userId, session start, Some(record timestamp), record.trackName :: session.songs)
}

case class Song(artist: String, trackId: String, name: String)

object Song {

  def apply(record: RecordOfPlay): Song = Song(record.artist, record.trackId, record.trackName)
}
case class RecordOfPlay(userId: String, timestamp: Long, artistId: Option[String], artist: String, trackId: String, trackName: String)
object RecordOfPlay {

  def apply(line: String): RecordOfPlay = line.split("\\t") match {
    case Array(userId, timestamp, artistId, artist, trackId, trackName) =>
      val date = new DateTime(timestamp)
      this(userId, date.getMillis, Some(artistId), artist, trackId, trackName)
    case Array(userId, timestamp, artist, trackId, trackName) =>
      val date = new DateTime(timestamp)
      this(userId, date.getMillis, None, artist, trackId, trackName)
    case _ => throw new IllegalArgumentException("Cannot construct record of play")
  }
}
