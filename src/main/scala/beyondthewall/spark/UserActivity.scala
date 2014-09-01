package beyondthewall.spark

import scala.collection.mutable.HashMap
import scala.runtime.ScalaRunTime._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class UserDetails(language : String, age : Int, city : String)
case class UserProfile(name: String, userId : Int, lastName : String, sex : String, userDetails : UserDetails)
case class UserActivity(userId : Int, activityType : String, sessionTime : Int, otherId : Int )
case class UserPreference(language : String, age : Int, city : String)

object UserActivity {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val marathi25Pune = new UserDetails("MARATHI", 25, "Pune")
    val marathi28Pune = new UserDetails("MARATHI", 28, "Pune")
    val marathi25Mumbai = new UserDetails("MARATHI", 25, "Mumbai")
    val hindi25Pune = new UserDetails("HINDI", 25, "Pune")
    val hindi25Mumbai = new UserDetails("HINDI", 25, "Mumbai")
    val hindi25Delhi = new UserDetails("HINDI", 25, "Delhi")

    val array = Seq(
      new UserProfile("Tushar", 1, "Khairnar", "M", marathi28Pune),
      new UserProfile("Avinash", 2, "Dongre", "M", marathi28Pune),
      new UserProfile("Rishi", 3, "Mishra", "M", hindi25Mumbai),
      new UserProfile("Ajay", 4, "Pande", "M", marathi28Pune),
      new UserProfile("Amogh", 5, "Shetkar", "M", marathi25Pune),
      new UserProfile("Suyog", 6, "Bhokare", "M", marathi25Mumbai),

      new UserProfile("Girl1", 7, "Sirname1", "F", marathi25Pune),
      new UserProfile("Girl2", 8, "Sirname2", "F", marathi28Pune),
      new UserProfile("Girl3", 9, "Sirname3", "F", marathi25Mumbai),
      new UserProfile("Girl4", 10, "Sirname4", "F", hindi25Pune),
      new UserProfile("Girl5", 11, "Sirname5", "F", hindi25Mumbai),
      new UserProfile("Girl6", 12, "Sirname6", "F", hindi25Delhi))

    val activityArray = Seq(
      //active users
      new UserActivity(1, "SESSION", 25, 0),      
      new UserActivity(1, "SESSION", 25, 0), 
      new UserActivity(1, "SESSION", 25, 0), 
      new UserActivity(1, "SESSION", 25, 0),
 
      new UserActivity(2, "SESSION", 50, 0),
      new UserActivity(2, "SESSION", 40, 0),
      new UserActivity(2, "SESSION", 90, 0),
      
      new UserActivity(3, "SESSION", 90, 0),
      new UserActivity(3, "SESSION", 10, 0),

      //other activity
      new UserActivity(1, "PROFILE_VIEW", 25, 7),
      new UserActivity(1, "PROFILE_VIEW", 25, 8),
      new UserActivity(1, "PROFILE_SHORTLIST", 25, 7),

      new UserActivity(2, "PROFILE_VIEW", 25, 10),
      new UserActivity(2, "PROFILE_VIEW", 25, 11),
      new UserActivity(2, "PROFILE_VIEW", 25, 7),
      new UserActivity(2, "PROFILE_VIEW", 25, 8),
      new UserActivity(2, "PROFILE_SHORTLIST", 25, 11),

      new UserActivity(3, "PROFILE_VIEW", 25, 7),
      new UserActivity(3, "PROFILE_VIEW", 25, 8),
      new UserActivity(3, "PROFILE_VIEW", 25, 7),
      new UserActivity(3, "PROFILE_VIEW", 25, 8),
      new UserActivity(3, "PROFILE_VIEW", 25, 8),
      new UserActivity(3, "PROFILE_VIEW", 25, 9))

    val userProfiles = sc.parallelize(array, 5)
    val userNames = userProfiles.map(u => (u.userId, u.name))
    val userDetails = userProfiles.map(u => (u.userId, u.userDetails))
    val userActivity = sc.parallelize(activityArray, 5)
    
    
    //session time count per user
    val userSessionTime = userActivity.filter(a => a.activityType.equals("SESSION"))
      .map(a => (a.userId, a.sessionTime))
      .reduceByKey(_ + _)
      .join(userNames)
      .map(t => (t._2._2, t._2._1/30))
      
    val sessionTimeCollect: Array[(String, Int)] = userSessionTime.collect()
    
    //cache profile activity rdd
    val profileViewActivity = userActivity.filter(a => a.activityType.equals("PROFILE_VIEW")).cache();
    
    //profile views per users
    val profileViews = profileViewActivity
      .map(a => (a.userId,a.otherId))
      .groupByKey()
      .join(userNames)
    val profileViewCount = profileViews.map(t => (t._2._2, t._2._1.size)) 
    val profileViewCountCollected = profileViewCount.collect()

    
    //this calculates what language profiles each user has visited usr - > map(language, visitCount)
    val otherProfilesViews =  profileViewActivity.map(a => (a.otherId, a.userId)).join(userDetails)
    val langPreferences = otherProfilesViews.
    	map( p => (p._2._1, p._2._2.language))
    	.join(userNames)
    	.map(k => (k._2._2, k._2._1))
    	.groupByKey()
    	.map(k => {
    		val array : Iterable[String] = k._2 
    		val map = new HashMap[String,Int]()
    		array.foreach(
    		    lang => {
    		      val count = map.getOrElse(lang,0).get()    		      
    		      map.put(lang,count+1)
    		    }
    		)
    		(k._1,map)
    	})//create language to view count map
    val langPreferencesCollect = langPreferences.collect();
    
    //this calculates what language profiles each user has visited usr - > map(language, visitCount)
    val cityPreferences = otherProfilesViews.
    	map( p => (p._2._1, p._2._2.city))
    	.join(userNames) // join with userName so that userName is included in projection 
    	.map(k => (k._2._2, k._2._1)) //select userName and city
    	.groupByKey() // group by username
    	.map(k => { //transform array to map
    		val array : Iterable[String] = k._2 
    		val map = new HashMap[String,Int]()
    		array.foreach(
    		    city => {
    		      val count = map.getOrElse(city,0).get()    		      
    		      map.put(city,count+1)
    		    }
    		)
    		(k._1,map)
    	})//create language to view count map
    val cityPreferencesCollect = cityPreferences.collect();
    
    println("Total time spent by the users today " +stringOf(sessionTimeCollect))
    println("Total profileView by the users today " +stringOf(profileViewCountCollected))    
    println("User language preferences" +stringOf(langPreferencesCollect))
    println("User city preferences" +stringOf(cityPreferencesCollect))
    sc.stop
  }

}

