package com.intel.streaming_benchmark.common

object QueryConfig {
  val queryScene: Map[String, String] = Map(
    "q1.sql" -> "Shopping_record",
    "q2.sql" -> "Real_time_Advertising",
    "q3.sql" -> "Real_time_Advertising",
    "q4.sql" -> "Real_time_Advertising",
    "q5.sql" -> "User_visit_session_record",
    "q6.sql" -> "User_visit_session_record",
    "q7.sql" -> "User_visit_session_record",
    "q8.sql" -> "User_visit_session_record",
    "q9.sql" -> "Real_time_Advertising",
    "q10.sql" -> "User_visit_session_record",
    "q11.sql" -> "User_visit_session_record",
    "q12.sql" -> "User_visit_session_record"
  )

  val queryTables: Map[String, String] = Map(
    "q1.sql" -> "shopping",
    "q2.sql" -> "click",
    "q3.sql" -> "imp",
    "q4.sql" -> "dau,click",
    "q5.sql" -> "userVisit",
    "q6.sql" -> "userVisit",
    "q7.sql" -> "userVisit",
    "q8.sql" -> "userVisit",
    "q9.sql" -> "dau,click",
    "q10.sql" -> "userVisit",
    "q11.sql" -> "userVisit",
    "q12.sql" -> "userVisit"
  )

  def getScene(query: String): String ={
    if (queryScene.contains(query)) {
      queryScene(query)
    } else {
      throw new IllegalArgumentException(s"$query does not exist!")
    }
  }

  def getTables(query: String): String ={
    if (queryTables.contains(query)) {
      queryTables(query)
    } else {
      throw new IllegalArgumentException(s"$query does not exist!")
    }
  }
}
