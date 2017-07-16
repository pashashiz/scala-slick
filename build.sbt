name := "slick"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.2.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.0",
  "com.h2database" % "h2" % "1.4.192",
  "org.scalatest" % "scalatest_2.12" % "3.0.1" % "test"
)

