name := "immuTable2"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "repo.codahale.com" at "http://repo.codahale.com"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "log4j" % "log4j" % "1.2.17"

// use this if you just want jawn's parser, and will implement your own facade
libraryDependencies += "org.spire-math" %% "jawn-parser" % "0.8.3"

// use this if you want jawn's parser and also jawn's ast
libraryDependencies += "org.spire-math" %% "jawn-ast" % "0.8.3"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.7"

javaOptions += "-Xms512m"

javaOptions += "-Xmx2g"

enablePlugins(JavaAppPackaging)

packageDescription in Debian := "immuTable"

maintainer in Debian := "Marcin Kossakowski <marcin.kossakowski@gmail.com>"

fork in run := true

connectInput in run := true // to wait for user input

mainClass in assembly := Some("immutable.Main")

mainClass in (Compile) := Some("immutable.Main")