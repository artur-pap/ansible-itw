resolvers += Resolver.file("Local Override Repository", baseDirectory.value.getParentFile / "repo")(Resolver.ivyStylePatterns)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

//addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.1.4")

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")