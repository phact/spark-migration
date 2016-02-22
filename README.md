# spark-migration
cluster migration with spark

Make sure to change the path to your dse.jar:

https://github.com/phact/spark-migration/blob/master/pom.xml#L43

You will need to tweak your case class to match the table you want to migrate. I'm working on a way to make this dynamic.

https://github.com/phact/spark-migration/blob/master/src/main/java/com/phact/Migrate.java#L32-L131

You can build with `mvn package` and run with `dse spark-submit target`

dse spark-submit --class com.phact.App target/sparkjJavaApi-1.0-SNAPSHOT.jar sourceNodeIP destinationNodeIP Keyspace.Table
