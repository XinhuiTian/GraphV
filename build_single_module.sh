build/mvn package -DskipTests -pl graphx
cp graphx/target/spark-graphx_2.11-2.1.0-SNAPSHOT.jar ~/.m2/repository/org/apache/spark/spark-graphx_2.11/2.1.0-SNAPSHOT/spark-graphx_2.11-2.1.0-SNAPSHOT.jar
build/mvn package -DskipTests -pl examples
