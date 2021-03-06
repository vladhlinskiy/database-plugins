# database-plugins

In order to run integration tests for these plugins you should have running database instance. It may be running on host
machine or in docker container. Tests create tables and sample data, so user configured via system property should have 
corresponding permissions. To run integration tests execute following command in shell:
```
mvn clean test \ 
-Dmysql.host=localhost -Dmysql.port=3306 -Dmysql.database=mydb -Dmysql.username=root -Dmysql.password=root \
-Dpostgresql.host=localhost -Dpostgresql.port=54032 -Dpostgresql.database=test -Dpostgresql.username=postgres \ 
-Dpostgresql.password=cdap \
-Doracle.host=localhost -Doracle.port=1521 -Doracle.username=ora -Doracle.password=cdap -Doracle.database=EE \
-Doracle.connectionType=SID \
-Ddb2.host=localhost -Ddb2.port=50000 -Ddb2.database=SAMPLE -Ddb2.username=DB2INST -Db2.password=DB2INST1-pwd \
-Dnetezza.host=localhost -Dnetezza.port=5480 -Dnetezza.database=test -Dnetezza.username=admin \
-Dnetezza.password=password \
-DauroraMysql.clusterEndpoint=cdap-cluster.xyz.eu-central-1.rds.amazonaws.com -DauroraMysql.port=3306 \
-DauroraMysql.database=cdapdb -DauroraMysql.username=cdap -DauroraMysql.password=cdap \
-DauroraPostgresql.clusterEndpoint=pginstance.cxywmbgwp60k.eu-central-1.rds.amazonaws.com -DauroraPostgresql.port=5432 \
-DauroraPostgresql.database=cdappg -DauroraPostgresql.username=cdap -DauroraPostgresql.password=cdap
-DjdbcDriversJars="/jdbc/drivers/jars/some.jar, "
```