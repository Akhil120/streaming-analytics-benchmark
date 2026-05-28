FROM apache/fluss-quickstart-flink:1.20-0.9.1-incubating

# Copy Paimon lake JARs into the Flink lib dir so Union Read and $lake queries work.
# Exclude hadoop-apache-3.3.5-2.jar (44MB, conflicts with Flink's shaded Hadoop)
# paimon-s3 from the image has classes under paimon-plugin-s3/ prefix (wrong for lib);
# we bind-mount the Maven Central version (flat classpath structure) at runtime instead.
RUN cp /opt/flink/paimon/fluss-lake-paimon-0.9.1-incubating.jar /opt/flink/lib/ && \
    cp /opt/flink/paimon/paimon-flink-1.20-1.3.1.jar /opt/flink/lib/

# Activate the Flink S3 Hadoop plugin so FlinkFileIOLoader can access s3:// paths
# (used by Paimon SQL queries — e.g. transactions$lake — via the Flink filesystem layer).
RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.20.0.jar /opt/flink/plugins/flink-s3-fs-hadoop/

# PaimonCatalogProvider requires HdfsConfiguration (hadoop-hdfs-client) unconditionally.
# The bundled hadoop-apache-3.3.5-2.jar in /opt/flink/paimon/ includes Scala classes
# that break Flink's Pekko/RPC layer. Instead, fetch the two minimal pure-Java JARs:
#   hadoop-client-api  — provides org.apache.hadoop.conf.Configuration (base class)
#   hadoop-hdfs-client — provides org.apache.hadoop.hdfs.HdfsConfiguration
RUN wget -q -O /opt/flink/lib/hadoop-client-api-3.3.5.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.5/hadoop-client-api-3.3.5.jar && \
    wget -q -O /opt/flink/lib/hadoop-hdfs-client-3.3.5.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.5/hadoop-hdfs-client-3.3.5.jar

# Add Kafka SQL connector (not bundled in the Fluss quickstart image)
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# Bake SQL files into the image so they're available in all Flink containers
COPY sql/ /sql/

# Custom entrypoint: starts JobManager + SQL Gateway in the same container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
