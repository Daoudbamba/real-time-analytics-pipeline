#!/usr/bin/env bash
# Bakes the Kafka and Delta connectors into the image, so containers start
# without network access. Shared by the Spark and Airflow images, which must
# carry the same jars to run drivers and executors side by side.
set -euo pipefail

: "${SPARK_VERSION:?SPARK_VERSION is required}"
: "${DELTA_VERSION:?DELTA_VERSION is required}"
: "${GCS_CONNECTOR_VERSION:?GCS_CONNECTOR_VERSION is required}"
INSTALL_CLOUD_CONNECTORS="${INSTALL_CLOUD_CONNECTORS:-false}"

spark_home="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
spark_minor="${SPARK_VERSION%.*}"
packages="io.delta:delta-spark_${spark_minor}_2.13:${DELTA_VERSION}"
packages+=",org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}"

if [[ "${INSTALL_CLOUD_CONNECTORS}" == "true" ]]; then
  hadoop_jar="$(ls "${spark_home}"/jars/hadoop-client-api-*.jar)"
  hadoop_version="$(basename "${hadoop_jar}" .jar)"
  hadoop_version="${hadoop_version#hadoop-client-api-}"
  packages+=",org.apache.hadoop:hadoop-aws:${hadoop_version}"
  curl --fail --silent --show-error --location \
    --output "${spark_home}/jars/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar" \
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR_VERSION}/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar"
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT
echo 'pass' > "${work_dir}/noop.py"
"${spark_home}/bin/spark-submit" \
  --conf "spark.jars.ivy=${work_dir}/ivy" \
  --packages "${packages}" \
  "${work_dir}/noop.py"

# Ivy names jars <group>_<artifact>-<version>.jar. Spark already ships several
# of these transitive dependencies: keep its versions to avoid classpath clashes.
for jar in "${work_dir}"/ivy/jars/*.jar; do
  file_name="$(basename "${jar}")"
  target_name="${file_name#*_}"
  artifact="$(sed -E 's/-[0-9].*$//' <<< "${target_name}")"
  if compgen -G "${spark_home}/jars/${artifact}-[0-9]*" > /dev/null; then
    echo "Keeping the Spark version of ${artifact}"
  else
    cp "${jar}" "${spark_home}/jars/${target_name}"
  fi
done
