"""Resolve hadoop-aws (+ AWS SDK) and the Postgres JDBC driver with Ivy at
image build time, then copy the jars into PySpark's jars/ directory."""

import glob
import os
import re
import shutil
import urllib.request

import pyspark
from pyspark.sql import SparkSession

POSTGRES_JDBC = "org.postgresql:postgresql:42.7.13"
IVY_DIR = "/tmp/ivy"

jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
hadoop_jar = next(iter(glob.glob(os.path.join(jars_dir, "hadoop-client-api-*.jar"))))
hadoop_version = re.search(r"hadoop-client-api-(.+)\.jar", hadoop_jar).group(1)
packages = f"org.apache.hadoop:hadoop-aws:{hadoop_version},{POSTGRES_JDBC}"
print(f"PySpark {pyspark.__version__} bundles Hadoop {hadoop_version}; resolving {packages}")

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars.packages", packages)
    .config("spark.jars.ivy", IVY_DIR)
    .getOrCreate()
)
spark.stop()

existing = set(os.listdir(jars_dir))
copied = []
for jar in glob.glob(os.path.join(IVY_DIR, "jars", "*.jar")):
    name = os.path.basename(jar)
    # avoid shadowing jars Spark already ships (e.g. hadoop-client-*)
    stem = re.sub(r"-\d[\w.\-]*\.jar$", "", name)
    if any(e.startswith(stem + "-") for e in existing):
        continue
    shutil.copy(jar, jars_dir)
    copied.append(name)

# spark-hadoop-cloud provides the PathOutputCommitProtocol needed for the S3A
# "magic" committer (safe, rename-free commits on S3/MinIO). Its other
# transitive deps (Azure/GCS connectors) are not needed, so fetch just the jar.
scala = next(iter(glob.glob(os.path.join(jars_dir, "spark-core_*.jar"))))
scala_version = re.search(r"spark-core_(\d+\.\d+)-", scala).group(1)
cloud_jar = f"spark-hadoop-cloud_{scala_version}-{pyspark.__version__}.jar"
url = (
    "https://repo1.maven.org/maven2/org/apache/spark/"
    f"spark-hadoop-cloud_{scala_version}/{pyspark.__version__}/{cloud_jar}"
)
urllib.request.urlretrieve(url, os.path.join(jars_dir, cloud_jar))
copied.append(cloud_jar)
print("Copied jars:", ", ".join(sorted(copied)))
