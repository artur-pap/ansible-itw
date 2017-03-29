#!/usr/bin/env bash

SPARKMASTER=${SPARKMASTER:-local}
MESOSEXECUTORCORE=${MESOSEXECUTORCORE:-1}
SPARKIMAGE=${SPARKIMAGE:-spark_mesos:latest}
CURRENTIP=$(hostname -i)

sed -i 's;SPARKMASTER;'$SPARKMASTER';g' /spark/conf/spark-defaults.conf
sed -i 's;MESOSEXECUTORCORE;'$MESOSEXECUTORCORE';g' /spark/conf/spark-defaults.conf
sed -i 's;SPARKIMAGE;'$SPARKIMAGE';g' /spark/conf/spark-defaults.conf
sed -i 's;CURRENTIP;'$CURRENTIP';g' /spark/conf/spark-defaults.conf

export SPARKLOCALIP=${SPARKLOCALIP:-${CURRENTIP:-"127.0.0.1"}}
export SPARKPUBLICDNS=${SPARKPUBLICDNS:-${CURRENTIP:-"127.0.0.1"}}


if [ $ADDITIONALVOLUMES ];
then
echo "spark.mesos.executor.docker.volumes: $ADDITIONALVOLUMES" >> /spark/conf/spark-defaults.conf
fi

exec "$@"