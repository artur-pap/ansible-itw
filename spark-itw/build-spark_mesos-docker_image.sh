#!/usr/bin/env bash
#docker build -t docker.epmc-bdcc.projects.epam.com/spark_mesos:1.0 .
#docker push docker.epmc-bdcc.projects.epam.com/spark_mesos:1.0
docker build -t spark_mesos:1.0 .
docker push spark_mesos:1.0