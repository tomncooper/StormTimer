#!/bin/bash

rm target/*.jar

mvn package -Dtopology.class=TimerTopologyRunner

storm jar target/topology-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    uk.org.tomcooper.stormtimer.topology.TimerTopologyRunner \
    remote \
    StormTimer
