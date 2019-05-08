#!/bin/bash

# Arguments:
#	1: local/remote 
#   2: basic/multi

rm target/*.jar

if [ $2 = 'basic' ] 
then
	topo='BasicTimerTopologyRunner'
elif [ $2 = 'multi' ] 
then
	topo='MultiplierTimerTopologyRunner'
else
	echo 'Unkown topology type :' $2
	exit 1
fi

mvn package -Dtopology.class=$topo

if [ $1 = 'local' ] 
then

storm jar target/topology-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    uk.org.tomcooper.stormtimer.topology.$topo \
    local \
    TimerLocal \
    120000

elif [ $1 = 'remote' ]
then

storm jar target/topology-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    uk.org.tomcooper.stormtimer.topology.$topo \
    remote \
    StormTimer

else
	echo 'Unkown deployment type :' $1
	exit 1
fi
