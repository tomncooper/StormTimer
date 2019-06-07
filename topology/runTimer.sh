#!/bin/bash

# Arguments:
#   1: local/remote 
#   2: basic/f2f/multi/window
#   3: sync/async

if [ $2 = 'basic' ] 
then
	topo='BasicTimerTopologyRunner'
        name='BasicTimer'
elif [ $2 = 'f2f' ] 
then
	topo='F2FTimerTopologyRunner'
        name='F2FTimer'
elif [ $2 = 'multi' ] 
then
	topo='MultiplierTimerTopologyRunner'
        name='MultiplierTimer'
elif [ $2 = 'window' ] 
then
	topo='WindowedTimerTopologyRunner'
        name='WindowedTimer'
else
	echo 'Unkown topology type :' $2 '. Should be one of basic/f2f/multi/window'
	exit 1
fi

rm target/*.jar

mvn package -Dtopology.class=$topo

if [ $1 = 'local' ] 
then

    localname=$name'-Local'

    storm jar target/topology-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
        uk.org.tomcooper.stormtimer.topology.$topo \
        local \
        $localname \
        $3 \
        120000

elif [ $1 = 'remote' ]
then

storm jar target/topology-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    uk.org.tomcooper.stormtimer.topology.$topo \
    remote \
    $name \
    $3

else
	echo 'Unkown deployment type :' $1
	exit 1
fi
