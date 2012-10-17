#!/bin/bash

if [ $# -gt 0 ] ; then
  java -cp target/storm-pattern-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.ssparrow.storm.pattern.topo.AssociatePatternMiningTopology "$*" 
else
  java -cp target/storm-pattern-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.ssparrow.storm.pattern.topo.AssociatePatternMiningTopology
fi
