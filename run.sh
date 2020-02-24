#!/usr/bin/env bash


# Don't use this yet. not complete
cd
cd kafka_2.13-2.4.0
bin/zookeeper-server-stop.sh config/zookeeper.properties
bin/zookeeper-server-start.sh config/zookeeper.properties


EW=$(xdotool search --onlyvisible --classname Gnome-terminal|head -1)

if [[ -z  $EW ]]
then
 gnome-terminal &
else
 xdotool windowactivate --sync  $EW
 xdotool key --clearmodifiers ctrl+shift+t
fi

kafka-server-stop.sh config/server.properties
kafka-server-start.sh config/server.properties
