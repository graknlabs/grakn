#!/bin/bash

#
# Grakn - A Distributed Semantic Database
# Copyright (C) 2016  Grakn Labs Limited
#
# Grakn is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Grakn is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# <http://www.gnu.org/licenses/gpl.txt>.

if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

redisRunning()
{
    echo $(ps -ef | grep "redis-server" | grep -v grep | awk '{ print $2}')
}

executeRedisServer(){
    if [ "$(uname)" == "Darwin" ]; then
        "${GRAKN_HOME}/bin/"redis-server-osx $1 &
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        "${GRAKN_HOME}/bin/"redis-server-linux $1 &
    fi
}

executeRedisCli(){
    if [ "$(uname)" == "Darwin" ]; then
        "${GRAKN_HOME}/bin/"redis-cli-osx $1
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        "${GRAKN_HOME}/bin/"redis-cli-linux $1
    fi
}

case "$1" in

start)
	if [ $(redisRunning) ] ; then
    	echo "Redis is already running"
    else
    	echo "Starting redis"
    	executeRedisServer "${GRAKN_HOME}/conf/redis/redis.conf"
	fi
    ;;
stop)
	echo "Stopping redis"
    executeRedisCli shutdown
    ;;
clean)
    echo "Cleaning redis"
    executeRedisCli flushall
    executeRedisCli shutdown
    ;;
status)
    if [[ $(executeRedisCli PONG) ]]; then
        echo "Redis is running"
	else
	    echo "Redis has stopped"
	fi
	;;
esac