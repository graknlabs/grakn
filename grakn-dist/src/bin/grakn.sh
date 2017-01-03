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

export GRAKN_INCLUDE="${GRAKN_HOME}/bin/grakn.in.sh"
. "$GRAKN_INCLUDE"

SLEEP_INTERVAL_S=2

case "$1" in

start)

    if [ $USE_CASSANDRA ]; then
        "${GRAKN_HOME}/bin/grakn-cassandra.sh" start
    fi
    "${GRAKN_HOME}/bin/grakn-engine.sh" start
    ;;

stop)

    "${GRAKN_HOME}/bin/grakn-engine.sh" stop
    if [ $USE_CASSANDRA ]; then
        "${GRAKN_HOME}/bin/grakn-cassandra.sh" stop
    fi
    ;;

clean)

    "${GRAKN_HOME}/bin/grakn-engine.sh" stop
    if [ $USE_CASSANDRA ]; then
        "${GRAKN_HOME}/bin/grakn-cassandra.sh" stop
        "${GRAKN_HOME}/bin/grakn-cassandra.sh" clean
    fi
    ;;

status)

    "${GRAKN_HOME}/bin/grakn-engine.sh" status
    if [ $USE_CASSANDRA ]; then
        "${GRAKN_HOME}/bin/grakn-cassandra.sh" status
    fi
    ;;

*)
    echo "Usage: $0 {start|stop|clean|status}"
    ;;

esac
