#!/bin/bash

PIDFILE=/var/run/db_worker_incoming.pid

case $1 in
   start)
       #source /home/cia
       cd "/home/cia/bitbucket/frontera/examples/cluster/bc"
       # Launch your program as a detached process
       /usr/bin/nohup stdbuf -oL /usr/bin/python -m frontera.worker.db --config config.dbw --no-batches >> db-log.log &
       # Get its PID and store it
       echo $! > ${PIDFILE} 
   ;;
   stop)
      kill `cat ${PIDFILE}`
      # Now that it's killed, don't forget to remove the PID file
      rm ${PIDFILE}
   ;;
   *)
      echo "usage: db_worker_incoming {start|stop}" ;;
esac
exit 0
