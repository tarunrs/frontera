#!/bin/bash

PIDFILE=/var/run/strategy_worker.pid

case $1 in
   start)
       #source /home/cia
       cd "/home/cia/bitbucket/frontera/examples/cluster/bc"
       # Launch your program as a detached process
       /usr/bin/nohup stdbuf -oL /usr/bin/python -m frontera.worker.strategy --config config.sw --partition-id 0 --strategy frontera.worker.strategies.bfs.CrawlingStrategy >> strategy.log &
       # Get its PID and store it
       echo $! > ${PIDFILE} 
   ;;
   stop)
      kill `cat ${PIDFILE}`
      # Now that it's killed, don't forget to remove the PID file
      rm ${PIDFILE}
   ;;
   *)
      echo "usage: strategy_worker {start|stop}" ;;
esac
exit 0
