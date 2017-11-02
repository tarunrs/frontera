#!/bin/bash

PIDFILE=/var/run/feed_processor.pid

case $1 in
   start)
       #source /home/cia
       # Launch your program as a detached process
       /usr/bin/python /home/cia/bitbucket/frontera/examples/feed_parser/feed_processor.py 1 0 /home/cia/bitbucket/frontera/examples/feed_parser/output.log 2>/tmp/feed_processor.log &
       # Get its PID and store it
       echo $! > ${PIDFILE} 
   ;;
   stop)
      kill `cat ${PIDFILE}`
      # Now that it's killed, don't forget to remove the PID file
      rm ${PIDFILE}
   ;;
   *)
      echo "usage: feed_processor {start|stop}" ;;
esac
exit 0
