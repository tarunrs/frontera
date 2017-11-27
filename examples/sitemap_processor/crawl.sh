#!/bin/sh
if ps -ef | grep -v grep | grep sitemap_processor/crawl.py ; then
        exit 0
else
        /home/cia/bitbucket/frontera/examples/sitemap_processor/crawl.py 1 0 output.log
        exit 0
fi
