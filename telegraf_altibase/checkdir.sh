#!/bin/sh

ALTIBASE_HOME=~altibase/altibase_home

for FIND in `echo "LOG_DIR ARCHIVE_DIR"`
do
   DIR=`egrep "^${FIND}" $ALTIBASE_HOME/conf/altibase.properties | cut -d '=' -f 2 | sed -e "s/#.*$//g" | sed -e "s/ //g"`
   export DIR

   COUNT=`ls -l ${DIR}/logfile* 2>/dev/null | wc -l`

   RESULT=`df -k ${DIR} | grep -v Filesystem |  sed -e "s/%//g" | sed -e "s/\ \+/ /g"`

   FILESYSTEM=`echo $RESULT | cut -d' ' -f 1`
   USED=`echo $RESULT | cut -d' ' -f 3`
   AVAILABLE=`echo $RESULT | cut -d' ' -f 4`
   USERATIO=`echo $RESULT | cut -d' ' -f 5`
   MOUNT=`echo $RESULT | cut -d' ' -f 6`

   echo "alti_diskusage,name=${FIND} used=${USED},available=${AVAILABLE},useratio=${USERATIO},count=${COUNT}"
done
