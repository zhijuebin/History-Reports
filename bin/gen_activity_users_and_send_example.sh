#!/usr/bin/env bash
source ~/.bash_profile
echo "begin ######################################################################"
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
home=`echo $bin|xargs dirname`
echo "directory path: $home"

source $home/virtual_python/bin/activate

datestr=`date "+%Y-%m-%d"`
datestr_US=`date -d "-1 day" "+%m/%d/%Y"`
start_time=`date +%s`

echo "excuting date: $datestr"

echo "delete duplicate report data  ###############################################"
datapath=$home
rm -rf $datapath/data/csv/$datestr*.csv

echo "start to generate new report data  ##########################################"
#python -W ignore $home/scripts/exception_user_statistics.py

echo "start to send emails  #######################################################"

#add some receivers here
outer="xxx@xxx.com"

title="[TP Report] Active users $datestr_US"

python $home/scripts/send_email.py  $outer "$title" "$datapath/data/csv/$datestr Last 7 days activity users.csv,$datapath/data/csv/$datestr activity users.csv"

end_time=`date +%s`
echo "cost time:$((end_time-start_time))"
echo "end #########################################################################"
