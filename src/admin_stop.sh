#!/usr/bin/env bash

binary_name=$1
echo $binary_name

process_count=`ps -A | grep $binary_name|grep -v grep`
echo $process_count
process_id=`ps -A | grep python | grep $binary_name|grep -v grep | awk '{ print $1}'`
if [ "$process_id" != "" ]; then
	kill -9 $process_id
fi
