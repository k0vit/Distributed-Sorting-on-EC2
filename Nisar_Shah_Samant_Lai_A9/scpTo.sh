#!/bin/bash

chmod 400 InstanceDetails.csv

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type S/C

while IFS="," read II PRIP PUIP NT
do
	if [ "$NT" == "C" ]
	then
		echo "C for ip $PUIP"
	else 
		echo "S for ip $PUIP"
	fi
done < InstanceDetails.csv
