chmod 400 ec2key.pem

while IFS="=" read key value
do
	key=${key// }
        value="${value// }"

        if [ "$key" == "AccessKey" ]
        then
                accesskey=$value
        elif [ "$key" == "SecretKey" ]
        then
                secretkey=$value
	elif [ "$key" == "Bucket" ]
        then
                clusterdetails=$value
		clusterdetails+="/InstanceDetails.csv"
        fi
done < cluster.properties

echo $accesskey
echo $secretkey
echo $clusterdetails

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type S/C

while IFS="," read II PRIP PUIP NT
do
        if [ "$NT" == "C" ]
        then
                clientip=$PUIP
        else 
                ssh -i ec2key.pem -o StrictHostKeyChecking=no ec2-user@$PUIP "java -cp ~/HelloWorldSortNode.jar test.Main 2>&1" > sortnode-$PUIP-output.txt &
        fi
done < InstanceDetails.csv

ssh -i ec2key.pem -o StrictHostKeyChecking=no ec2-user@$clientip "java -cp ~/HelloWorldSortNode.jar test.Main 2>&1" > client-output.txt
