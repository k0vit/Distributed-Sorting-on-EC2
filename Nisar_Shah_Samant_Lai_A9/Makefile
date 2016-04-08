dist_sort:
	cd ClusterManager && exec mvn clean install
	cp ClusterManager/target/nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cd SortNode && exec mvn clean install
	cp SortNode\0.0.1-SNAPSHOT\SortNode-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cd client && exec mvn clean install
	cp client\0.0.1-SNAPSHOT\client-0.0.1-SNAPSHOT-jar-with-dependencies.jar .	
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar create > cluster_manager_create.txt 2>&1
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar start > cluster_manager_start.txt 2>&1
	chmod 777 scpTo.sh && ./scpTo.sh
	chmod 777 sshToRun.sh && ./sshToRun.sh $(INPUT_PATH) $(OUTPUT_PATH)
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar terminate $(OUTPUT_PATH) > cluster_manager_terminate.txt 2>&1