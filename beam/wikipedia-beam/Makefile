run_word_count:
	mvn compile exec:java -D exec.mainClass=org.datanerd.dataflow.WordCount -D exec.args="--inputFile=pom.xml --output=counts" -P direct-runner

clean:
	rm counts*