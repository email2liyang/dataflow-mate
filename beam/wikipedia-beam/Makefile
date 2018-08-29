run_word_count_direct:
	mvn compile exec:java -D exec.mainClass=org.datanerd.dataflow.MinimalWordCount \
	-D exec.args="--inputFile=pom.xml --output=counts" \
	-Pdirect-runner

run_word_count_flink:
	mvn compile exec:java -D exec.mainClass=org.datanerd.dataflow.WordCount \
	-D exec.args="--runner=FlinkRunner --parallelism=6 --inputFile=pom.xml --output=counts" \
	-Pflink-runner

run_window_word_count_flink:
	mvn compile exec:java -D exec.mainClass=org.datanerd.dataflow.WindowedWordCount \
	-D exec.args="--runner=FlinkRunner --parallelism=1 --numShards=2 --inputFile=pom.xml --output=counts" \
	-Pflink-runner
clean:
	rm counts*
build:
	mvn package