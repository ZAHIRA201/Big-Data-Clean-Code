# Spark vs PIG Data Processing

This project's main goal is to model and implement big data processes. The project consisted of 10 stages described below.

# Stages
1. Project Assumptions
    - Selection of source datasets and determination of their content and availability. Indication of expected data processing results.
2. Source Data for Input
    - Find 2 datasets on the web, 100MB minimum each and analyze them.
    - For each data source, provide a description of the content, including attributes, their types, and an example of the content.
3. Data for Output
    - Describe the structure of the resulting dataset, including attributes, their types, and an example of the content.
4. Model Data Processes.
    - **Required implementation of data merging from various sources, data aggregation, and data transformation operations.**
    - Development of a method for deriving the resulting dataset based on the source data. Identification of intermediate stages of data processing and the obtained temporary data.
5. Hadoop
    - Installation and execution of a Hadoop cluster in a Docker environment with at least 3 nodes.
6. Data Acquisition
    - Downloading the source data and storing it in the Hadoop Distributed File System (HDFS). Automating the process of retrieving data that undergoes changes in the source system.
    - Log the timestamp, size of the data, and the status of the process
    - 3 replicas in HDFS.
7. PIG
    - Installation and execution of PIG in the cluster.
8. PIG - data processing workflows implementation
    - Prepare the implementation of the previously designed data processing workflows using PIG on the Hadoop platform. Log execution times and errors of every stage.
9. Spark
    - Installation and execution of Spark in the cluster.
10. Spark - data processing workflows implementation
    - Prepare the implementation of the previously designed data processing workflows using Spark on the Hadoop platform. Log execution times and errors of every stage.
11. Conclusion & PIG vs Spark
    - Compare those tools basing on the data gathered.
    - Write conclusions

# Ongoing