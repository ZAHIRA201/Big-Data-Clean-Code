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

# Project's Final Conclusions
The identified bottleneck is the similarity classification function, which has high complexity due to inefficient data joining. To improve this function in the future, when running processes on full datasets instead of just 10%, an initial classification could be performed, such as checking only if the first letter matches. This could positively impact execution time by reducing memory complexity. In the current case of joining data by city, there is either one or no companies from the first dataset in the second dataset, resulting in inefficient computations. The efficiency of the string similarity algorithm implementation is also crucial.

The difference in execution time between Spark (~4m30s) and PIG (~21m) could be attributed to the fact that the temporary dataset `tmp_injuries_filtered` had a size of 1.6MB in Spark, while it was 6MB in PIG.

---

## Issues to Address
1. **RAM Memory Tuning**  
   Adjust memory allocation to optimize resource utilization.

2. **Bottleneck Identification**  
   Use profiling tools to locate and address computational bottlenecks.

3. **Efficient Queries**  
   Refactor queries to minimize unnecessary operations.

4. **Fuzzy Matching Concept**  
   Optimizing the fuzzy matching implementation can significantly reduce processing times.

For example, upgrading the fuzzy matching library using the command:
```bash
pip install fuzzywuzzy[speedup]
```
This version includes optimized C-based implementations for better performance.

---

## Workflows and Results
### Workflow 1
![Workflow 1](./readme-res/p1-workflow.png "Workflow 1 Description")

### Result 1
![Result 1](./readme-res/p1-result.png?raw=true "Result 1 Description")

### Workflow 2
![Workflow 2](./readme-res/p2-workflow.png?raw=true "Workflow 2 Description")

### Result 2
![Result 2](./readme-res/p2-result-top10.png?raw=true "Result 2 Top 10")

---

## Datasets Origins
### Dataset 1
[OSHA Injury Data (2016-2021)](https://www.kaggle.com/datasets/robikscube/osha-injury-data-20162021)

### Dataset 2
[Free 7 Million Company Dataset](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset)

---

## Example Run Command
To execute the script using Spark, run the following command:
```bash
time docker exec -it master spark-submit /usr/local/spark-scripts/tmp_companies_extracted.py
```
- **time**: Tracks the script's runtime.
- **docker exec -it master**: Runs the command interactively within the `master` container.
- **spark-submit**: Submits the Python script to the Spark cluster for processing.

---

## Recommended Practices
1. **Use Containerization**:  
   Utilize Docker to standardize environments and simplify deployment.

2. **Maintain a Clean Repository**:  
   Organize scripts logically in directories, such as placing utility functions under `udf/`, and track dependencies using a `requirements.txt` file:
   ```bash
   pip freeze > requirements.txt
   ```

3. **Monitor Performance**:  
   Leverage Spark's UI to monitor stages, tasks, and jobs.

4. **Optimize Algorithms**:  
   Prioritize optimizing heavy computations, such as fuzzy matching functions, for better scalability and performance.

---

This README serves as a guide for project workflows, challenges, and best practices. For further information or support, consult the repository or reach out to project maintainers.
