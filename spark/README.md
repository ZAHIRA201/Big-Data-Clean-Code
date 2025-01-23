# Project Overview
This project demonstrates the use of Spark for efficient functional programming, with significant improvements in performance compared to PIG-based implementations. Functional programming not only simplifies the execution but also allows for more streamlined and efficient query processing.

---

## Conclusion
The implementation using Spark is considerably faster and easier to execute than the previous approach using PIG. Its functional programming paradigm enables writing and executing queries that are inherently more efficient, contributing to improved overall performance.

---

## Key Issues to Address
Despite the advantages of Spark over PIG, certain optimizations are required to fully utilize the system's capabilities:

1. **RAM Memory Tuning**
   - Adjust memory allocation for Spark executors to ensure optimized resource utilization.

2. **Bottleneck Identification**
   - Conduct profiling of Spark jobs to detect and address bottlenecks during execution.

3. **Efficient Query Optimization**
   - Refactor queries to minimize unnecessary operations and improve runtime efficiency.

4. **Fuzzy Matching Optimization**
   - The use of fuzzy matching for company names, powered by the `fuzzywuzzy` Python library, had performance issues due to outdated packages. Updating to the latest version resolved the problem:
     ```bash
     pip install fuzzywuzzy[speedup]
     ```
     The "speedup" installation provides significant improvements by leveraging C-based implementations under the hood.

---

## Example Run Command
To execute the Spark-based implementation, use the following command:

```bash
time docker exec -it master spark-submit /usr/local/spark-scripts/tmp_companies_extracted.py
```
- **time**: Measures the time taken for the job execution.
- **docker exec -it master**: Executes the command interactively inside the `master` container.
- **spark-submit**: Submits the Python script to the Spark cluster for processing.

---

## Recommended Best Practices
1. **Containerization:**
   - Use Docker to isolate environments and ensure consistency across different systems.

2. **Script Location:**
   - Store all Spark scripts in `/usr/local/spark-scripts` to maintain a clean and organized file hierarchy.

3. **Code Repository Structure:**
   - Include a folder for utility functions (e.g., `udf/`) and ensure that all library dependencies are well-documented in a requirements file.
     ```bash
     pip freeze > requirements.txt
     ```

4. **Performance Monitoring Tools:**
   - Leverage Spark's UI for monitoring tasks, stages, and jobs to quickly identify areas that need optimization.

---

## Note
This project highlights the importance of continuous optimization. Moving from PIG to Spark significantly reduces processing time and complexity, but the steps outlined above ensure that the implementation is efficient, scalable, and maintains optimal performance.

For further clarifications, feel free to consult the repository or contact the maintainers.
