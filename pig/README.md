# Fuzzy Matching and Dataset Join Optimization

Here is  an explanation and guidance for using and optimizing a fuzzy matching process and a dataset join operation. It also includes a practical example for execution and important considerations for building JAR files.

---

## Conclusion

The company names fuzzy matching stage takes too long and requires performance tuning. Simply joining two datasets on a city column only partially reduces data crossing. The result of the join operation still contains excessive data. To improve this process, the following solutions are suggested:

1. **Performance Tuning:** Optimize the fuzzy matching stage by investigating bottlenecks in the process.
   - Consider improving the underlying UDF (User-Defined Function) leveraging the FuzzyWuzzy library.

2. **Join Optimization:** Ensure the join logic is more selective by combining columns or pre-filtering datasets.

3. **Underlying Library Optimization:** Optimize the FuzzyWuzzy configurations or switch to alternative libraries if performance remains inadequate.

---

## Run Example

The following command demonstrates how to execute the Pig script in Docker:

```sh
docker exec -it master pig /usr/local/pig-scripts/tmp_mix.pig
```

### Explanation
- `docker exec`: Runs a command in an active Docker container.
- `-it master`: Specifies the container named `master`.
- `pig`: Invokes Apache Pig for running big data scripts.
- `/usr/local/pig-scripts/tmp_mix.pig`: Path to the script that manages the fuzzy matching and join process.

---

## Notes

### Note 1: Optimizing Fuzzy Matching
- **Challenge:** The current fuzzy matching stage based on the FuzzyWuzzy library is inefficient when dealing with large datasets.
  - Example: Matching company names such as `"Google Inc."` and `"Google LLC"`.
  - Reason: The FuzzyWuzzy algorithm is not inherently optimized for high-volume data matching.

- **Solution:**
  1. Pre-filter datasets by removing duplicates and normalizing strings.
     - Example: Convert all strings to lowercase and strip whitespaces.
  2. Replace or modify FuzzyWuzzy with a more efficient library like `RapidFuzz`.
  3. Implement parallel processing for the fuzzy matching task to leverage multi-core processors.

### Note 2: Optimizing Dataset Joins
- **Challenge:** Joining datasets using the `city` column alone creates too many combinations, resulting in unnecessary overhead.
  - Example: Joining `Dataset A` with `Dataset B` where `Dataset A` has 1 million rows and `Dataset B` has 500,000 rows based solely on a shared column leads to an explosion of interim data.

- **Solution:**
  1. Filter datasets before the join:
     - Example: Use WHERE clauses to exclude irrelevant rows.
  2. Combine multiple columns in the join condition to reduce the result set:
     - Example: Use `city` and `state` or `city` and `company_name`.
  3. Perform distributed joins when working with large-scale datasets in Apache Hadoop or Spark.

### Note 3: Building JAR Files Properly
- **Recommendation:** Use a proper code editor (e.g., IntelliJ IDEA or Eclipse) for building JAR files. This ensures correct metadata inclusion and prevents runtime issues.

---

### Practical Example of JAR Metadata
1. **Manifest File:** Add metadata in the `MANIFEST.MF` file located in the `META-INF` directory.
   ```plaintext
   Manifest-Version: 1.0
   Main-Class: com.example.Main
   Class-Path: lib/
   ```

2. **Use Gradle or Maven:** These tools automate dependency management and JAR building:
   - Gradle Example:
     ```groovy
     apply plugin: 'java'

     jar {
         manifest {
             attributes(
                 'Main-Class': 'com.example.Main'
             )
         }
     }
     ```

   - Maven Example:
     ```xml
     <build>
         <plugins>
             <plugin>
                 <groupId>org.apache.maven.plugins</groupId>
                 <artifactId>maven-jar-plugin</artifactId>
                 <configuration>
                     <archive>
                         <manifest>
                             <mainClass>com.example.Main</mainClass>
                         </manifest>
                     </archive>
                 </configuration>
             </plugin>
         </plugins>
     </build>
     
