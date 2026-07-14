# Interval join helper for Apache Spark
[![Unit tests](https://github.com/amadeusz-kosik/interwalled/actions/workflows/code_quality.yml/badge.svg)](https://github.com/amadeusz-kosik/interwalled/actions/workflows/code_quality.yml)

This repository contains all components to implement, use and benchmark a few implementations for interval join 
    in Apache Spark library. The interval join is defined as a join with following conditions 
    on _database_ and _query_ tables:
```sql
SELECT 
    *
FROM 
    database, query
WHERE database.key = query.key -- grouping key
    AND query.start    <= database.end
    AND database.start <= query.end
```

### Running on Java 17
Running on newer Java versions requires adding exports' parameters as a JVM option:
```bash
JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.lang=ALL-UNNAMED"
```

## External links
- [Original AIList article](https://academic.oup.com/bioinformatics/article/35/23/4907/5509521)
- [Original AIList implementation on GitHub](https://github.com/databio/AIList/)
- [IITII implementation on Apache Spark - 1](https://github.com/Wychowany/mgr-iitii/tree/main)
- [IITII implementation on Apache Spark - 2](https://github.com/Wychowany/mgr-code/tree/main)

## License
This work is licensed under <a href="https://creativecommons.org/licenses/by-nc-sa/4.0/">CC BY-NC-SA 4.0</a>. See [LICENSE](LICENSE) file for full license text.
