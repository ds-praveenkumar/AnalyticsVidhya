# Analytic Vidhya Data Engineering Hackathon

- [Hackathon Link](https://datahack.analyticsvidhya.com/contest/job-a-thon-june-2021/)
## Prerequisite
- spark 2.4
- scala 2.11
- java 1.8
- maven

Instruction
```
cd <project folder>
mvn clean package
spark-submit --class sample.PreProcessData target\DataEnggHackJune2021-1.0-SNAPSHOT.jar src/main/resources/userTable.csv src/main/resources/VisitorLogsData.csv "2018-05-28" "src/main/resources/submission"
```

