Impala Workload Analyzer
========================
    Parse thrift-formatted profiles and analyze characteristics of workloads

## USAGE
    bash driver.sh tag inputFile|inputDir outputDir
    Run all the code and generate graphs
    Instead of running the driver script, users can run each component individually (see driver.sh)

## DIRECTORY STRUCTURE
    .
    |— README.md
    |— data: example profiles
    |— code
        |— driver.sh
        |— java
            |— src/com/cloudera/impala/analysis/QueryAnalyzer.java: parse sql
        |— python
            |— RuntimeProfile.thrift: definition of thrift-formatted profiles
            |— analyze_profiles.py: analyze one profile file
            |— profile_analyzer.py: analyze one query profile
            |— stats.py: generate graphs for a workload
            |— joins.py: check the quality of joins
            |— aggs.py: check the quality of pre aggs
            |— plots.py: helper file to draw graphs
            |— delete_queries_with_tag.py: delete queries with a particular tag
            |— clustering.py: group queries into clusters

## RUN ON vd0204.halxg.cloudera.com
    1. Make sure mongodb is running
    2. Activate virtualenv (source venv/bin/activate)
    3. Clear database (use impala; db.dropDatabase()) or delete queries with a particular tag (python delete_queries_with_tag.py tag)
    4. Run driver.sh
