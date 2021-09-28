# SPARQL2Flink-HDT library
RDF compression capabilities have been studied in Fernández et al. [2010]. An overall strategy has been defined as an RDF data-centric format, which reduces verbosity in favor of machine understandability and data management, the so-called RDF HDT format [Fernández, 2012]. HDT is a binary serialization format for RDF, and it is currently a W3C Member Submission (http://www.w3.org/Submission/HDT/). HDT provides effective RDF decomposi- tion, simple compression notions, and basic indexed access in a compact serialization format, providing efficient access to the data.

This repository is an proof of concept which implement an HDT technique in orde to improves the SPARQL2Flink capabilities for querying massive static RDF data. 

## System requirements

* Oracle JDK 1.8.0_144
* Apache Maven 3.5.0 or higher

## Compile the SPARQL2Flink artifact

Deploy with maven usign the configuration in pom.xml

```
mvn clean install compile package
```

## Transform your SPARQL query to an Apache Flink program

Run the sparql2fLink java library with the name of the query file and the name of the input dataset

```
java -cp target/sparql2flink-1.0-SNAPSHOT.jar SPARQL2FLink examples/query.rq
```

## Create the Flink program as .jar file to be runned on your Flink local cluster

Deploy with maven using the configuration in pom_query.xml

```
mvn -f pom_query.xml clean package
```

## References
Fernández, J. D., Gutierrez, C., and Martínez-Prieto, M. A. (2010). Rdf compression: Basic approaches. In Proceedings of the 19th International Conference on World Wide Web, WWW ’10, page 1091?1092, New York, NY, USA. Association for Computing Machinery

Fernández, J. D. (2012). Binary rdf for scalable publishing, exchanging and consumption in the web of data. In Proceedings of the 21st International Conference on World Wide Web, WWW ’12 Companion, page 133?138, New York, NY, USA. Association for Computing Machinery.
