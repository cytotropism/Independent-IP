# Independent project fron hkust

A realization based on Cquirrel Algorithm. Excecute query 12 on TPC-H

## Version Configuration

This project uses the following versions for key dependencies:

- Java: JDK 21
- Apache Flink: 1.18.0
- Maven: 3.8.1 (or the version you are using)

## Building the Project
use maven to get jar package

  Maven URLS
  ----------

  Home Page:          https://maven.apache.org/
  Downloads:          https://maven.apache.org/download.html
  Release Notes:      https://maven.apache.org/docs/history.html
  Mailing Lists:      https://maven.apache.org/mailing-lists.html
  Source Code:        https://gitbox.apache.org/repos/asf/maven.git
  Issue Tracking:     https://issues.apache.org/jira/browse/MNG
  Wiki:               https://cwiki.apache.org/confluence/display/MAVEN/
  Available Plugins:  https://maven.apache.org/plugins/

## Algorithm Reference

Demo:https://cse.hkust.edu.hk/~yike/Cquirrel.pdf

Full Paper and Algorithm details:https://cse.hkust.edu.hk/~yike/sigmod20.pdf

## File

The flink file contains the algorithm main code

The data file contains the TPC-H source data and preprocessing of the data

The query file contains the query 12 code and the corectness check of the code