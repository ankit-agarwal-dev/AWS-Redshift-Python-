# Redshift Tutorial

Redshift Tutorial is collection of scripts that explains in simple steps about various ETL operations on a Redshift DB using Python on AWS. This tutorial also briefly explains about Data Warehousing concepts and its implementation in a star schema (Fact and Dimensions).
Following concepts have been covered in this tutorial
* Creation of redshift cluster
* Dropping tables (handling existence/non-existence)
* Creating tables (handling existence/non-existence). This also emphasize on constraints and its implementation.
* Normal and Bulk insertion into tables ensuring no violation of constraints.
* Joining of two tables for fetching some values.

     
## Installation

```python
pip install create_tables
pip install etl
pip install redshift_cluster
```

## Usage

```python
import create_tables
import etl
import redshift_cluster

```
## Contributing
Any suggestions are welcome. For major changes, please open an issue first to discuss what you would like to change.   

## License
Applied