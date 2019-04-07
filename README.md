# Scholarly Impact Services (SIS) ETL Jobs

Scripts to extract, transform and load data from various data sources to a MongoDB database. 
It also contains scripts to transform data in MongoDB to CSV files for further analysis.

## Setup

`pipenv` is used to manage the python version and packages. You may install it with brew or pip. 

Install packages with pipenv

```
pipenv install
``` 

## Run ETL Jobs

```
pipenv run bonobo run -m etl.jobs.{etl_job_name}
```

Or

```
pipenv run python etl/jobs/{etl_job_name}.py 
```

## Build and run with Docker

```
docker build -t jiaola/sisetl .
```

