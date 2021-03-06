# Scholarly Impact Services (SIS) ETL Jobs

Scripts to extract, transform and load data from various data sources to a MongoDB database. 
It also contains scripts to transform data in MongoDB to CSV files for further analysis.

## Setup

`pipenv` is used to manage the python version and packages. You may install it with brew or pip. 

The package `pymssql` requires `freetds`, so on mac, install it first. 

```
brew install freetds 
```

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

## ETL Jobs

### Scopus Jobs

Here are jobs to retrieve data from Scopus. Set ENV to `dev` (default), `test`, or `production` to load
different environment variables. For example,

```
ENV=TEST pipenv run bonobo run -m etl.jobs.scopus.biophysics 
```

#### Biophysics

This is a prototype job to retrieve scopus data for only one department at JHU: biophysics

```
$scopus.biophysics 
```

#### Extract data based on Faculty Force

Using the following scrip to extract articles data based on faculty force

``` 
pipenv run bonobo run -m etl.jobs.ff.ff_import
```

## Build and run with Docker

```
docker build -t jiaola/sisetl .
```


