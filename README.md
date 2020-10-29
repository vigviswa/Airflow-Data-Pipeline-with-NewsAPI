# Data Engineer Pipeline in Airflow
In this challenge,
 we will develop a simple
 [Apache Airflow](https://airflow.apache.org) data pipeline.

## Summary
Our data pipeline fetches data from [News API](https://newsapi.org),
 transforms the data into a tabular structure,
 and stores the transformed data on [Amazon S3](https://aws.amazon.com/s3/).

## Quickstart
1. I recommend starting with a fresh virtualenv using Python 3.6 on a *nix system.
    * Our docker versions are docker 17.12.0-ce and docker-compose 1.18.0.
2. Run `make init` to download project dependencies.
3. Run `make test` to make sure basic smoke tests are passing.
4. Run `make run` with docker running to bring up airflow.
    * The Airflow UI/Admin Console should now be visible on [http://localhost:8080](http://localhost:8080).
    * There will be a DAG named `sample_dag`. You should be able to view logs from the [Task Instance Context Menu](https://airflow.readthedocs.io/en/latest/ui.html#task-instance-context-menu).
5. Follow the requirements + rules of engagement below.

## Requirements
- [ ] Use Airflow to construct a new data pipeline (DAG) named 'tempus_challenge_dag'.
- [ ] Data pipeline is scheduled to run once a day.
- [ ] Data pipeline will:
  - [ ] Retrieve all English news sources.
  - [ ] For each news source, retrieve the top headlines.
    - [ ] Top headlines must be flattened into a CSV file. CSV Filename: `<pipeline_execution_date>_top_headlines.csv`
    - [ ] Result CSV must be uploaded to the following s3 location `<s3_bucket>/<source_name>`
- [ ] Built a separate pipeline that uses the following keywords instead of English news sources: Tempus Labs, Eric Lefkofsky, Cancer, Immunotherapy


## [Apache Airflow](https://airflow.apache.org)
From the Apache Airflow documentation:

>Airflow is a platform to programmatically author, schedule and monitor workflows.

>Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

>When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

In order to facilitate the use of Airflow, we have included a Dockerfile and a docker-compose.yml that can be used to set up a local airflow development environment. **Make sure to have [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.**

From the root folder, you can execute the following command to run airflow:
```
docker-compose up --build
```
The Airflow UI/Admin Console should now be visible on [http://localhost:8080](http://localhost:8080).

### DAGs
In order to build the data pipeline, it will be necessary to create a DAG. We have provided an example DAG, `dags/sample_dag.py`, that can be used as a reference. Further documentation can be found in the [airflow tutorial](https://airflow.apache.org/tutorial.html) and the [airflow concepts](https://airflow.apache.org/concepts.html) pages.

To load a new DAG into airflow, simply create a new Python file in the `dags` folder that contains an airflow DAG object.

### Python Packages
To install additional Python packages (boto3, pandas, requests, etc.), add them to `requirements.txt`.

### Related Articles/Tutorials
* https://airflow.apache.org/index.html
* https://medium.com/@dustinstansbury/understanding-apache-airflows-key-concepts-a96efed52b1a
* https://speakerdeck.com/artwr/apache-airflow-dataengconf-sf-2017-workshop
* https://github.com/hgrif/airflow-tutorial

## [News API](https://newsapi.org)
A simple REST API that can be used to retrieve breaking headlines and search for articles. **A free News API account is required to obtain an API key.**

| Route             | Description                                                                                                                |
|-------------------|----------------------------------------------------------------------------------------------------------------------------|
| [/v2/top-headlines](https://newsapi.org/docs/endpoints/top-headlines) | Returns live top and breaking headlines for a country, specific category in a country, single source, or multiple sources. |
| [/v2/sources](https://newsapi.org/docs/endpoints/sources) | Returns the subset of news publishers that top headlines are available from. |


## [Amazon S3](https://aws.amazon.com/s3/)
A simple cloud storage service run by Amazon Web Services (AWS). **An AWS account is needed to use AWS S3. Furthermore, AWS has a [free tier](https://aws.amazon.com/free/) that can be used for this challenge.**

Amazon provides a Python SDK (**[boto](http://boto3.readthedocs.io/en/latest/guide/resources.html)**), that provides an easy to use API for interacting with AWS S3.
