<img align="center" src="https://media.vcg.group/media/2024/11/starlink_banner.jpg" alt="Starlink Banner Photo">

Problem Statement
=================

Imagine you are being task to build a data pipeline for SpaceX to solve one of their business question.

> When will there be 42,000 Starlink satellites in orbit, and how many launches will it take to reach that number?

To answer this question, we may break it down into several sub questions:
1. What is the number of Starlink satellites in the orbit now?
2. How many launches did SpaceX made within a period (monthly/yearly)?
3. How many satellites per launch?

Therefore, the essential data to ingest from [SpaceX-API](https://github.com/r-spacex/SpaceX-API) are:
- Starlink data
- Launches data

Run this project
================

> [!Tip]
> Prerequisite
> 1. [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/)
> 2. [Docker](https://www.docker.com/)

Start Airflow on your local machine by running `astro dev start`.

This command will spin up seven Docker containers on your machine:

-   Data Warehouse (Postgres): Simulating a Data Warehouse
-   Minio: Simulating Data Lake e.g. AWS S3
-   Airflow Postgres: Airflow's Metadata Database
-   Scheduler: The Airflow component responsible for monitoring and triggering tasks
-   DAG Processor: The Airflow component responsible for parsing DAGs
-   API Server: The Airflow component responsible for serving the Airflow UI and API
-   Triggerer: The Airflow component responsible for triggering deferred tasks

When all containers are ready, the command will open the browser to the Airflow UI at http://localhost:8080/. If the dag are yet to be enable, enable it. It will automatically run. The dag is scheduled to run on daily basic.

Some links to take note:
| Component | Link |
| --- | --- |
| Airflow UI | http://localhost:8080/ |
| Data Warehouse | postgres://warehouse:warehouse123@localhost:5433/spacex |
| Data Lake | http://localhost:9001 (U:minio P:minio123) |

Expected Result
===============

![Data Pipeline](./docs/data-pipeline.png)

The overall flow should look something like this, which does:
1. Get data from API
2. Store API in JSON
3. Format JSON data into CSV
4. Store CSV file
5. Load to Data Warehouse
6. Transform data by using dbt

Once the pipeline complete, you may login to [Minio](http://localhost:9001) - Data Lake to see the raw data.

![Minio Screenshot](./docs/minio-screenshot.png)

> [!Note]
> I use Minio to simulate Data Lake, you can imagine it is AWS S3. 

To check if dataset is ready, you may sign in to Postgres, you may use [pgAdmin 4](https://www.pgadmin.org/download/) with the following credentials:

| Key | Value |
| :- | :- |
| Hostname | localhost |
| Port | 5433 |
| Username | warehouse |
| Password | warehouse123 |

> [!Note]
> I used port `5433` to prevent it clashes with Airflow's Postgres.
> I used Postgres to simulate my data warehouse.

In order to complete this exercise within limited timeframe, I have come out with the simplest data models that are sufficient to answer the business question above.

![Data Models](./docs/data-models.png)

From these models, data analysts can get a list of all current satellites from the fact table and know how many satellites per launch. To find out more details about the number of launches per month, they can check the dim_launches table.

In this data model, I've used factless fact table as it doesn't have any metrics/facts about starlink. I assume the Starlinks data return from the API represents the latest status tracked by Space-Track and they are all currently active.

Challenges
==========

Mainly due to the limitations of the available documentation and my current domain knowledge of spacex, I couldn't clearly identity daily transactions like of data from this API. Therefore, I have temporarily put the daily ingestion plan aside. In a real world project, a daily ingestion pipeline would allow the entire process to remain idempotent, making it easy to backfill or rerun any specific dates. With that said, I made the tradeoff here, instead of performing incremental updates, I have chosen to simply ingest and replace all the data in my data warehouse during the daily pipeline execution. This is not an adviceable approach for a professional project with a large dataset, as it cause a significant waste of resource. By replacing the data completely, the dbt transformation must recreate the data from scratch every time. With a larger dataset, this translates to longer compute times and higher cost.

The second challenge was related to the tech stack required for this exercise. With the limited experience using kubernetes, I faced a choice: either develop with Airflow Kubernetes, which carries a high risk of getting stuck on configuration and having to redo everything, or start development locally, get everything working, and then chain the piece together. I chose the latter, which is start by develping with Airflow locally. The tradeoff I made was primarily due to time constraints. I would rather have a working solution first than a perfect one from the very begining. Once a working solution is in place, I can then transition towards the end goal of running it in Airflow Kubernetes. That was my strategy to get things done. With the stack I chose, I am using [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/) and [Cosmos](https://astronomer.github.io/astronomer-cosmos/) to run the entire process.

This led to the third challenge, because I used Airflow 3 and cosmos to run my dbt transformation. I found out that [dbt docs](https://astronomer.github.io/astronomer-cosmos/configuration/generating-docs.html) couldn't be host on Airflow 3 yet (_as of 2025-07-31_).

The forth challenge was with data modelling, the challenge comes in when I considered taking all columns from the raw data, but I quickly realized that not all columns would be needed to answer business question. Therefore, I only select the essential columns required to complete the exercise.

Future Work
===========

In order to get production ready data pipeline, I should use Kubernetes as it is the de facto industry standard. After some research, these are the steps I could implement using Kubernetes. 
1. Create Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/).
2. Install Airflow
    - create kubernetes namespace
    - add Airflow helm repo
    - use helm to install [Airflow Chart](https://artifacthub.io/packages/helm/apache-airflow/airflow)
    - port forward airflow service to access the UI
3. Configure Airflow on kubernetes by modifying its values.yaml
    - executor: KubernetesExecutor
    - use configMap if needed
4. Deploy my DAGs using gitSync
5. Configure Persistent Volume and Persistent Volume Claim to save logs
6. Could use PV and PVC to sync dags locally with using gitSync

> [!Note]
> References:
> - [Use minikube to run Airflow Kubernetes locally](https://medium.com/@jdegbun/deploying-apache-airflow-on-kubernetes-with-helm-and-minikube-syncing-dags-from-github-bce4730d7881)
> - [Use Kind to run Airflow Kubernetes locally](https://marclamberti.com/blog/airflow-on-kubernetes-get-started-in-10-mins/)

Once the Airflow Kubernetes is configured correctly, I may want to migrate my current tasks (which used Python operators) to the `KubernetesPodOperator`, including dbt transformation task.

Once Airflow is running in Kubernetes and the data pipeline is working as expected, the next step would be to implement alerts and notifications. Currently, we have to log into the Airflow UI to check if the pipeline has run successfully. It would be better to have alerts and notifications sent directly to a chat application (e.g. Slack).
