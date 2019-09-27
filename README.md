# About

Goal is to capture each sources measurable value with respect to its contribution to the person formations in the identity graph through a readable scorecard table.


#### MapReduce: 
- Mapper
- Reducer

Mapper.py takes the dataset and converts it to another set of data, where individual elements are broken down into tuples of key,value pairs intermediate outputs. reducer.py takes the output from mapper.py as the input.In the reducer, a string is parsed from mapper.py as a key, value pair and kept as a set(). Combining the data tuples into smaller set, this created the final output. 

### Technologies Used
- Jenkins
- Hadoop
- Docker
- Airflow
- BigQuery
 

## Docker Installation

Click [here](https://hub.docker.com/editions/community/docker-ce-desktop-mac) to download Docker

Move Docker to applications folder and Double-click Docker.dmg to start the install process.

When the installation completes and Docker starts, the whale in the top status bar shows that Docker is running, and accessible from a terminal.

Run ``` docker version``` in terminal to check that you have the latest release installed.

- Add Mapper into DOCKER ---> COPY **mapper.py**
- Add Reducer into DOCKER ---> COPY **reducer.py**

## Airflow

Airflow is a platform to programmatically author, schedule and monitor data pipelines. The Airflow scheduler, while following the specified dependencies of the DAGs created, executes your tasks. 

#### run_touchpoint_source.py

1) ```import abilitec_build_utils as abu``` in python script as library

2) Pass in specified parameters into
Abu.run_dataproc_hadoop_streaming_with_most_recent_source_inputs()

3) Save File
4) Add file into DOCKER ---> COPY **run_touchpoint_source.py**
---

> **Path Structure:** gs://<bucket>/<prefix(DATA/US)>/<unique dataset name>/yyyymmdd/partfile


Airflow can have multiple dependencies and no cycles. The scheduler will take care of the order in which it runs --->  do this with python script ```source_scorecard_standalone.py```

#### Creating an airflow DAG in scorecard_standalone.py

1) Create/Rename --> DOCKER_IMAGE_NAME, SOURCE_NAMES, COMMAND
2) Create DAG:

> with DAG(DAG_NAME, default_args=dict(D), params={}, concurrency=16, catchup=False, schedule_interval=None, user_defined_filters=dict(recursive_render=recursive_render)) as dag:

3) Create specific task:
> task_name = docker_run_operator('TASK_NAME','DOCKER_IMAGE','COMMAND')

4) Add file into DOCKER ---> COPY **scorecard_standalone.py**


## BigQuery

Create BigQuery layout:
- sourceScorecard_bq_layout.json can be used as a template
- If needed, use **fieldnames.py** to create Field Names for long list of values



## Contributions
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

### notes
Due to confidential reasons:
- Data that includes source names has been removed from files
- Jenkins file not included
- run_touchpoint_source.py not included
