# Itunes_Ingestion_Pipeline

Prerequisites:
Docker Desktop

Kind

Kubectl

Code editor

Docker CLI (optional)

DockerHub credentials

Helm

AWS account with S3 and EMR access

One of the most important aspects about running a company is to maintain data. It has to be accurate and available immediately. Big Tech companies or any data driven company for that matter can’t just work on spreadsheets and expect to float. They need a database and with that data pipelines to fill these databases.

The most famous and efficient way to create a Data Pipeline is using Apache Airflow. Airflow is a task scheduler wherein we can define dependencies between tasks, type of tasks, workflow etc. Airflow works very well with AWS EMR clusters to submit spark jobs. Now, usually when companies create Data Pipelines, they would want to create a template for every new DAG that they want to create. Therefore, they adopt containerized Airflow which they host on EKS. 

I decided to containerize Airflow locally using a Kind cluster. We first installed Helm. It is a package manager which helps in bootstrapping Apache Airflow on a Kubernetes Cluster. After installation, we create the cluster, build the docker image, push it to the cluster, deploy the Airflow service and that's it, our DAG shows up on Airflow’s UI. 

Task 1: pull_and_store_itunes_data

In this task, we use the itunes api to pull 200 records for each artist that we provide to the DAG. We then store these records (JSON format) separately in each row of a Pandas Dataframe and export it as parquet in s3.

Task 2: create_emr_cluster

In this task, we simply create an EMR cluster on AWS with the following configuration -

job_flow_overrides = {
    'Name': 'Airflow-EMR',
    'ReleaseLabel': 'emr-5.29.0',
    "LogUri": "s3://dag-emr-logs/",
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker nodes',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

Task 3: wait_for_cluster_to_be_ready

In this task, we just simply wait for the cluster to get ready after creation.

Task 4: add_transform_raw_itunes

In this task, we provide a step to the EMR cluster, which converts the JSON blobs in our raw parquet file into columns with properly arranged and formatted data. 

Task 5: wait_for_transform_raw_itunes

In this task, we simply wait for the cluster to complete the execution of the step.

Task 6,7,8: add_[entity]_split_step

In these tasks we provide the cluster with a step that would split our Itunes Flat table into 3 tables - artists, tracks and collections.

Task 9,10,11: wait_for_[entity]_split_step

In these tasks, we simply wait for the cluster to complete the execution of the steps.

Task 12: terminate_emr_cluster

After all the tasks are completed successfully, the DAG will execute the last task to terminate the active EMR cluster.

Setting up Kubernetes Cluster:

Step 1: Install Helm using the following -

```bash
brew install helm
```

Step 2: Create a new folder and name it my-airflow-project or anything of your choice. Create another folder in my-airflow-project named dags. Place you DAG.py in that folder. Now in the main folder create a Dockerfile containing the following -

```Dockerfile
FROM apache/airflow
RUN pip install apache-airflow
COPY . .
```

Step 3: Login to docker using - docker login

Step 4: Create Kind cluster using the following -

```bash
kind create cluster --image kindest/node:v1.21.1
```

Step 5: Add the apache/airflow repo to the kind cluster using Helm chart -

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

Step 6: Create Namespace and Release

Step 7: Import your release to the cloned Git repo on your kind cluster -

```bash
helm install $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE
```

Step 8: Build docker image -

```bash
docker build --pull --tag my-dags:0.0.1 .
```

Step 9: Push the docker image to Kind cluster -

```bash
kind load docker-image my-dags:0.0.1
```

Step 10: Deploy the Airflow application to Kind cluster -

```bash
helm upgrade $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE \    --set images.airflow.repository=my-dags \
    --set images.airflow.tag=0.0.1
```

Step 11: Port forward Airflow application service to localhost:8080 -

```bash
kubectl port-forward svc/$RELEASE_NAME-webserver 8080:8080 --namespace $NAMESPACE
```

Before we run our DAG we need to enter two connections in the connections menu under admin. ‘Aws_default’ and ‘emr_default’ which is basically aws key and aws secret key. 

After the connections are set we go ahead and trigger the DAG.



