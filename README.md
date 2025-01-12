# apache-spark-project
A distributed analysis of Google's cluster data using Apache Spark.

## Project structure

- [`notebooks/`](notebooks/): Jupyter notebooks used for data exploration and analysis. Note that in these notebooks, we run data analysis on a sample of the data, not the entire dataset. Another important note is that in the notebooks, we are using RDDs to perform the analysis and not DataFrames. However, in the final implementation, we use DataFrames.

- [`data/`](data/): Contains the sample data used in the notebooks.

- [`src/`](src/): Source code used mainly in the cloud environment. 
    - [`src/overall_analysis/`](src/overall_analysis/): Contains the code for the overall analysis of the data. Each file in this directory corresponds to a spark job for specific question in the project description. Instructions on how to run these jobs can be found later in this README.
    - [`src/stream-processing/`](src/stream-processing/): Contains the code for the stream processing part of the project.
        - [`src/stream-processing/producer.py`](src/stream-processing/producer.py): The producer script: reads the data from the Google Cloud Storage bucket and sends it to the designated Kafka topics. In our case we decided to simulate the stream processing by sending the data related to the `task_events` and `job_events` tables to Kafka topics (one topic for each table).
        - [`src/stream-processing/spark_streaming_job.py`](src/stream-processing/spark_streaming_job.py): Spark streaming job: reads the data from the Kafka topics and performs some analysis on it in real-time. In our case, we are counting the number of jobs and tasks per scheduling class and per event type.

- [`cloud/`](cloud/): Contains all the necessary files to deploy the project on the cloud.
    - [`cloud/terraform/`](cloud/terraform/): Contains the terraform scripts to provision the infrastructure on Google Cloud Platform.
    - [`cloud/k8s/`](cloud/k8s/): Contains the Kubernetes manifests needed in the deployment of the project.
    - [`cloud/scripts/`](cloud/scripts/): Contains the scripts to deploy the project on the cloud including provisioning the infrastructure, installing spark, kafka and also some helper scripts to do the necessary configurations and run the spark jobs.
    - [`cloud/dashboards/`](cloud/dashboards/): Contains a Grafana dashboard to visualize the results of the stream processing job.

- [`analysis_results/`](analysis_results/): Contains the results of the analysis jobs that we conducted in the cloud environment.

## Running the project

### Prerequisites

- Google Cloud Platform account
- Google Cloud SDK
- A Project on Google Cloud Platform with a valid ID
- Terraform
- kubectl
- Helm

### Provisioning the infrastructure

1. Setting up gcloud configuration: (Authentication and Service Account)

    Set the default project

    ```bash
    gcloud config set project <PROJECT_ID>
    ```
    
    Run the setup script

    ```bash
    ./cloud/scripts/setup.sh <PROJECT_ID>
    ```

    This script will authenticate you with gcloud and create a service account in your project. It will also download the service account key and save it in the `~/terraform-key.json` file.

2. Provisioning the infrastructure:

    ```bash
    ./cloud/scripts/deploy.sh
    ```

    This script will provision the infrastructure on Google Cloud Platform using Terraform. It will create a GKE (Google Kubernetes Engine) cluster with 4 nodes of type `e2-standard-2`. To change the number of nodes or the machines type, you can modify this section in the [`cloud/terraform/main.tf`](cloud/terraform/main.tf#L19):

    ```shell
    resource "google_container_cluster" "my_cluster" {
        // ...
        initial_node_count = 7
        node_config {
            machine_type = "e2-standard-2"
        }
        // ...
    }
    ```

3. Get the credentials

    ```bash
    ./cloud/scripts/get-creds.sh
    ```
    This script will get the credentials of the GKE cluster and set the current context to the GKE cluster. This is necessary to be able to use `kubectl` & `helm` to interact with the cluster.

4. Installing a monitoring stack (Prometheus and Grafana):

    ```bash
    ./cloud/scripts/apply-monitor.sh
    ```
    
    This script will install Prometheus and Grafana on the GKE cluster using Helm. It will display the Grafana URL at the end of the script.

5. Installing Spark with GCS connector:
    
    First we need to create a service account for Spark to access the Google Cloud Storage bucket. Run the following script:

    ```bash
    ./cloud/scripts/configure-gcs.sh <PROJECT_ID>
    ```
    
    This script will create a service account for Spark and download the service account key in the `cloud/k8s/secrets/spark-gcs-key.json` file.

    Then, we can install Spark with the GCS connector:
    ```bash
    ./cloud/scripts/apply-gcs-spark.sh
    ```
    This script will install Spark with the GCS connector on the GKE cluster using Helm. It will also download a necessary jar file (the GCS connector) and send it to the Spark driver pod.

### Running the overall analysis

1. Run the spark jobs

    All the jobs scripts are located in the `src/overall_analysis/` directory. To run a job, all you need to do is to copy the name of the file wanted and run the following command:

    ```bash
    ./cloud/scripts/jobs/submit_analysis_job.sh <JOB_NAME>
    ```

    For example, to run the job that answers the first question in the project description, you can run the following command:

    ```bash
    ./cloud/scripts/jobs/submit_analysis_job.sh q1_machines_by_cpu.py
    ```

2. Get the results

    In order to get the results of the job, you can run the following command:

    ```bash
    kubectl cp spark-master-0:/opt/bitnami/spark/results ./results
    ```

    This command will copy the results of the job to the `results/` directory.

    The results of our analysis can be found in the [`analysis_results/`](analysis_results/) directory.

### Running the stream processing

    In this part, we will simulate the stream processing by sending the data related to the `task_events` and `job_events` tables to Kafka topics (one topic for each table). Then, we will read the data from these topics and perform some analysis on it in real-time.

1. Install Kafka

    ```bash
    ./cloud/scripts/apply-kafka.sh
    ```

    This script will:

    - install Strimzi (Kafka operator) on the GKE cluster on the `kafka` namespace.
    - create a Kafka cluster on the same namespace.
    - create two topics: `task-events` and `job-events`.
    - Install kafdrop (a web UI for Kafka) on the same namespace to visualize the topics and messages.
    - At the end of the script, it will display the kafdrop URL.

2. Run the producer

    ```bash
    ./cloud/scripts/apply-producer.sh
    ```

    This script will create a pod that reads the data from the Google Cloud Storage bucket (Two tables: `task_events` and `job_events`) and sends it to the corresponding Kafka topics.

    The [producer script](src/stream-processing/producer.py) accepts the following arguments:

    - `START_FILE`: The file to start reading from (File 0 by default).
    - `NUM_FILES`: The number of files to read (1 by default).
    - `SPEED_FACTOR`: This parameter controls how fast the historical data is replayed into the Kafka topics. It works by compressing the original time intervals between events. For example:

        - With `SPEED_FACTOR=60`, 1 minute of historical data is replayed in 1 second
        - With `SPEED_FACTOR=3600`, 1 hour of historical data is replayed in 1 second
        - With `SPEED_FACTOR=432000`, 5 days of historical data is replayed in 1 second (as used in our configuration)

        A higher speed factor means faster replay of historical data, while a lower speed factor provides a replay speed closer to real-time.

    In the script [`apply-producer.sh`](cloud/scripts/apply-producer.sh), we modified the `NUM_FILES` to 20 and the `SPEED_FACTOR` to 432000 (5 days = 1 sec) to simulate the stream processing in a faster way.

3. Run the stream processing job

    ```bash
    ./cloud/scripts/jobs/submit-stream-job.sh
    ```

    This script will submit the Spark streaming job to the Spark cluster. The job reads the data from the Kafka topics and performs some analysis on it in real-time. In our case, we are counting the number of jobs and tasks per scheduling class and per event type.

    The jobs results are then written in the `/metrics` endpoint of the `port 8081` of the Spark master pod. They are then scraped by Prometheus and visualized in Grafana.

4. Visualize the results

    To visualize the results of the stream processing job, go to the Grafana dashboard and import the dashboard [`cloud/dashboards/stream-dashboard.json`](cloud/dashboards/stream-dashboard.json) using the Grafana UI.

    Once the dashboard is imported, you can see the results of the stream processing job in real-time.

    Note that the results are automatically timestamped by prometheus, so you can see the evolution of the results over time. We can timestamp the results manually in the Spark job if needed, however, we decided to keep it simple for this project.

### Cleaning up

To clean up the resources created by the project, you can run the following script:

```bash
./cloud/scripts/destroy.sh
```

    









