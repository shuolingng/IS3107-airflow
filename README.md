# IS3107 Data Engineering Project
Project is done by National University of Singapore (NUS) Business Analytics Undergradautes:
- Shuo Ling
- Zandra Tay
- Pearlyn Liu
- Wan Xin
- Bryan Lee Jie Long

This README provides a guide for setting up Apache Airflow for local development using Docker and Docker Compose. Our project is meant to extract and tranform job listing data from multiple online sources, loading the data in Google Cloud Platform, and subsequently synthesizing our data into visually insightful representations as well as input for a resume parsing machine learning model.

## Prerequisites
Docker: Ensure Docker is installed on your system. Docker is used to create and manage containers for the Airflow services.
Docker Compose: Ensure Docker Compose is installed. It simplifies the process of managing multi-container Docker applications.

## Setup Instructions

1. Clone the Repository: Start by cloning the repository to your local machine.
```bash
 git clone https://github.com/shuolingng/IS3107.git
```
2. Navigate to the Repository: Open a terminal and navigate to the root directory of the cloned repository.
 ```bash
   cd IS3107
 ```
3. Install Docker and Docker Compose: If not already installed, follow the official guides to install Docker and Docker Compose on your system.

4. Build or Rebuild Services:
Before starting the Airflow services, ensure that any custom Docker images are built or rebuilt to include the latest changes. In the terminal, type:
 ```bash
docker compose build
```
This command processes the Dockerfile and docker-compose.yaml configurations to build or update the necessary Docker images for your Airflow setup.

6. Start Airflow Using Docker Compose: Use the docker-compose up command to start all defined services in containers. This command utilizes the docker-compose.yaml file located in the repository's root directory.
```bash
docker-compose up
```

6. Access the Airflow Web Interface: Once the containers are running, the Airflow web interface should be accessible at http://localhost:8080. Here, you can manage workflows and monitor DAG execution.

7. Explore and Modify the DAGs: The dags folder contains Directed Acyclic Graphs (DAGs) definitions. You can add new DAG files or modify existing ones to define your workflows.

Note: Additional Python Dependencies: If your DAGs require additional Python packages, add them to the requirements.txt file. You can rebuild the Docker image or mount the requirements.txt file in docker-compose.yaml to install these dependencies when the container starts.
Logs and Debugging: The logs folder contains logs for DAG runs. Review these logs if you encounter issues with your workflows.

## Troubleshooting
It may take a while to access Airflow even after docker container has been started up.
If you cannot access Airflow at `localhost:8080`, it may be because `localhost:8080` is being used, please follow these steps to diagnose the issue:

1. Ensure all Docker containers are running: `docker ps`. Pause other containers that may be using `localhost:8080`
2. Check the logs of the Airflow webserver container for errors: `docker logs <container_name_or_id>`.
3. Verify the port mapping in `docker-compose.yaml` matches `"8080:8080"` under the `airflow-webserver` service.
4. Check if your firewall or antivirus is blocking port 8080.
5. Test port accessibility using `curl http://localhost:8080` or `netstat -an | grep 8080`.

## Shut down and turn off

1. Shut Down Airflow Containers: To stop the Airflow containers that are running in the background, open a terminal and navigate to the root directory of your Airflow setup (where your docker-compose.yaml file is located). Then run the following command:
```bash
docker-compose down
```
This command stops and removes the containers, networks, volumes, and images created by docker-compose up.

2. Quit Docker App


## Notes and Warnings
This setup is intended for local development and testing. It is not suitable for a production environment.
The provided docker-compose.yaml file and other configuration files are optimized for development use. Adjust these configurations as necessary for your specific use case.
For more detailed information on Apache Airflow, Docker, and Docker Compose, refer to their official documentation:

[Apache Airflow Documentation](https://airflow.apache.org/docs/)

[Docker Documentation](https://docs.docker.com)

[Docker Compose Documentation](https://docs.docker.com/compose/)
