# Climate Change Data Pipeline Project

## Project Overview
This project aims to integrate structured, semi-structured, and unstructured data sources into multiple analytical pipelines to assess the socioeconomic and environmental impacts of climate change. By leveraging big data technologies and machine learning models, the project provides valuable insights into climate trends, pollution effects, and public sentiment.

## Repository Structure and Folder Explanations

Each directory contains a readme explaining its concepts. In here, we present the general structure of the repository.

> **Note**: For the sake of simplicity in execution by the course lecturers, we have included an environment variables file (.env) in the project repository. However, be aware that this poses a significant security risk and should never be done in a real-world production environment.


```python

PROJECT/
│
├── dags/                               # Apache Airflow dags
├── duckdb_data/                        # Duckdb databases
├── exploitation_zone/                  # Exploitation Zone files
├── frontend/                           # Frontend related files
    ├── app.py                              # Main file to initialize the frontend
├── landing_zone/                       # Landing_zone
    ├── batch/                              # Folder containing batch ingested data (Delta-Lake).
        ├── .../                                # One different folder for each batch ingestion data source.    
    ├── streaming/                          # Folders containing real-time ingested data.
        ├── .../                                # One different json file per each real-time data source. 
├── logs/                               # Apache Airflow logs
├── plugins/                            # Apache Airflow plugins
├── TrustedZone/                        # Trusted Zone Files
├── utils/                              # Folder Containing useful code for the entire project.
    ├── batch_ingestion/                    # Folder containing management code related with batch ingestion protocols.
        ├── delta_manager.py                    # Class to centralize the delta spark connection and usage.
        ├── api_manager.py                      # Class to centralize the interaction with all APIs.
        ├── batch_ingestion.py                  # Class to centralize the batch ingestion protocols.
    ├── real_time_ingestion/                # Folder containing management code related with real-time ingestion protocols.
        ├── kafka_consumers/                    # Folder containing a kafka consumer per each real-time ingested data source.
        ├── kafka_producers/                    # Folder containing a kafka producer per each real-time ingested data source.
            ├── .env/                            # ENVIRONMENT FILE THAT SHOULD NOT BE IN THE REPOSITORY, WE INCLUDE IT BY MEANS OF SIMPLICITY.
    ├── temp/                               # Folder to store temporal data during the ingestion process, which is automatically cleaned.
    ├── cities.json                         # Json file containing several cities toghether with its coordinates.
├── .env                                # ENVIRONMENT FILE THAT SHOULD NOT BE IN THE REPOSITORY, WE INCLUDE IT BY MEANS OF SIMPLICITY.
├── README.md                           # Project documentation and guidelines.
├── requirements.txt                    # Python dependencies needed for the project Docker image.
├── .gitignore                          # Specifies files and folders to ignore in version control.              
├── docker-compose.yml                  # Orchestrates docker containers and sets up other configurations.
├── Dockerfile.airflow                      # Creates the image for Apache Airfow container.
├── Dockerfile.streamlit                    # Creates the image for the streamlit (frontend) app.

```


## Execution Instructions

To get your project up and running with Docker, follow these steps:

1. **Ensure Docker and Docker Compose are installed**  
   You need Docker (and Docker Compose) installed on your machine to build and run the container. The easiest way to set it up is by installing [Docker Desktop](https://www.docker.com/products/docker-desktop), which includes both Docker and Docker Compose.

2. **Start the Docker Daemon**  
   - If you're using Docker Desktop, open the application to start the Docker Daemon.  
   - If you're using Docker without Docker Desktop, make sure the Docker Daemon is running by executing:  
     ```bash
     sudo systemctl start docker
     ```

3. **Navigate to the Project Directory**  
   Open your terminal or command prompt and navigate to the root directory of the project where the `docker-compose.yml` file is located. This ensures that Docker Compose can find the necessary configuration to build and run the container.

4. **Build the Docker Image**  
   Run the following command to build the project Docker main container (containing all containers) pulling all images + creating the specific one in the `Dockerfile`:
   ```bash
   sudo docker compose build  # Builds images using Dockerfiles + builds the containers of the project
   ```

    This command will pull the required base images, install dependencies, and prepare the containers according to the project’s setup.

    > **Note**: This is a CPU & Disk Memory consuming step. Depending on your machine, this process may take some time. Make sure to allow Docker to (at least) use up to 20GB of disk memory and as much RAM as possible, the project may be quite big.

5. **Start the Docker Container**
    After the main container is built, run the following command to run it:
    ```bash
    sudo docker compose up  # Starts the containers and runs the application
    ```

    > **Note**: The project can take a bit to initialize, wait until all containers are initialized to start interacting with them. Try refreshing GUI pages in case they are still not initialized.


6. **(Interacting via Streamlit) Access the Frontend Interface**  
    Once the Docker containers are running, open your web browser and navigate to **[http://0.0.0.0:8501](http://0.0.0.0:8501)** to launch the frontend. This interface allows you to interact with the project in an intuitive and user-friendly way.

    > **Note**: Depending on your system, you may also access it via **[http://localhost:8501](http://localhost:8501)**.

7. **(Interacting via Apache Airflow ) Access Airflow User Interface**

    Once the Docker containers are running, open your web browser and navigate to **[http://0.0.0.0:8083](http://0.0.0.0:8083)** to launch the Apache Airflow GUI.

    Log in with the following credentials:
    - Username: airflow
    - Password: airflow

    Execute the DAGs to manage the data flow of the pipeline.

8. **Clean Up**
    If you need to stop the running container, you can use:

    ```bash
    sudo docker compose down  # Stop and remove the container
    ```

    Make sure to free the space once you don't want the project anymore by using:

    ```bash
    docker compose down --volumes
    ```


## Authors
- Anna Monsó
- Eduardo Tejero
- Joan Acero