This project uses Airflow to orchestrate the download of daily Shopify configurations contained into CSV files from a public S3 bucket, the transformation of the data and the insertion into a PostgreSQL instance.

## Prerequisites

- Docker and Docker Compose installed
- Python installed

## Getting Started

### Clone the Repository

Start by cloning the repository and navigating into the project directory:

```bash
git clone git@github.com:MehdiKenana1/algolia_assignment.git
cd algolia_assignment
```

To start services and notably airflow, run the following command:

```bash
docker compose up --build
```

### Running the DAG

Access the Airflow web interface by opening your web browser and go to http://localhost:8080.

You can manually trigger the <em>etl_s3_to_postgres</em> dag from the Airflow UI.

### Running unit tests

Create a virtual environment and activate it:

```bash
python3 -m venv venv
source venv/bin/activate
```

Then run the following command:

```bash
pytest tests/
```
