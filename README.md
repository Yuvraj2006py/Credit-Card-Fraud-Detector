# Fraud Detection ETL Pipeline

This repository contains a production‑style ETL pipeline for
credit‑card fraud detection. The pipeline ingests raw transaction
records, cleans and enriches the data, trains a simple logistic
regression model to identify potentially fraudulent transactions and
writes the results into a PostgreSQL database. Everything is
orchestrated via [Apache Airflow](https://airflow.apache.org/) and
containerised with Docker Compose so you can run the entire stack with
a single command.

## Project Structure

```
fraud_pipeline/
├── dags/                  # Airflow DAG definitions
│   └── fraud_etl_dag.py   # Main DAG for ETL and modelling
├── scripts/               # Modular Python scripts used by the DAG
│   ├── extract.py         # Extracts the raw dataset
│   ├── transform.py       # Cleans and engineers features
│   ├── fraud_model.py     # Trains the logistic regression model
│   └── load.py            # Loads predictions into Postgres
├── data/                  # Dataset directory
│   └── creditcard.csv     # Raw credit card transactions (placed here)
├── docker-compose.yml     # Defines Airflow and Postgres services
├── requirements.txt       # Python dependencies
└── README.md              # You are here
```

## Dataset

The pipeline uses the well‑known credit card fraud detection dataset
discussed in the [GeeksforGeeks article](https://www.geeksforgeeks.org/machine-learning/ml-credit-card-fraud-detection/)【175464801668524†L988-L1000】.
The data contains 284 807 transactions with 30 anonymised features
(`V1`…`V28`), the transaction amount and a label (`Class`) indicating
whether the transaction is legitimate (`0`) or fraudulent (`1`)【175464801668524†L988-L1000】.  It has been
downloaded for you and placed under `data/creditcard.csv`.

## Setup Instructions

Follow these steps to get the pipeline running on your local machine.

### 1. Prerequisites

- **Docker Desktop** — install from [docker.com](https://www.docker.com/).  You
  should be able to run `docker compose version` to verify your
  installation.
- (Optional) **Python 3.10+** — not required for the Docker setup but
  useful for exploring the data outside of the pipeline.

### 2. Clone the repository

Clone this project to your local machine and navigate into it:

```bash
git clone <repository-url>
cd fraud_pipeline
```

Ensure that `data/creditcard.csv` is present. If you wish to use a
different dataset, replace the CSV in the `data/` directory and keep
the header names the same.

### 3. Install dependencies (optional)

If you plan on running the scripts locally (outside of Docker), create
a virtual environment and install Python dependencies:

```bash
python -m venv .venv
.venv/bin/activate
pip install -r requirements.txt
pip install -r requirements.txt -c https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt

```

### 4. Start the services

Use Docker Compose to start PostgreSQL and Airflow. The first run will
download Docker images and initialise the Airflow database.

```bash
docker compose up
```

This command will:

- Launch a **PostgreSQL** container listening on port `5432` with
  credentials `airflow:airflow` and database `frauddb`.
- Launch an **Airflow** container that runs both the scheduler and
  webserver, exposes the UI on <http://localhost:8080>, and mounts
  your local `dags/`, `scripts/` and `data/` directories into the
  container.
- Create an initial Airflow admin user with username/password
  `admin/admin`.

Once both services are up, open your browser at
<http://localhost:8080> and log in with `admin`/`admin`.  In the
Airflow UI, locate the `fraud_etl_pipeline` DAG and turn it on.  On
each run the DAG will process the dataset and load the results into
the `frauddb` database.

### 5. Inspecting results

After the DAG has completed, connect to the Postgres database using
your favourite client (e.g. `psql`, pgAdmin, DBeaver) with the
credentials above.  The pipeline writes to a table named
`transactions`.  Example queries:

```sql
-- Count predicted fraudulent transactions
SELECT COUNT(*) FROM transactions WHERE "FraudPrediction" = 1;

-- Compare the number of actual fraud cases to predictions
SELECT Class, FraudPrediction, COUNT(*)
FROM transactions
GROUP BY Class, FraudPrediction
ORDER BY Class, FraudPrediction;
```

You can also build dashboards in Tableau or Power BI by connecting
directly to the `frauddb` Postgres instance on `localhost:5432`.  A
typical dashboard might show the proportion of fraudulent transactions,
fraud rate by hour of the day or fraud rate by transaction amount.

## Architecture Diagram

The following illustrates the overall flow of data through the system:

```
            +---------------------+
            |  Raw CSV (data/)    |
            +----------+----------+
                       |
                       v
                 [Extract Task]
                       |
                       v
                 [Transform Task]
                       |
                       v
             [Fraud Model Task]
                       |
                       v
                  [Load Task]
                       |
                       v
            +---------------------+
            | PostgreSQL (frauddb)|
            +---------------------+
                       |
                       v
              +-------------------+
              | Dashboards / SQL  |
              +-------------------+
```

## Notes

- This project is intended as a learning example. In production
  fraud‑detection systems you would likely use much more complex
  models, incorporate real‑time streaming data, handle class
  imbalance, and perform model evaluation on separate test sets.
- The schedule interval is set to `@daily` to simulate a daily batch
  ingestion. Although the provided dataset is static【175464801668524†L988-L1000】, scheduling the
  pipeline demonstrates how one would process new data in a real
  environment.
- To update the pipeline for real‑time or streaming use cases you
  could replace the file‑based handoff between tasks with a shared
  object store (e.g. S3) or message queue (e.g. Kafka) and adjust the
  schedule accordingly.

Happy analysing!