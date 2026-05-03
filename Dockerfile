<<<<<<< HEAD
FROM apache/airflow:3.2.1
=======
FROM apache/airflow:3.1.7
>>>>>>> 8b1edd99b6beedd76b6af90bffb520cfee52c7bd

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-git
