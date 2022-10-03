# master-thesis-code

airflow db init

airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org

cp download_rocket_launches.py ~/airflow/dags/

airflow webserver

airflow scheduler

spark.sql.execution.arrow.enabled=true


# ┌─────── minute (0 - 59)
# │ ┌────── hour (0 - 23)
# │ │ ┌───── day of the month (1 - 31)
# │ │ │ ┌───── month (1 - 12)
# │ │ │ │ ┌──── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │      7 is also Sunday on some systems)
# * * * * *


    0 * * * * = hourly (running on the hour)

    0 0 * * * = daily (running at midnight)

    0 0 * * 0 = weekly (running at midnight on Sunday)

Besides this, we can also define more complicated expressions such as the following:

    0 0 1 * * = midnight on the first of every month

    45 23 * * SAT = 23:45 every Saturday
#First pipeline: 
trigger scraper
Store raw file in s3
Trigger pyspark scripts/ together with
Trigger geoapify scripts
save to db
