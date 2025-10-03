## track password for logging in airflow 
docker exec -it airflow bash -lc 'echo $AIRFLOW_HOME; cat $AIRFLOW_HOME/standalone_admin_password.txt'

## rebuild command line 
docker compose up -d --build

## Start up airflow on Command Line
docker exec -it airflow bash -lc "airflow db check || airflow db init; AIRFLOW__WEBSERVER__WEB_SERVER_SSL=False airflow webserver -H 0.0.0.0 -p 8080 --debug"

## Enter airflow container 
docker exec -it -u root airflow bash

## Set up git from command line
git init
git add (project name)
git commit -m ("content")
git remote add origin (SSH)
git branch -M main
git push -u origin main  or git push -u origin main --allow-unrelated-histories
 
