# track password for logging in airflow 

docker exec -it airflow bash -lc 'echo $AIRFLOW_HOME; cat $AIRFLOW_HOME/standalone_admin_password.txt'

# rebuild command line 

docker compose up -d --build
