# track username and password for logging in airflow 

docker logs airflow --since=30m | Select-String -Pattern "username|password|admin"
