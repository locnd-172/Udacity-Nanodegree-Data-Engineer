!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
airflow connections get aws_credentials -o json 

[{"id": "1", 
"conn_id": "aws_credentials",
"conn_type": "aws", 
"description": "", 
"host": "", 
"schema": "", 
"login": "AKIAQ6OAX3UXZOHQU4PZ", 
"password": "Q/t6w40HY1RNXoSPSi0YyDk1GKzqzVQ4lcRiilgw", 
"port": null, 
"is_encrypted": "False", 
"is_extra_encrypted": "False", 
"extra_dejson": {}, 
"get_uri": "aws://AKIAQ6OAX3UXZOHQU4PZ:Q/t6w40HY1RNXoSPSi0YyDk1GKzqzVQ4lcRiilgw"
}]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri 'aws://AKIAQ6OAX3UXZOHQU4PZ:Q/t6w40HY1RNXoSPSi0YyDk1GKzqzVQ4lcRiilgw'
#
#
# TO-DO: run the follwing command and observe the JSON output: 
airflow connections get redshift -o json

[{"id": "3", 
"conn_id": "redshift", 
"conn_type": "redshift", 
"description": "", 
"host": "default-workgroup.065365597487.us-east-1.redshift-serverless.amazonaws.com", 
"schema": "dev", 
"login": "awsuser", 
"password": "Pidolphinzxc_1", 
"port": "5439", 
"is_encrypted": "False", 
"is_extra_encrypted": "False", 
"extra_dejson": {}, 
"get_uri": "redshift://awsuser:Pidolphinzxc_1@default-workgroup.065365597487.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://awsuser:Pidolphinzxc_1@default-workgroup.065365597487.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket locnd15-sean-murdock
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines