#!/bin/bash
# This script is used to build and push a temporary image to AWS ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
aws ecr delete-repository --repository-name $MJ_STATUS_ECR_ETL --no-force
var=`aws ecr create-repository --repository-name $MJ_STATUS_ECR_ETL --query 'repository.repositoryUri' --output text`
docker build -t $MJ_STATUS_ECR_ETL .
docker tag $MJ_STATUS_ECR_ETL:latest $var:latest
docker push $var:latest