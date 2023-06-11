# create lambda function with image
aws iam create-role --role-name $MJ_STATUS_LAMBDA_ROLE --assume-role-policy-document file://roles/trust-policy.json
var=`aws ecr describe-repositories --repository-names $MJ_STATUS_ECR_ETL --query 'repositories[0].repositoryUri' --output text`
aws lambda create-function --function-name $MJ_STATUS_ECR_ETL --package-type Image --code ImageUri=$var:latest --role arn:aws:iam::$AWS_ACCOUNT_ID:role/$MJ_STATUS_LAMBDA_ROLE