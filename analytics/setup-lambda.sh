# create lambda function with image
aws iam create-role --role-name lambda-$MJ_STATUS_ECR_ANALYTICS-role --assume-role-policy-document file://roles/trust-policy.json
var=`aws ecr describe-repositories --repository-names $MJ_STATUS_ECR_ANALYTICS --query 'repositories[0].repositoryUri' --output text`
aws lambda create-function --function-name $MJ_STATUS_ECR_ANALYTICS --package-type Image --code ImageUri=$var:latest --role arn:aws:iam::$AWS_ACCOUNT_ID:role/lambda-$MJ_STATUS_ECR_ANALYTICS-role