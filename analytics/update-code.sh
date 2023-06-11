aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
var=`aws ecr describe-repositories --repository-names $MJ_STATUS_ECR_ANALYTICS --query 'repositories[0].repositoryUri' --output text`
docker build -t $MJ_STATUS_ECR_ANALYTICS .
docker tag $MJ_STATUS_ECR_ANALYTICS:latest $var:latest
docker push $var:latest
aws lambda update-function-code --function-name $MJ_STATUS_ECR_ANALYTICS --image-uri $var:latest