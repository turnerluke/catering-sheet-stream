#!/bin/bash

URL_STRING=".dkr.ecr.us-east-1.amazonaws.com"
CONTAINER_STRING=$(basename "$(pwd)")
IMAGE_STRING="latest"
ECR_IMAGE_URI="$AWS_ACCOUNT_ID$URL_STRING/$CONTAINER_STRING:$IMAGE_STRING"
LAMBDA_EXECUTION_ROLE_ARN="arn:aws:iam::591851667748:role/lambda_basic_execution"

# Log in to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID$URL_STRING"

# Remove previous local images to save space
docker rmi "$AWS_ACCOUNT_ID$URL_STRING/$CONTAINER_STRING" "$CONTAINER_STRING"

# Build image
docker build --tag "$CONTAINER_STRING" .

# Run the Docker image locally
docker run -d --name "$CONTAINER_STRING" "$CONTAINER_STRING"

# Execute tests against the running container
# Replace the following command with your test command
docker exec "$CONTAINER_STRING" python -m pytest

# Stop and remove the local container
docker stop "$CONTAINER_STRING"
docker rm "$CONTAINER_STRING"

# Check if ECR repository exists, create if it doesn't
ECR_REPOSITORY_NAME="$CONTAINER_STRING"
ECR_REPOSITORY_EXISTS=$(aws ecr describe-repositories --repository-names "$ECR_REPOSITORY_NAME" --query 'repositories' --output text 2>/dev/null)
if [ -z "$ECR_REPOSITORY_EXISTS" ]; then
    echo "ECR repository $ECR_REPOSITORY_NAME does not exist"
    echo "Creating ECR repository $ECR_REPOSITORY_NAME"
    aws ecr create-repository --repository-name "$ECR_REPOSITORY_NAME"
fi

# Tag and push to AWS ECR
docker tag "$CONTAINER_STRING:latest" "$ECR_IMAGE_URI"
docker push "$ECR_IMAGE_URI"

# Check if Lambda function exists, create if it doesn't
LAMBDA_FUNCTION_NAME="$CONTAINER_STRING"
LAMBDA_FUNCTION_EXISTS=$(aws lambda list-functions --query "Functions[?FunctionName=='$LAMBDA_FUNCTION_NAME'].FunctionName" --output text)
if [ -z "$LAMBDA_FUNCTION_EXISTS" ]; then
    echo "Lambda function $LAMBDA_FUNCTION_NAME does not exist"
    echo "Creating Lambda function $LAMBDA_FUNCTION_NAME"
    aws lambda create-function --function-name "$LAMBDA_FUNCTION_NAME" --code "ImageUri=$ECR_IMAGE_URI" --role "$LAMBDA_EXECUTION_ROLE_ARN" --package-type Image
else
    echo "Updating Lambda function $LAMBDA_FUNCTION_NAME"
    aws lambda update-function-code --function-name "$LAMBDA_FUNCTION_NAME" --image "$ECR_IMAGE_URI"
fi
