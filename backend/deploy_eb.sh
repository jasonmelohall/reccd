#!/bin/bash

# Deployment script for AWS Elastic Beanstalk

echo "🚀 Deploying Reccd Items API to AWS Elastic Beanstalk"

# Check if EB CLI is installed
if ! command -v eb &> /dev/null; then
    echo "❌ EB CLI not found. Installing..."
    pip install awsebcli
fi

# Check if EB is initialized
if [ ! -d ".elasticbeanstalk" ]; then
    echo "📝 Initializing Elastic Beanstalk..."
    eb init -p python-3.9 reccd-items-api --region us-west-2
fi

# Check if environment exists
if ! eb list | grep -q "reccd-items-api-env"; then
    echo "🏗️  Creating new environment..."
    eb create reccd-items-api-env --instance-type t3.micro --single
    
    echo "⚙️  Setting environment variables..."
    eb setenv \
        DB_HOST=$DB_HOST \
        DB_USER=$DB_USER \
        DB_PASSWORD=$DB_PASSWORD \
        DB_NAME=$DB_NAME \
        RAINFOREST_API_KEY=$RAINFOREST_API_KEY \
        KEEPA_API_KEY=$KEEPA_API_KEY \
        AMAZON_ASSOCIATE_TAG=$AMAZON_ASSOCIATE_TAG \
        USER_ID=$USER_ID \
        USER_EMAIL=$USER_EMAIL
else
    echo "📦 Deploying to existing environment..."
    eb deploy
fi

echo "✅ Deployment complete!"
echo "🌐 Opening application..."
eb open



