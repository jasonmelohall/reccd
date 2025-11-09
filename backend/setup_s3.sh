#!/bin/bash

# Script to set up S3 bucket for product images

BUCKET_NAME="reccd-items-images"
REGION="us-west-2"

echo "🪣 Creating S3 bucket: $BUCKET_NAME"

# Create bucket
aws s3 mb s3://$BUCKET_NAME --region $REGION

# Enable public access (required for public-read ACL)
echo "🔓 Configuring public access..."
aws s3api put-public-access-block \
    --bucket $BUCKET_NAME \
    --public-access-block-configuration \
    "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"

# Set bucket policy for public read access
echo "📜 Setting bucket policy..."
cat > /tmp/bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadGetObject",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::$BUCKET_NAME/*"
    }
  ]
}
EOF

aws s3api put-bucket-policy \
    --bucket $BUCKET_NAME \
    --policy file:///tmp/bucket-policy.json

# Enable CORS for web/mobile access
echo "🌐 Configuring CORS..."
cat > /tmp/cors-config.json <<EOF
{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": ["*"],
      "MaxAgeSeconds": 3000
    }
  ]
}
EOF

aws s3api put-bucket-cors \
    --bucket $BUCKET_NAME \
    --cors-configuration file:///tmp/cors-config.json

echo "✅ S3 bucket setup complete!"
echo "📍 Bucket URL: https://$BUCKET_NAME.s3.$REGION.amazonaws.com"

# Clean up temp files
rm /tmp/bucket-policy.json /tmp/cors-config.json



