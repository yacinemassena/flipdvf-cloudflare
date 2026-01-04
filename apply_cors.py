import boto3
import sys

# Configuration
ACCOUNT_ID = "2a139e9393f803634546ad9d541d37b9"
BUCKET_NAME = "europe"

def apply_cors(access_key, secret_key):
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    cors_configuration = {
        'CORSRules': [{
            'AllowedHeaders': ['*'],
            'AllowedMethods': ['GET', 'HEAD'],
            'AllowedOrigins': ['*'],  # Allow all origins (localhost + production)
            'ExposeHeaders': ['ETag'],
            'MaxAgeSeconds': 3000
        }]
    }

    try:
        s3.put_bucket_cors(Bucket=BUCKET_NAME, CORSConfiguration=cors_configuration)
        print("✅ CORS configuration applied successfully!")
        print("Your map should now be able to fetch tiles from any domain.")
    except Exception as e:
        print(f"❌ Error applying CORS: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python apply_cors.py <ACCESS_KEY_ID> <SECRET_ACCESS_KEY>")
        sys.exit(1)
    
    apply_cors(sys.argv[1], sys.argv[2])
