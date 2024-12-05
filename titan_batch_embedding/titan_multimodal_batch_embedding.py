import boto3
import json
import base64
import time
import requests
import base64
import json
from io import BytesIO
import random
import string
import os

"""
This script is used to prepare the input data for bedrock titan multimodal embedding batch inference.
It downloads the images from the input data and converts them to base64 format, then stores the images in the S3 bucket.
It also creates the JSONL file for the input data.
You can find the batch infrence documentation here: https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html
"""

# AWS Configuration
REGION = "us-west-2"  # Replace with your AWS region
BUCKET_NAME = "xxxxx"  # Replace with your S3 bucket name
OUTPUT_EMBEDDING_LENGTH =  256 # Optional, One of: [256, 384, 1024], default: 1024
INPUT_FILE = "input.jsonl"


def generate_random_string(length):
    characters = string.ascii_letters + string.digits 
    return ''.join(random.choice(characters) for _ in range(length))

def get_image_from_url_and_store_image(url,image_file_name):
    """
    Download image from URL and convert to base64
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        image_content = response.content
        
        if not os.path.exists('images'):
            os.makedirs('images')
        with open(f"images/{image_file_name}.jpg", "wb") as image_file:
            image_file.write(image_content)
        base64_image = base64.b64encode(image_content).decode('utf-8')
        print(f"Downloaded image from {url} and stored as {image_file_name}.jpg")
        return base64_image
    except Exception as e:
        print(f"Error downloading image from {url}: {str(e)}")
        return None

def prepare_jsonl_for_bedrock_batch_inference(datasets, output_file=INPUT_FILE):
    """
    Prepare the JSONL file for bedrock titan multimodal embedding batch inference.
    Uses 'large' URL from datasets for images and converts them to base64.
    
    Args:
        datasets: A text file containing multi rows of JSON objects, each representing an 
        row like this:
        {"main_category": "AMAZON FASHION", "title": "BALEAF Women's Long Sleeve Zip Beach Coverup UPF 50+ Sun Protection Hooded Cover Up Shirt Dress with Pockets", "average_rating": 4.2, "rating_number": 422, "features": ["90% Polyester, 10% Spandex", "Zipper closure", "Machine Wash", "Long sleeve sun protection coverups--UPF 50+ blocks the sun from burning", "Zipped v-neckline--fashionable V neck and smooth 1/4 zipper allows to staying place as you like", "Two drop-in side pockets--hold your phone or keys well\uff0cno worries of falling out", "Hoodie with non-slip drawcord--Enhancing hooded design is convenient to wrap your face and enough space to put your head and hair easily", "A flattering coverups company you spend all day on the beach\uff0ctraveling with lovers or busying around house. Recommended For everyday leisure or daily exercise"], "description": [], "price": 31.99, "images": [{"thumb": "https://m.media-amazon.com/images/I/31zPYU+UkVL._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/31zPYU+UkVL._AC_.jpg", "variant": "MAIN", "hi_res": "https://m.media-amazon.com/images/I/61KIZjb54AL._AC_UL1500_.jpg"}, {"thumb": "https://m.media-amazon.com/images/I/31LYNX5KQ6L._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/31LYNX5KQ6L._AC_.jpg", "variant": "PT01", "hi_res": "https://m.media-amazon.com/images/I/61OkTrsW88L._AC_UL1500_.jpg"}, {"thumb": "https://m.media-amazon.com/images/I/51dD8Ga+wQL._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/51dD8Ga+wQL._AC_.jpg", "variant": "PT02", "hi_res": "https://m.media-amazon.com/images/I/811lbVuVIAL._AC_UL1500_.jpg"}, {"thumb": "https://m.media-amazon.com/images/I/41kM3Cn3ERL._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/41kM3Cn3ERL._AC_.jpg", "variant": "PT03", "hi_res": "https://m.media-amazon.com/images/I/71wa-UQGlYL._AC_UL1500_.jpg"}, {"thumb": "https://m.media-amazon.com/images/I/41ZzITDO+OL._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/41ZzITDO+OL._AC_.jpg", "variant": "PT04", "hi_res": "https://m.media-amazon.com/images/I/61tspZIpHqL._AC_UL1500_.jpg"}, {"thumb": "https://m.media-amazon.com/images/I/51jTzfIP3dL._AC_SR38,50_.jpg", "large": "https://m.media-amazon.com/images/I/51jTzfIP3dL._AC_.jpg", "variant": "PT05", "hi_res": "https://m.media-amazon.com/images/I/71oVECB3VYL._AC_UL1500_.jpg"}], "videos": [{"title": "Women's UPF 50+ Front Zip Beach Coverups with Pockets", "url": "https://www.amazon.com/vdp/0041f3df9f604577bd982078c9f3fec2?ref=dp_vse_rvc_0", "user_id": ""}, {"title": " Women's Swim Cover Up Dress", "url": "https://www.amazon.com/vdp/0222bb314b16471a8b43f19080358fdc?ref=dp_vse_rvc_1", "user_id": ""}], "store": "BALEAF", "categories": ["Clothing, Shoes & Jewelry", "Women", "Clothing", "Swimsuits & Cover Ups", "Cover-Ups"], "details": {"Department": "womens", "Date First Available": "April 3, 2022"}, "parent_asin": "B09X1MRDN6", "bought_together": null}

    """
    try:
        with open(output_file, "w") as f:
            with open(datasets, "r") as items:
                for row in items:
                    item = json.loads(row)
                    # Get the large image URL from the dataset
                    # Find the image with variant "MAIN" and get its large URL
                    image_url = next((img['large'] for img in item['images'] if img['variant'] == 'MAIN'), None)
                    if not image_url:
                        print(f"Skipping item - no large image URL found")
                        continue

                     # create a recordid ,also used as the image file name
                    recordId = generate_random_string(11)

                    # Get image in base64 format
                    image_base64 = get_image_from_url_and_store_image(image_url,recordId)
                    if not image_base64:
                        print(f"Skipping item - failed to download image")
                        continue
                
                    # Create the JSON entry
                    # get inputText from features,if features is empty, use title instead
                    inputText = item.get('features', '')[0] if item.get('features', '') else item.get('title', '')
                    modelInput = {
                        "inputImage": image_base64,
                        "inputText": inputText
                    }
                    json_entry = {
                        "recordId": recordId,
                        "modelInput": modelInput,
                        "embeddingConfig": { 
                            "outputEmbeddingLength": OUTPUT_EMBEDDING_LENGTH
                        }
                        }
                
                    # Write to JSONL file
                    f.write(json.dumps(json_entry) + "\n")
                    
        print(f"JSONL file '{output_file}' created successfully")
        # get the number of records in the JSONL file,if the number less than 100, print the number and error message,then exit
        num_records = sum(1 for line in open(output_file))
        if num_records < 100:
            print(f"Number of records in the JSONL file: {num_records}")
            print("Error: The number of records is less than 100, batch job need at least 100 records, please check the input data")
            exit(1)
    except Exception as e:
        print(f"Error creating JSONL file: {str(e)}")



def upload_jsonl_to_s3():
    """
    Step 2: Upload Input Data to S3
    Upload images to S3 bucket
    Args:
        image_paths (list): List of local image paths
    Returns:
        list: List of S3 URIs
    """
    s3_client = boto3.client('s3', region_name=REGION)
    s3_uris = []
    s3_client.upload_file(INPUT_FILE, BUCKET_NAME, f"input/{INPUT_FILE}")
    s3_uri = f"s3://{BUCKET_NAME}/input/{INPUT_FILE}"
    s3_uris.append(s3_uri)
    print(f"Uploaded {INPUT_FILE} to {s3_uri}")
    return s3_uris
    
    
def create_iam_role():
    """
    Step 3: Create an IAM Role
    Create IAM role with necessary permissions for Bedrock
    Returns:
        str: IAM role ARN
    """
    iam = boto3.client('iam')
    
    # Define the role policy
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "bedrock.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # Create the role
    try:
        response = iam.create_role(
            RoleName='BedrockBatchInferenceRole',
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        
        # Attach necessary policies
        policy_arns = [
            'arn:aws:iam::aws:policy/AmazonS3FullAccess',
            'arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
        ]
        
        for policy_arn in policy_arns:
            iam.attach_role_policy(
                RoleName='BedrockBatchInferenceRole',
                PolicyArn=policy_arn
            )
        
        return response['Role']['Arn']
    except iam.exceptions.EntityAlreadyExistsException:
        print("Role already exists, retrieving ARN...")
        response = iam.get_role(RoleName='BedrockBatchInferenceRole')
        return response['Role']['Arn']

def setup_batch_inference(s3_input_location, s3_output_location, role_arn):

    bedrock = boto3.client(service_name="bedrock",region_name=REGION)

    inputDataConfig=({
        "s3InputDataConfig": {
            "s3Uri": s3_input_location
        }
    })

    outputDataConfig=({
        "s3OutputDataConfig": {
            "s3Uri": s3_output_location
        }
    })

    response=bedrock.create_model_invocation_job(
        roleArn=role_arn,
        modelId="amazon.titan-embed-image-v1",
        jobName=f"titan-batch-embedding-{int(time.time())}",
        inputDataConfig=inputDataConfig,
        outputDataConfig=outputDataConfig
    )

    jobArn = response.get('jobArn')
    return jobArn


"""
def setup_batch_inference(s3_input_location, s3_output_location, role_arn):
    '''
    第4步：设置批量推理作业
    配置并启动批量推理作业
    参数:
        s3_input_location (str): 输入数据的S3 URI
        s3_output_location (str): 输出数据的S3 URI 
        role_arn (str): IAM角色ARN
    返回:
        str: 批量推理作业ID
    '''
    bedrock = boto3.client('bedrock', region_name=REGION)
    
    response = bedrock.create_model_inference_job(
        jobName=f"titan-batch-embedding-{int(time.time())}", # 使用时间戳创建唯一作业名称
        modelId="amazon.titan-embed-image-v1", # 使用Titan多模态嵌入模型
        inputConfiguration={
            "s3Uri": s3_input_location, # 输入数据的S3位置
            "dataInputConfig": {
                "format": "json", # 输入格式为JSON
                "maxLength": 512 # 最大序列长度
            }
        },
        outputConfiguration={
            "s3Uri": s3_output_location # 输出数据的S3位置
        },
        roleArn=role_arn, # 执行作业的IAM角色
        inferenceConfiguration={
            "mode": "embedding", # 生成嵌入向量模式
            "batchStrategy": "DYNAMIC", # 动态批处理策略
            "maxConcurrentInvocations": 5 # 最大并发调用数
        }
    )
    
    return response['jobArn'] # 返回作业ARN
"""
def retrieve_output(job_arn):
    """
    Step 5: Retrieve Output Data
    Monitor job status and retrieve results
    Args:
        job_arn (str): Batch inference job ARN
    module_output like this:
{"modelInput":{"inputImage":"image base64","inputText":"90% Polyester, 10% Spandex"},"modelOutput":{"embedding": embedding result ,"inputTextTokenCount":12},"recordId":"bFBEdQuy3Gp"}
    """
    bedrock = boto3.client('bedrock', region_name=REGION)
    s3_client = boto3.client('s3', region_name=REGION)
    
    while True:
        response = bedrock.get_model_invocation_job(jobIdentifier=job_arn)
        print(response)
        status = response['status']
        
        if status.lower() == 'completed':
            print("Job completed successfully!")
            
            # Get output file from S3
            file_path = job_arn.split('/')[-1]  
            print(f"File path: {file_path}")
            output_key = f"output/{file_path}/{INPUT_FILE}.out"
            local_output_file = f"{INPUT_FILE}.out"
            print(f"Downloading output file {output_key} to {local_output_file}")
            
            try:
                s3_client.download_file(BUCKET_NAME, output_key, local_output_file)                
                # Read and print recordId and embeddings
                with open(local_output_file, 'r') as f:
                    for line in f:
                        result = json.loads(line)
                        print(f"recordId: {result['recordId']}, Embedding: {result['modelOutput']['embedding']}")
                return                
            except Exception as e:
                print(f"Error retrieving output: {str(e)}")
            break
            
        elif status.lower() in ['failed', 'stopped']:
            print(f"Job failed with status: {status}")
            break
        
        print(f"Job status: {status}")
        time.sleep(30)  # Check status every 30 seconds

def main():
    """Main execution function"""
    """# Step 1: Prepare sample images
    prepare_jsonl_for_bedrock_batch_inference("datasets")
    
    # Step 2: Upload to S3
    s3_uris = upload_jsonl_to_s3()
    
    # Step 3: Create IAM role
    role_arn = create_iam_role()
    
    # Step 4: Setup batch inference
    s3_input_location = f"s3://{BUCKET_NAME}/input/"
    s3_output_location = f"s3://{BUCKET_NAME}/output/"
    job_arn = setup_batch_inference(s3_input_location, s3_output_location, role_arn)"""
    
    job_arn = "arn:aws:bedrock:us-west-2:733876261325:model-invocation-job/fez05va2apyx"
    # Step 5: Retrieve output
    retrieve_output(job_arn)

if __name__ == "__main__":
    main()
