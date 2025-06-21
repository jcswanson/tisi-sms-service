import json
import os
import logging
import re
import time
from typing import Optional

# Use ujson for faster JSON parsing
try:
    import ujson as json_lib
except ImportError:
    import json as json_lib

# Use structured logging
import structlog
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from cachetools import TTLCache
import urllib3
from urllib3.util.retry import Retry
import boto3
from botocore.exceptions import ClientError

# Set up structured logging with Powertools
logger = Logger(service="tisi-sms")
tracer = Tracer(service="tisi-sms")
metrics = Metrics(namespace="Tisi", service="sms-service")

# Initialize cache for subscription status (5-minute TTL)
subscription_cache = TTLCache(maxsize=1000, ttl=300)

# Initialize AWS clients
sns = boto3.client('sns')
cognito = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')

# Environment variables
PERPLEXITY_API_URL = os.environ['PERPLEXITY_API_URL']
PERPLEXITY_API_KEY = os.environ['PERPLEXITY_API_KEY']
COGNITO_USER_POOL_ID = os.environ['COGNITO_USER_POOL_ID']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Configure urllib3 with retries
retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
http = urllib3.PoolManager(retries=retries)

# Error message template for users
ERROR_MESSAGE_TEMPLATE = "TISI SERVICE ALERT: We're experiencing a technical issue that prevented the service from responding. Please try again in a few minutes as the service might be experiencing heavier than normal traffic. If it still isn't working after five minutes, go to your account dashboard on textitsearchit.com and fill out a support ticket to alert our team. We apologize for the inconvenience and thank you for writing a ticket and helping us make Tisi stronger."

def send_error_message_to_user(phone_number: str, origination_number: str, transaction_id: str, error_type: str = "general"):
    """Send error notification to user when service fails"""
    try:
        logger.warning("Sending error message to user", extra={
            "transaction_id": transaction_id,
            "phone": phone_number,
            "error_type": error_type
        })
        
        response = sns.publish(
            PhoneNumber=phone_number,
            Message=ERROR_MESSAGE_TEMPLATE,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {'DataType': 'String', 'StringValue': 'Transactional'},
                'AWS.MM.SMS.OriginationNumber': {'DataType': 'String', 'StringValue': origination_number}
            }
        )
        
        logger.info("Error message sent to user", extra={
            "transaction_id": transaction_id,
            "message_id": response.get('MessageId', 'unknown'),
            "phone": phone_number
        })
        
        # Add metric for error notifications sent
        metrics.add_metric(name="ErrorNotificationsSent", unit=MetricUnit.Count, value=1)
        
    except Exception as e:
        logger.error("Failed to send error message to user", extra={
            "transaction_id": transaction_id,
            "phone": phone_number,
            "error": str(e)
        })
        metrics.add_metric(name="ErrorNotificationFailed", unit=MetricUnit.Count, value=1)

def validate_and_clean_message(message_body):
    """Validate and normalize message for case-insensitive comparison"""
    if not message_body or not isinstance(message_body, str):
        return None
    cleaned = message_body.strip().lower()
    if not cleaned:
        return None
    return cleaned

def update_subscription_status(phone_number, status):
    """Update user's subscription status in DynamoDB"""
    try:
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            return False
        
        user = cognito_response['Users'][0]
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            return False
        
        # Optimized: Use query instead of scan if GSI exists
        try:
            response = table.query(
                IndexName='cognitoId-index',
                KeyConditionExpression='cognitoId = :cognito_id',
                ExpressionAttributeValues={':cognito_id': cognito_id}
            )
        except ClientError:
            response = table.scan(
                FilterExpression="cognitoId = :cognito_id",
                ExpressionAttributeValues={":cognito_id": cognito_id}
            )
        
        if not response.get('Items'):
            return False
            
        user_item = response['Items'][0]
        user_id = user_item.get('id')
        
        if not user_id:
            return False
        
        table.update_item(
            Key={'id': user_id},
            UpdateExpression='SET subscriptionStatus = :status',
            ExpressionAttributeValues={':status': status}
        )
        return True
    except Exception as e:
        logger.error("Error updating subscription status", extra={"phone": phone_number, "error": str(e)})
        return False

def handle_join_request(phone_number):
    """Handle join keyword - reactivate subscription if currently stopped"""
    try:
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            return False
        
        user = cognito_response['Users'][0]
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            return False
        
        # Optimized: Use query instead of scan if GSI exists
        try:
            response = table.query(
                IndexName='cognitoId-index',
                KeyConditionExpression='cognitoId = :cognito_id',
                ExpressionAttributeValues={':cognito_id': cognito_id}
            )
        except ClientError:
            response = table.scan(
                FilterExpression="cognitoId = :cognito_id",
                ExpressionAttributeValues={":cognito_id": cognito_id}
            )
        
        if not response.get('Items'):
            return False
            
        user_item = response['Items'][0]
        current_status = user_item.get('subscriptionStatus')
        
        if current_status != 'stopped':
            return False
        
        user_id = user_item.get('id')
        
        if not user_id:
            return False
        
        table.update_item(
            Key={'id': user_id},
            UpdateExpression='SET subscriptionStatus = :status',
            ExpressionAttributeValues={':status': 'active'}
        )
        return True
        
    except Exception as e:
        logger.error("Error processing join request", extra={"phone": phone_number, "error": str(e)})
        return False

def handle_keywords(message_body, phone_number):
    """Handle stop, help, and join keywords - all skip Perplexity response"""
    cleaned_message = validate_and_clean_message(message_body)
    if not cleaned_message:
        return False
    
    if cleaned_message == 'stop':
        update_subscription_status(phone_number, 'stopped')
        return True
    elif cleaned_message == 'help':
        return True
    elif cleaned_message == 'join':
        handle_join_request(phone_number)
        return True
    
    return False

def is_user_authorized(phone_number, destination_number):
    """Check if user is authorized based on Cognito and DynamoDB data"""
    try:
        # Check cache first
        cache_key = f"{phone_number}:{destination_number}"
        if cache_key in subscription_cache:
            return subscription_cache[cache_key]
        
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            subscription_cache[cache_key] = False
            return False
        
        user = cognito_response['Users'][0]
        user_status = user['UserStatus']
        phone_verified = next((attr for attr in user['Attributes'] if attr['Name'] == 'phone_number_verified'), None)
        
        if not (user_status == 'CONFIRMED' and phone_verified and phone_verified['Value'] == 'true'):
            subscription_cache[cache_key] = False
            return False
        
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            subscription_cache[cache_key] = False
            return False
        
        user_email = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'email'), None)
        
        if not user_email:
            subscription_cache[cache_key] = False
            return False
            
        # Optimized: Use query instead of scan if GSI exists
        try:
            response = table.query(
                IndexName='cognitoId-index',
                KeyConditionExpression='cognitoId = :cognito_id',
                ExpressionAttributeValues={':cognito_id': cognito_id}
            )
        except ClientError:
            response = table.scan(
                FilterExpression="cognitoId = :cognito_id",
                ExpressionAttributeValues={":cognito_id": cognito_id}
            )
        
        if not response.get('Items'):
            subscription_cache[cache_key] = False
            return False
            
        user_item = response['Items'][0]
        
        if user_item.get('subscriptionStatus') != 'active':
            subscription_cache[cache_key] = False
            return False
            
        service_phone_number = user_item.get('servicePhoneNumber')
        
        if not service_phone_number:
            subscription_cache[cache_key] = True
            return True
            
        if service_phone_number != destination_number:
            subscription_cache[cache_key] = False
            return False
        
        subscription_cache[cache_key] = True
        return True
    
    except Exception as e:
        logger.error("Error checking authorization", extra={"phone": phone_number, "error": str(e)})
        subscription_cache[cache_key] = False
        return False

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event, context):
    transaction_id = context.aws_request_id
    start_time = int(time.time() * 1000)
    
    # Initialize variables for error handling
    sender_phone = None
    destination_number = None
    is_subscribed_user = False
    
    logger.info("SMS received", extra={
        "transaction_id": transaction_id,
        "start_time_ms": start_time
    })
    
    metrics.add_metric(name="SMSReceived", unit=MetricUnit.Count, value=1)
    
    try:
        # Parse SNS message
        sns_message = json_lib.loads(event['Records'][0]['Sns']['Message'])
        sender_phone = sns_message.get('originationNumber', 'unknown')
        message_content = sns_message.get('messageBody', '')
        destination_number = sns_message.get('destinationNumber', 'unknown')
        
        logger.info("Message parsed", extra={
            "transaction_id": transaction_id,
            "phone": sender_phone,
            "destination": destination_number
        })
        
        # Handle keywords first (these don't require full authorization)
        if handle_keywords(message_content, sender_phone):
            logger.info("Keyword processed", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone
            })
            return {
                'statusCode': 200,
                'body': json_lib.dumps('Keyword processed, no response sent')
            }
        
        # Check if user is authorized/subscribed
        try:
            is_subscribed_user = is_user_authorized(sender_phone, destination_number)
            
            if not is_subscribed_user:
                logger.warning("Unauthorized access", extra={
                    "transaction_id": transaction_id,
                    "phone": sender_phone
                })
                metrics.add_metric(name="UnauthorizedAccess", unit=MetricUnit.Count, value=1)
                return {
                    'statusCode': 403,
                    'body': json_lib.dumps('Unauthorized phone number for this service')
                }
                
        except Exception as auth_error:
            logger.error("Authorization check failed", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone,
                "error": str(auth_error)
            })
            # Don't send error message if we can't verify subscription
            metrics.add_metric(name="AuthorizationError", unit=MetricUnit.Count, value=1)
            return {
                'statusCode': 500,
                'body': json_lib.dumps('Error checking authorization')
            }
        
        # Process Perplexity query
        try:
            perplexity_response = query_perplexity(message_content)
            cleaned_response = clean_response(perplexity_response)
            
            # Send successful response
            send_sms(sender_phone, cleaned_response, destination_number, transaction_id)
            
            metrics.add_metric(name="SuccessfulResponses", unit=MetricUnit.Count, value=1)
            
            return {
                'statusCode': 200,
                'body': json_lib.dumps('Message processed successfully')
            }
            
        except Exception as processing_error:
            logger.error("Error processing Perplexity request", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone,
                "error": str(processing_error)
            })
            
            # Send error message to subscribed user
            if is_subscribed_user and sender_phone and destination_number:
                send_error_message_to_user(sender_phone, destination_number, transaction_id, "perplexity_error")
            
            metrics.add_metric(name="ProcessingError", unit=MetricUnit.Count, value=1)
            
            return {
                'statusCode': 500,
                'body': json_lib.dumps('Error processing message')
            }
            
    except Exception as e:
        logger.error("Critical error in lambda_handler", extra={
            "transaction_id": transaction_id,
            "phone": sender_phone if sender_phone else "unknown",
            "error": str(e)
        })
        
        # Send error message to user if we know they're subscribed
        if is_subscribed_user and sender_phone and destination_number:
            send_error_message_to_user(sender_phone, destination_number, transaction_id, "critical_error")
        
        metrics.add_metric(name="CriticalError", unit=MetricUnit.Count, value=1)
        
        return {
            'statusCode': 500,
            'body': json_lib.dumps('Critical error processing message')
        }

def clean_response(text):
    """Clean text by removing Markdown and unnecessary characters"""
    try:
        text = re.sub(r'[*#`_~]', '', text)
        text = re.sub(r'\[\d*\]', '', text)
        text = ' '.join(text.split()).strip()
        return text
    except Exception as e:
        logger.error("Error cleaning response", extra={"error": str(e)})
        return "Sorry, there was an error formatting the response."

def query_perplexity(query):
    """Query Perplexity API with optimized timeout and error handling"""
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "sonar", 
        "messages": [
            {
                "role": "system",
                "content": "Provide answers suitable for SMS messages, with a max of 280 characters. Do not provide citations and don't format responses with Markdown syntax."
            },
            {
                "role": "user",
                "content": query
            }
        ]
    }
    
    try:
        encoded_payload = json_lib.dumps(payload).encode('utf-8')
        response = http.request(
            'POST',
            PERPLEXITY_API_URL,
            body=encoded_payload,
            headers=headers,
            timeout=urllib3.Timeout(connect=2.0, read=10.0)
        )
        
        if response.status == 200:
            result = json_lib.loads(response.data.decode('utf-8'))
            return result['choices'][0]['message']['content']
        else:
            logger.error("Perplexity API error", extra={"status": response.status})
            raise Exception(f"Perplexity API returned status {response.status}")
            
    except urllib3.exceptions.TimeoutError:
        logger.error("Perplexity API timeout")
        raise Exception("Perplexity API timeout")
    except Exception as e:
        logger.error("Perplexity API error", extra={"error": str(e)})
        raise Exception(f"Perplexity API error: {str(e)}")

def send_sms(phone_number, message, origination_number, transaction_id):
    """Send SMS response to user and log end time for latency tracking"""
    try:
        end_time = int(time.time() * 1000)
        
        logger.info("SMS response sent", extra={
            "transaction_id": transaction_id,
            "end_time_ms": end_time,
            "phone": phone_number if phone_number else "unknown"
        })
        
        response = sns.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {'DataType': 'String', 'StringValue': 'Transactional'},
                'AWS.MM.SMS.OriginationNumber': {'DataType': 'String', 'StringValue': origination_number}
            }
        )
        
        logger.info("SMS sent successfully", extra={
            "transaction_id": transaction_id,
            "message_id": response.get('MessageId', 'unknown')
        })
        
        metrics.add_metric(name="SMSSent", unit=MetricUnit.Count, value=1)
        
    except Exception as e:
        logger.error("Error sending SMS", extra={
            "transaction_id": transaction_id,
            "error": str(e)
        })
        metrics.add_metric(name="SMSSendError", unit=MetricUnit.Count, value=1)
        raise Exception(f"Failed to send SMS: {str(e)}")
