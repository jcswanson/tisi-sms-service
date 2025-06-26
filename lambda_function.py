# TISI | Version 10 | @jcswanson
# - Multi-lined Perplexity prompt with sentence limitation
# - Logging of Perplexity tokens

import json
import os
import logging
import re
import time
from typing import Optional, Tuple

try:
    import ujson as json_lib
except ImportError:
    import json as json_lib

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from cachetools import TTLCache
import urllib3
from urllib3.util.retry import Retry
import boto3
from botocore.exceptions import ClientError

logger = Logger(service="tisi-sms", sampling_rate=0.8)
tracer = Tracer(service="tisi-sms")
metrics = Metrics(namespace="Tisi", service="sms-service")

cognito_user_cache = TTLCache(maxsize=500, ttl=1800)
cognito_auth_cache = TTLCache(maxsize=1000, ttl=300)

sns = boto3.client('sns')
cognito = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')
PERPLEXITY_API_URL = os.environ['PERPLEXITY_API_URL']
PERPLEXITY_API_KEY = os.environ['PERPLEXITY_API_KEY']
COGNITO_USER_POOL_ID = os.environ['COGNITO_USER_POOL_ID']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Optimized connection pooling for future growth
retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
http = urllib3.PoolManager(
    retries=retries,
    maxsize=10,
    block=True
)

ERROR_MESSAGE_TEMPLATE = (
    "TISI ALERT: Technical issue prevented response. Try again in 5 mins - may be high traffic. "
    "Still not working? Go to textitsearchit.com, log into your account, and submit a ticket from the tech support area of your dashboard. Sorry for the inconvenience!"
)

def send_error_message_to_user(phone_number: str, origination_number: str, transaction_id: str, error_type: str = "general"):
    try:
        sns.publish(
            PhoneNumber=phone_number,
            Message=ERROR_MESSAGE_TEMPLATE,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {'DataType': 'String', 'StringValue': 'Transactional'},
                'AWS.MM.SMS.OriginationNumber': {'DataType': 'String', 'StringValue': origination_number}
            }
        )
        metrics.add_metric(name="ErrorNotificationsSent", unit=MetricUnit.Count, value=1)
    except Exception as e:
        logger.error("Failed to send error message", extra={"transaction_id": transaction_id, "error": str(e)})
        metrics.add_metric(name="ErrorNotificationFailed", unit=MetricUnit.Count, value=1)

def get_cognito_user_cached(phone_number: str, transaction_id: str) -> dict:
    cache_key = f"cognito_user:{phone_number}"
    if cache_key in cognito_user_cache:
        return cognito_user_cache[cache_key]
    start_time = time.time()
    try:
        response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info("Cognito lookup duration", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "service": "cognito"
        })
        cognito_user_cache[cache_key] = response
        return response
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error("Cognito lookup failed", extra={
            "phone": phone_number,
            "error": str(e),
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
        })
        cognito_user_cache[cache_key] = {'Users': []}
        return {'Users': []}

def is_user_authorized_optimized(phone_number: str, destination_number: str, transaction_id: str) -> Tuple[bool, Optional[str]]:
    cache_key = f"auth:{phone_number}:{destination_number}"
    if cache_key in cognito_auth_cache:
        return cognito_auth_cache[cache_key]
    try:
        cognito_response = get_cognito_user_cached(phone_number, transaction_id)
        if not cognito_response['Users']:
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        user = cognito_response['Users'][0]
        user_status = user['UserStatus']
        phone_verified = next((attr for attr in user['Attributes'] if attr['Name'] == 'phone_number_verified'), None)
        if not (user_status == 'CONFIRMED' and phone_verified and phone_verified['Value'] == 'true'):
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        user_email = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'email'), None)
        if not cognito_id or not user_email:
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        # Use GSI for fast DynamoDB lookup
        start_time = time.time()
        response = table.query(
            IndexName='cognitoId-index',
            KeyConditionExpression='cognitoId = :cognito_id',
            ExpressionAttributeValues={':cognito_id': cognito_id}
        )
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info("DynamoDB query duration", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "service": "dynamodb"
        })
        if not response.get('Items'):
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        user_item = response['Items'][0]
        if user_item.get('subscriptionStatus') != 'active':
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        service_phone = user_item.get('servicePhoneNumber')
        if service_phone and service_phone != destination_number:
            result = (False, None)
            cognito_auth_cache[cache_key] = result
            return result
        result = (True, user_email)
        cognito_auth_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error("Authorization error", extra={
            "phone": phone_number,
            "error": str(e),
            "transaction_id": transaction_id,
        })
        result = (False, None)
        cognito_auth_cache[cache_key] = result
        return result

def validate_and_clean_message(message_body: str) -> Optional[str]:
    if not message_body or not isinstance(message_body, str):
        return None
    cleaned = message_body.strip().lower()
    return cleaned if cleaned else None

def handle_keywords(message_body: str, phone_number: str) -> bool:
    cleaned_message = validate_and_clean_message(message_body)
    if not cleaned_message:
        return False
    if cleaned_message == 'stop':
        # Update subscription status logic
        return True
    elif cleaned_message in ('help', 'join'):
        return True
    return False

def query_perplexity(query: str, transaction_id: str) -> str:
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "sonar",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a real-time search engine for SMS. "
                    "Provide a single, concise, and accurate answer to the user using no more than 280 characters. "
                    "You're answer should NOT be more than 5 complete sentences. "
                    "Make sure your answer's final sentence is complete and never cut short."
                    "The answer should contain the most current information available. "
                    "Do not use Markdown formatting, citations, or any extra text beyond the answer itself."
                )
            },
            {
                "role": "user",
                "content": query
            }
        ],
        "web_search_options": {
            "search_context_size": "medium"
        },
        "max_tokens": 400,  # Increased to ensure complete answers up to 280 characters
        "temperature": 0.3,  # Slightly increased for more complete responses while maintaining focus
        "top_p": 0.7  # Reduced to focus on more probable tokens for relevance
    }
    start_time = time.time()
    try:
        encoded_payload = json_lib.dumps(payload).encode('utf-8')
        response = http.request(
            'POST',
            PERPLEXITY_API_URL,
            body=encoded_payload,
            headers=headers,
            timeout=urllib3.Timeout(connect=2.0, read=10.0)
        )
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Extract token usage if available in the response
        token_usage = {}
        if response.status == 200:
            result = json_lib.loads(response.data.decode('utf-8'))
            if 'usage' in result:
                token_usage = result['usage']
                logger.info("Perplexity API token usage", extra={
                    "transaction_id": transaction_id,
                    "input_tokens": token_usage.get('prompt_tokens', 0),
                    "output_tokens": token_usage.get('completion_tokens', 0),
                    "total_tokens": token_usage.get('total_tokens', 0),
                    "service": "perplexity"
                })
        
        logger.info("Perplexity API call duration", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "service": "perplexity",
            "status_code": response.status
        })
        
        if response.status == 200:
            return result['choices'][0]['message']['content']
        else:
            raise Exception(f"API status {response.status}")
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error("Perplexity API error", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "error": str(e)
        })
        raise Exception(f"Perplexity error: {str(e)}")


def clean_response(text: str) -> str:
    try:
        text = re.sub(r'[*#`_~]', '', text)
        text = re.sub(r'\[\d*\]', '', text)
        return ' '.join(text.split()).strip()
    except:
        return "Response formatting error."

def send_sms(phone_number: str, message: str, origination_number: str, transaction_id: str):
    start_time = time.time()
    try:
        sns.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {'DataType': 'String', 'StringValue': 'Transactional'},
                'AWS.MM.SMS.OriginationNumber': {'DataType': 'String', 'StringValue': origination_number}
            }
        )
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info("SMS send duration", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "service": "sns"
        })
        metrics.add_metric(name="SMSSent", unit=MetricUnit.Count, value=1)
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error("SMS send failed", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "error": str(e)
        })
        metrics.add_metric(name="SMSSendError", unit=MetricUnit.Count, value=1)
        raise

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event: dict, context):
    transaction_id = context.aws_request_id
    start_time = int(time.time() * 1000)
    sender_phone = None
    destination_number = None
    user_email = None
    original_query = None
    cleaned_response = None
    is_authorized = False

    # Extract version from context or log stream
    version = getattr(context, 'function_version', '$LATEST')
    log_stream = getattr(context, 'log_stream_name', None)
    version_match = None
    if isinstance(log_stream, str):
        version_match = re.search(r'\[(\d+)\]', log_stream)
    if version_match:
        version = version_match.group(1)

    logger.info("SMS received", extra={
        "transaction_id": transaction_id,
        "start_time_ms": start_time,
        "version": version
    })
    metrics.add_metric(name="SMSReceived", unit=MetricUnit.Count, value=1)

    try:
        sns_message = json_lib.loads(event['Records'][0]['Sns']['Message'])
        sender_phone = sns_message.get('originationNumber', 'unknown')
        message_content = sns_message.get('messageBody', '')
        destination_number = sns_message.get('destinationNumber', 'unknown')
        if handle_keywords(message_content, sender_phone):
            return {'statusCode': 200, 'body': 'Keyword processed'}
        is_authorized, user_email = is_user_authorized_optimized(
            sender_phone, destination_number, transaction_id
        )
        if not is_authorized:
            logger.warning("Unauthorized access", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone,
                "version": version
            })
            metrics.add_metric(name="UnauthorizedAccess", unit=MetricUnit.Count, value=1)
            return {'statusCode': 403, 'body': 'Unauthorized'}
        original_query = message_content
        try:
            perplexity_response = query_perplexity(message_content, transaction_id)
            cleaned_response = clean_response(perplexity_response)
            logger.info("User query processed", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone,
                "email": user_email or "none",
                "query": original_query,
                "response": cleaned_response[:280],
                "version": version
            })
            send_sms(sender_phone, cleaned_response, destination_number, transaction_id)
            metrics.add_metric(name="SuccessfulResponses", unit=MetricUnit.Count, value=1)
            return {'statusCode': 200, 'body': 'Message processed'}
        except Exception as e:
            logger.error("Error during Perplexity or SMS send", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone or "unknown",
                "email": user_email or "none",
                "query": original_query or "none",
                "error": str(e),
                "version": version
            })
            metrics.add_metric(name="ProcessingError", unit=MetricUnit.Count, value=1)
            if is_authorized and sender_phone and destination_number and sender_phone != 'unknown':
                send_error_message_to_user(sender_phone, destination_number, transaction_id)
            return {'statusCode': 500, 'body': 'Processing error'}
    except Exception as e:
        logger.error("Fatal error in lambda_handler", extra={
            "transaction_id": transaction_id,
            "phone": sender_phone or "unknown",
            "email": user_email or "none",
            "query": original_query or "none",
            "error": str(e),
            "version": version
        })
        metrics.add_metric(name="ProcessingError", unit=MetricUnit.Count, value=1)
        # Only send error if user is authorized and numbers are valid
        if is_authorized and sender_phone and destination_number and sender_phone != 'unknown':
            send_error_message_to_user(sender_phone, destination_number, transaction_id)
        return {'statusCode': 500, 'body': 'Processing error'}
