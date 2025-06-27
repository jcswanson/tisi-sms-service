# Text It Search It | Version 12 | @jcswanson
# - Removed Cognito authentication, replaced with DynamoDB auth only

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

# cognito_user_cache = TTLCache(maxsize=500, ttl=1800)
# Update this line near the top of your file
dynamodb_auth_cache = TTLCache(maxsize=1000, ttl=900)

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
# cognito = boto3.client('cognito-idp')
# COGNITO_USER_POOL_ID = os.environ['COGNITO_USER_POOL_ID']
PERPLEXITY_API_URL = os.environ['PERPLEXITY_API_URL']
PERPLEXITY_API_KEY = os.environ['PERPLEXITY_API_KEY']
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

# INCLUDED IN VERSION 11
# def get_cognito_user_cached(phone_number: str, transaction_id: str) -> dict:
#     cache_key = f"cognito_user:{phone_number}"
#     if cache_key in cognito_user_cache:
#         return cognito_user_cache[cache_key]
#     start_time = time.time()
#     try:
#         response = cognito.list_users(
#             UserPoolId=COGNITO_USER_POOL_ID,
#             Filter=f'phone_number = "{phone_number}"'
#         )
#         duration_ms = int((time.time() - start_time) * 1000)
#         logger.info("Cognito lookup duration", extra={
#             "transaction_id": transaction_id,
#             "duration_ms": duration_ms,
#             "service": "cognito"
#         })
#         cognito_user_cache[cache_key] = response
#         return response
#     except Exception as e:
#         duration_ms = int((time.time() - start_time) * 1000)
#         logger.error("Cognito lookup failed", extra={
#             "phone": phone_number,
#             "error": str(e),
#             "transaction_id": transaction_id,
#             "duration_ms": duration_ms,
#         })
#         cognito_user_cache[cache_key] = {'Users': []}
#         return {'Users': []}

def is_user_authorized_dynamodb_only(phone_number: str, destination_number: str, transaction_id: str) -> Tuple[bool, Optional[str]]:
    """Streamlined authorization using only DynamoDB for maximum performance"""
    cache_key = f"auth_ddb:{phone_number}:{destination_number}"
    
    # Check cache first - increased TTL since we're eliminating slower Cognito calls
    if cache_key in dynamodb_auth_cache:
        return dynamodb_auth_cache[cache_key]
    
    try:
        start_time = time.time()
        
        # Direct DynamoDB query by phone number using the new GSI
        response = table.query(
            IndexName='phoneNumber-index',
            KeyConditionExpression='phoneNumber = :phone',
            ExpressionAttributeValues={':phone': phone_number}
        )
        
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info("DynamoDB auth query duration", extra={
            "transaction_id": transaction_id,
            "duration_ms": duration_ms,
            "service": "dynamodb_auth"
        })
        
        # Check if user exists
        if not response.get('Items'):
            result = (False, None)
            dynamodb_auth_cache[cache_key] = result
            return result
            
        user_item = response['Items'][0]
        
        # Check subscription status
        subscription_status = user_item.get('subscriptionStatus')
        if subscription_status != 'active':
            logger.info("User subscription not active", extra={
                "transaction_id": transaction_id,
                "phone": phone_number,
                "status": subscription_status
            })
            result = (False, None)
            dynamodb_auth_cache[cache_key] = result
            return result
        
        # Check service phone number if set
        service_phone = user_item.get('servicePhoneNumber')
        if service_phone and service_phone != destination_number:
            logger.warning("Service phone mismatch", extra={
                "transaction_id": transaction_id,
                "phone": phone_number,
                "expected": service_phone,
                "received": destination_number
            })
            result = (False, None)
            dynamodb_auth_cache[cache_key] = result
            return result
        
        # Get user email for logging
        user_email = user_item.get('email', 'unknown')
        
        # Success - cache and return
        result = (True, user_email)
        dynamodb_auth_cache[cache_key] = result
        
        logger.info("User authorized successfully", extra={
            "transaction_id": transaction_id,
            "phone": phone_number,
            "email": user_email
        })
        
        return result
        
    except Exception as e:
        logger.error("DynamoDB authorization error", extra={
            "phone": phone_number,
            "error": str(e),
            "transaction_id": transaction_id,
        })
        result = (False, None)
        dynamodb_auth_cache[cache_key] = result
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
    """Enhanced Perplexity API call with improved prompting and response handling"""
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
                    "You are a real-time search engine for SMS responses. "
                    "CRITICAL CONSTRAINTS: "
                    "- Aim for responses around 400 characters but prioritize complete sentences over strict length limits "
                    "- Use exactly 4-5 complete sentences maximum "
                    "- Each sentence MUST be fully complete with proper punctuation "
                    "- Never end mid-sentence - complete sentences are more important than character limits "
                    "- Prioritize the most important information first "
                    "- Use simple, direct language without unnecessary words "
                    "- Do not use Markdown, citations, or formatting "
                    "- If the answer requires more space for complete sentences, that is acceptable"
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
        "max_tokens": 280,  # Optimized for complete sentences within SMS constraints
        "temperature": 0.2,  # Lower temperature for more focused responses
        "top_p": 0.7
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
            raw_response = result['choices'][0]['message']['content']
            # Apply validation and truncation using the enhanced function
            validated_response = validate_and_truncate_response(raw_response, transaction_id)
            return validated_response
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

def validate_and_truncate_response(text: str, transaction_id: str) -> str:
    """
    Validates and optimizes response for SMS delivery:
    - Prioritizes complete sentences over strict character limits
    - Aims for ~400 characters but allows up to ~440 for sentence completion
    - Ensures final sentence is always complete (never truncated mid-sentence)
    - Maximum 5 sentences for readability
    - Handles periods, exclamation marks, and question marks as sentence endings
    """
    try:
        # First clean the response
        cleaned_text = clean_response(text)
        
        # If already under limit, return as-is
        if len(cleaned_text) <= 400:
            return cleaned_text
        
        # Split into sentences - handle multiple punctuation marks
        sentences = re.split(r'[.!?]+\s+', cleaned_text)
        
        # Remove empty sentences
        sentences = [s.strip() for s in sentences if s.strip()]
        
        # Build response sentence by sentence, prioritizing complete sentences
        final_response = ""
        for i, sentence in enumerate(sentences):
            # Add proper punctuation if missing
            if sentence and not sentence[-1] in '.!?':
                sentence += '.'
            
            # Check if adding this sentence would exceed the limit
            test_response = final_response + (' ' if final_response else '') + sentence
            
            # Always include the first sentence, even if it's long
            if i == 0 or len(test_response) <= 440:  # Increased buffer to 440 for complete sentences
                final_response = test_response
            else:
                # This sentence would make us exceed the reasonable limit, so stop here
                # But we prioritize having a complete final sentence over strict character limits
                break
                
            # Limit to maximum 5 sentences for SMS readability
            if i >= 4:  # 0-indexed, so this is the 5th sentence
                break
        
        # If we still have no response or it's too short, try to salvage something
        if not final_response or len(final_response) < 20:
            # Take first sentence and truncate it smartly if needed
            first_sentence = sentences[0] if sentences else cleaned_text
            if len(first_sentence) > 395:  # Leave room for period
                # Find last complete word that fits
                words = first_sentence.split()
                truncated = ""
                for word in words:
                    test_text = truncated + (' ' if truncated else '') + word
                    if len(test_text) <= 395:
                        truncated = test_text
                    else:
                        break
                final_response = truncated + '.'
            else:
                final_response = first_sentence if first_sentence.endswith(('.', '!', '?')) else first_sentence + '.'
        
        # Final length check and logging - more lenient for complete sentences
        if len(final_response) > 500:  # Only apply emergency truncation if extremely long
            logger.warning("Response significantly exceeds reasonable limit", extra={
                "transaction_id": transaction_id,
                "response_length": len(final_response)
            })
            # Emergency truncation only for extremely long responses
            # Find last complete sentence that fits within 440 characters
            emergency_sentences = re.split(r'[.!?]+\s+', final_response)
            emergency_response = ""
            for sentence in emergency_sentences:
                if sentence and not sentence[-1] in '.!?':
                    sentence += '.'
                test_text = emergency_response + (' ' if emergency_response else '') + sentence
                if len(test_text) <= 440:
                    emergency_response = test_text
                else:
                    break
            final_response = emergency_response if emergency_response else final_response[:440].rsplit(' ', 1)[0] + '.'
        
        sentence_count = len([s for s in re.split(r'[.!?]+\s+', final_response) if s.strip()])
        logger.info("Response validated", extra={
            "transaction_id": transaction_id,
            "original_length": len(text),
            "final_length": len(final_response),
            "sentence_count": sentence_count
        })
        
        return final_response
        
    except Exception as e:
        logger.error("Error in response validation", extra={
            "transaction_id": transaction_id,
            "error": str(e)
        })
        # Return a safe fallback
        return "Unable to process response properly."

def clean_response(text: str) -> str:
    """Enhanced response cleaning function"""
    try:
        # Remove markdown formatting
        text = re.sub(r'[*#`_~]', '', text)
        # Remove citations and reference markers
        text = re.sub(r'\[\d*\]', '', text)
        # Remove extra whitespace
        text = ' '.join(text.split()).strip()
        return text
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
        # VERSION 11 CODE: Replace this line:
        # is_authorized, user_email = is_user_authorized_optimized(
        #     sender_phone, destination_number, transaction_id
        # )

        # With this:
        is_authorized, user_email = is_user_authorized_dynamodb_only(
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
            logger.info("User query processed", extra={
                "transaction_id": transaction_id,
                "phone": sender_phone,
                "email": user_email or "none",
                "query": original_query,
                "response": perplexity_response[:280],
                "version": version
            })
            send_sms(sender_phone, perplexity_response, destination_number, transaction_id)
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
