import json
import boto3
import os
import logging
import re
import urllib3
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sns = boto3.client('sns')
cognito = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
PERPLEXITY_API_URL = os.environ['PERPLEXITY_API_URL']
PERPLEXITY_API_KEY = os.environ['PERPLEXITY_API_KEY']
COGNITO_USER_POOL_ID = os.environ['COGNITO_USER_POOL_ID']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Configure urllib3 with retries
retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
http = urllib3.PoolManager(retries=retries)

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
        # First get the user's cognito_id from Cognito
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            logger.warning(f"Phone number {phone_number} not found in Cognito for subscription update")
            return False
        
        user = cognito_response['Users'][0]
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            logger.warning(f"Cognito ID not found for phone number {phone_number}")
            return False
        
        # Get the user item from DynamoDB to find the record ID
        response = table.scan(
            FilterExpression="cognitoId = :cognito_id",
            ExpressionAttributeValues={
                ":cognito_id": cognito_id
            }
        )
        
        if not response.get('Items'):
            logger.warning(f"No user found in DynamoDB with cognito ID {cognito_id}")
            return False
            
        user_item = response['Items'][0]
        user_id = user_item.get('id')
        
        if not user_id:
            logger.error(f"No 'id' field found for user with cognito ID {cognito_id}")
            return False
        
        # Update the subscription status using the correct primary key
        table.update_item(
            Key={'id': user_id},
            UpdateExpression='SET subscriptionStatus = :status',
            ExpressionAttributeValues={':status': status}
        )
        logger.info(f"Subscription status updated to '{status}' for {phone_number} (cognito_id: {cognito_id})")
        return True
    except ClientError as e:
        logger.error(f"DynamoDB update failed for {phone_number}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error updating subscription status for {phone_number}: {e}")
        return False

def handle_join_request(phone_number):
    """Handle join keyword - reactivate subscription if currently stopped"""
    try:
        # First get the user's cognito_id from Cognito
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            logger.warning(f"Phone number {phone_number} not found in Cognito for join request")
            return False
        
        user = cognito_response['Users'][0]
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            logger.warning(f"Cognito ID not found for phone number {phone_number}")
            return False
        
        # Get user from DynamoDB by cognitoId
        response = table.scan(
            FilterExpression="cognitoId = :cognito_id",
            ExpressionAttributeValues={
                ":cognito_id": cognito_id
            }
        )
        
        if not response.get('Items'):
            logger.warning(f"No user found in DynamoDB with cognito ID {cognito_id} for join request")
            return False
            
        user_item = response['Items'][0]
        current_status = user_item.get('subscriptionStatus')
        
        # Only allow join if current status is 'stopped'
        if current_status != 'stopped':
            logger.info(f"Join request from {phone_number} ignored - subscription status is '{current_status}', not 'stopped'")
            return False
        
        user_id = user_item.get('id')
        
        if not user_id:
            logger.error(f"No 'id' field found for user with cognito ID {cognito_id}")
            return False
        
        # Update subscription status to active using the correct primary key
        table.update_item(
            Key={'id': user_id},
            UpdateExpression='SET subscriptionStatus = :status',
            ExpressionAttributeValues={':status': 'active'}
        )
        logger.info(f"Join request processed for {phone_number} - subscription reactivated from 'stopped' to 'active'")
        return True
        
    except ClientError as e:
        logger.error(f"DynamoDB error during join request for {phone_number}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error processing join request for {phone_number}: {e}")
        return False

def handle_keywords(message_body, phone_number):
    """Handle stop, help, and join keywords - all skip Perplexity response"""
    cleaned_message = validate_and_clean_message(message_body)
    if not cleaned_message:
        return False
    
    if cleaned_message == 'stop':
        # Update subscription status to stopped
        updated = update_subscription_status(phone_number, 'stopped')
        if updated:
            logger.info(f"Stop command processed for {phone_number} - subscription stopped")
        else:
            logger.error(f"Failed to update subscription status for {phone_number}")
        # Skip Perplexity and response (Pinpoint handles stop automatically)
        return True
    elif cleaned_message == 'help':
        logger.info(f"Help command detected from {phone_number}")
        # Skip Perplexity and response (Pinpoint handles help automatically)
        return True
    elif cleaned_message == 'join':
        # Reactivate subscription if currently stopped
        reactivated = handle_join_request(phone_number)
        if reactivated:
            logger.info(f"Join command processed for {phone_number} - subscription reactivated")
        else:
            logger.info(f"Join command from {phone_number} - no action taken")
        # Skip Perplexity and response
        return True
    
    return False

def is_user_authorized(phone_number, destination_number):
    try:
        # Check Cognito first
        cognito_response = cognito.list_users(
            UserPoolId=COGNITO_USER_POOL_ID,
            Filter=f'phone_number = "{phone_number}"'
        )
        
        if not cognito_response['Users']:
            logger.warning(f"Phone number {phone_number} not found in Cognito")
            return False
        
        user = cognito_response['Users'][0]
        user_status = user['UserStatus']
        phone_verified = next((attr for attr in user['Attributes'] if attr['Name'] == 'phone_number_verified'), None)
        
        if not (user_status == 'CONFIRMED' and phone_verified and phone_verified['Value'] == 'true'):
            logger.warning(f"Phone number {phone_number} not confirmed or verified in Cognito")
            return False
        
        # Get the user's 'sub' attribute from Cognito, which should correspond to the cognito_id in DynamoDB
        cognito_id = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), None)
        
        if not cognito_id:
            logger.warning(f"Cognito ID not found for phone number {phone_number}")
            return False
        
        # Query DynamoDB using the user's email (since we need to join users table)
        user_email = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'email'), None)
        
        if not user_email:
            logger.warning(f"Email not found for phone number {phone_number}")
            return False
            
        # Get user from DynamoDB by cognitoId
        response = table.scan(
            FilterExpression="cognitoId = :cognito_id",
            ExpressionAttributeValues={
                ":cognito_id": cognito_id
            }
        )
        
        if not response.get('Items'):
            logger.warning(f"No user found in DynamoDB with cognito ID {cognito_id}")
            return False
            
        user_item = response['Items'][0]
        
        # Check if user has an active subscription
        if user_item.get('subscriptionStatus') != 'active':
            logger.warning(f"User {user_email} does not have an active subscription")
            return False
            
        # Check if user's service phone number matches the destination number
        service_phone_number = user_item.get('servicePhoneNumber')
        
        # If servicePhoneNumber is not set but subscription is active, allow for compatibility
        # This supports users who were created before the servicePhoneNumber field was added
        if not service_phone_number:
            logger.info(f"User {user_email} has active subscription but no assigned service number. Allowing for compatibility.")
            return True
            
        # Check if the destination number matches the user's assigned service number
        if service_phone_number != destination_number:
            logger.warning(f"Destination number {destination_number} does not match user's assigned service number {service_phone_number}")
            return False
        
        logger.info(f"User {user_email} is authorized to use service phone number {destination_number}")
        return True
    
    except ClientError as e:
        logger.error(f"Error checking authorization for {phone_number} to {destination_number}: {str(e)}")
        return False

def lambda_handler(event, context):
    logger.info("Lambda function invoked")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        logger.info(f"Parsed SNS message: {json.dumps(sns_message)}")
        
        sender_phone = sns_message['originationNumber']
        message_content = sns_message['messageBody']
        destination_number = sns_message['destinationNumber']
        logger.info(f"Sender phone: {sender_phone}")
        logger.info(f"Destination number: {destination_number}")
        logger.info(f"Message content: {message_content}")
        
        # Check for keywords first (case-insensitive) - before authorization check
        if handle_keywords(message_content, sender_phone):
            # Both 'stop' and 'help' skip Perplexity and response
            # Pinpoint will handle these keywords automatically
            logger.info("Keyword detected - skipping Perplexity and response")
            return {
                'statusCode': 200,
                'body': json.dumps('Keyword processed, no response sent')
            }
        
        # Check if the sender's phone number is authorized to use this service number
        if not is_user_authorized(sender_phone, destination_number):
            logger.warning(f"Unauthorized: {sender_phone} not authorized for service number {destination_number}")
            return {
                'statusCode': 403,
                'body': json.dumps('Unauthorized phone number for this service')
            }
        
        # Normal processing with Perplexity API
        perplexity_response = query_perplexity(message_content)
        logger.info(f"Perplexity API response: {perplexity_response}")

        cleaned_response = clean_response(perplexity_response)
        logger.info(f"Cleaned response: {cleaned_response}")

        send_sms(sender_phone, cleaned_response, destination_number)  
        
        logger.info("Message processed successfully")
        return {
            'statusCode': 200,
            'body': json.dumps('Message processed successfully')
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing message')
        }
    finally:
        logger.info("Lambda function execution completed")

def clean_response(text):
    # Remove Markdown syntax
    text = re.sub(r'[*#`_~]', '', text)
    # Remove brackets and any numbers inside them (e.g., [3])
    text = re.sub(r'\[\d*\]', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split()).strip()
    return text

def query_perplexity(query):
    logger.info(f"Querying Perplexity API with: {query}")
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
        encoded_payload = json.dumps(payload).encode('utf-8')
        response = http.request(
            'POST',
            PERPLEXITY_API_URL,
            body=encoded_payload,
            headers=headers,
            timeout=urllib3.Timeout(connect=5.0, read=30.0)
        )
        logger.info(f"Perplexity API response status: {response.status}")
        logger.info(f"Perplexity API response headers: {response.headers}")
        logger.info(f"Perplexity API response content: {response.data.decode('utf-8')}")
        
        if response.status == 200:
            result = json.loads(response.data.decode('utf-8'))
            return result['choices'][0]['message']['content']
        else:
            logger.error(f"Error querying Perplexity API: Status {response.status}, Response: {response.data.decode('utf-8')}")
            return f"Error querying Perplexity API: Status {response.status}"
    except urllib3.exceptions.TimeoutError:
        logger.error("Timeout error when querying Perplexity API")
        return "Sorry, the response took too long. Please try again later."
    except urllib3.exceptions.HTTPError as e:
        logger.error(f"HTTP error when querying Perplexity API: {str(e)}")
        return "Sorry, there was an error processing your request. Please try again later."
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return "Sorry, there was an error processing the response. Please try again later."
    except Exception as e:
        logger.error(f"Unexpected error in query_perplexity: {str(e)}", exc_info=True)
        return "Sorry, an unexpected error occurred. Please try again later."

def send_sms(phone_number, message, origination_number):
    logger.info(f"Attempting to send SMS to {phone_number} from {origination_number}")
    try:
        response = sns.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                },
                'AWS.MM.SMS.OriginationNumber': {
                    'DataType': 'String',
                    'StringValue': origination_number
                }
            }
        )
        logger.info(f"SMS sent successfully from {origination_number}. MessageId: {response['MessageId']}")
    except ClientError as e:
        logger.error(f"Error sending SMS from {origination_number}: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"Unexpected error sending SMS from {origination_number}: {str(e)}", exc_info=True)
