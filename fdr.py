import boto3
import json, gzip
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class FDRQueue(object):
  def __init__(self, access_key, secret, queue_url):
    self.sqs_cli = boto3.client("sqs", aws_access_key_id=access_key, aws_secret_access_key=secret, region_name="us-west-1")
    self.queue_url = queue_url

  @contextmanager
  def receive_messages(self, should_delete=True):
    logger.info(f'trying to receive messages from the queue: {self.queue_url}')
    sqs_messages = self.sqs_cli.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, VisibilityTimeout=600)
    metadata = sqs_messages['ResponseMetadata']
    if metadata['HTTPStatusCode'] == 200:
      if 'Messages' in sqs_messages:
        messages = [SQSMessage(self, msg['MessageId'], msg['ReceiptHandle'], msg['Body']) for msg in sqs_messages['Messages']]
        yield messages
        if should_delete:
          self.sqs_cli.delete_message_batch(QueueUrl=self.queue_url, Entries=[msg.entry for msg in messages])
      else:
        yield []
    else:
      err_msg = 'ResponseMetadata is None or doesn\'t have HTTPStatusCode' if metadata is None or ('HTTPStatusCode' not in metadata) else None
      if not err_msg:
        err_msg = f'StatusCode = {metadata["HTTPStatusCode"]}'
        if 'Error' in metadata:
          err_msg += f', ErrorCode = {metadata["Error"]["Code"]}, ErrorMessage = {metadata["Error"]["Message"]}'
      err_msg = f'Failed to receive messages. {err_msg}'
      logger.error(err_msg)
      raise RuntimeError(err_msg)
  def delete_message(self, message_id, receipt_handle):
    self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
      
class SQSMessage(object):
  def __init__(self, sqs_client, message_id, receipt_handle, body):
    self.body = json.loads(body)
    self.message_id = message_id
    self.receipt_handle = receipt_handle
    self.sqs_client = sqs_client
  def delete_message(self):
    self.sqs_client.delete_message(self.message_id, self.receipt_handle)

  @property
  def entry(self):
    return {"Id": self.message_id, "ReceiptHandle": self.receipt_handle}

def _concat(raw_payload):
  events = raw_payload['Payload']
  raw_bytes = b''
  end_event_received = False
  for e in events:
    if 'Records' in e:
      raw_bytes += e['Records']['Payload']
    elif 'Stats' in e:
      stats = e['Stats']['Details']
      logger.debug(f'S3 Select Stats. BytesScanned: {stats["BytesScanned"]}, BytesProcessed: {stats["BytesProcessed"]}, BytesReturned: {stats["BytesReturned"]}')
    elif 'End' in e:
      end_event_received = True
    else:
      logger.warning(f'Unknown type of event received: keys={e.keys()}')
  return raw_bytes, end_event_received

class Replicator(object):
  def __init__(self, src_s3_client, dest_s3_client):
    self.src_client = src_s3_client
    self.dest_client = dest_s3_client
  def on_message_received(self, message, query=None):
    for fl in message.body['files']:
      if query:
        raw = self.src_client.select_object_content(Bucket=message.body['bucket'], Key=fl['path'], InputSerialization={'JSON':{'Type':'LINES'},'CompressionType':'GZIP'},OutputSerialization={'JSON':{'RecordDelimiter':'\n'}},ExpressionType='SQL',Expression=query)
        rawbytes, succeeded = _concat(raw)
        if succeeded:
          objbytes = gzip(_concat(raw), compressionlevel=9)
        else:
          logger.error('End event not received.')
          return
      else:
        obj = self.src_client.get_object(Bucket=message.body['bucket'], Key=fl['path'])
        objbytes = obj['Body'].read()
      key_suffix = fl['path'][5:].replace("/", "_")
      date_string = (datetime.utcfromtimestamp(message.body['timestamp'] / 1000) + timedelta(hours=9)).strftime('year=%Y/month=%m/day=%d/hour=%H')
      key = f'{date_string}/{key_suffix}'
      self.dest_client.put_object(Key=key, Body=objbytes)

class S3(object):
  def __init__(self, region=None, access_key_id=None, secret_access_key=None, endpoint_url=None, prefix=None, bucket=None):
    if region and not (access_key_id or secret_access_key):
      self.s3cli = boto3.client("s3", region_name=region)
    elif region and access_key_id and secret_access_key:
      self.s3cli = boto3.client("s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region)
    elif endpoint_url:
      self.s3cli = boto3.client("s3", endpoint_url=endpoint_url)
    else:
      self.s3cli = boto3.client("s3")
    self.prefix = prefix
    self.bucket = bucket
  def get_object(self, **kwargs):
    return self.s3cli.get_object(**kwargs)
  def select_object_content(self, **kwargs):
    return self.s3cli.select_object_content(**kwargs)
  def put_object(self, **kwargs):
    if self.prefix:
      kwargs['Key'] = self.prefix + kwargs['Key']
    if self.bucket:
      kwargs['Bucket'] = self.bucket
    return self.s3cli.put_object(**kwargs)

def handler(event, context):
  import os
  queue_url = os.environ['SQS_QUEUE_URL']
  access_key_id = os.environ['ACCESS_KEY_ID']
  secret_access_key = os.environ['SECRET_ACCESS_KEY']
  dest_bucket = os.environ['TARGET_BUCKET']
  dest_prefix = os.environ['LOG_PREFIX']
  source_s3 = S3(region='us-west-1', access_key_id=access_key_id, secret_access_key=secret_access_key)
  dest_s3 = S3(prefix=dest_prefix, bucket=dest_bucket)
  replicator = Replicator(source_s3, dest_s3)
  sqs_queue = FDRQueue(access_key_id, secret_access_key, queue_url)
  count = 0
  try:
    for i in range(6):
      with sqs_queue.receive_messages(should_delete=True) as messages:
        for message in messages:
          replicator.on_message_received(message)
          count += 1
  except RuntimeError as e:
    return { 'message': 'failed', 'error': str(e) }
  return { 'message': f'succeeded. {count} object coppied.', 'error': None }
