import boto3

def send_message(topic_arn, message):
	sns = boto3.client('sns', region_name='eu-west-1')
	response = sns.publish(TopicArn=topic_arn, Message=message)
	return response

class Messenger:
	def __init__(self, topic_arn):
		self._topic_arn = topic_arn

	def send(self, message):
		return send_message(topic_arn=self._topic_arn, message=message)





