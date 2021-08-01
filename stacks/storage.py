from aws_cdk import Stack, Duration, RemovalPolicy
from constructs import Construct

import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_s3_notifications as s3n

from aws_cdk.aws_dynamodb import Table


class StorageStack(Stack):
    """S3 bucket for incoming files and reader lambda."""

    def __init__(self, scope: Construct, id: str,
                 lambda_tracing,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        storage_bucket = s3.Bucket(self, "MainStorage",
                                   bucket_name="rtcwprostats",
                                   versioned=True,
                                   encryption=s3.BucketEncryption.S3_MANAGED,
                                   # block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                   public_read_access=True,
                                   removal_policy=RemovalPolicy.RETAIN,
                                   lifecycle_rules=[
                                       s3.LifecycleRule(id="ExpireDebugFiles", expiration=Duration.days(30), prefix="debug/"),
                                       s3.LifecycleRule(id="ExpireOldVersions", noncurrent_version_expiration=Duration.days(30)),
                                       s3.LifecycleRule(id="Transitions",
                                                        transitions=[
                                                           s3.Transition(storage_class=s3.StorageClass.ONE_ZONE_INFREQUENT_ACCESS, transition_after=Duration.days(30))
                                                        ]
                                                        )
                                   ]
                                   )

        user = iam.User(self, "rtcwproadmin")
        storage_bucket.grant_read(user, "*")

        ops_topic = sns.Topic(self, "RTCWPro-notifications")
        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SnsDestination(ops_topic),
                                              s3.NotificationKeyFilter(prefix="intake_dlq/")
                                              )
        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SnsDestination(ops_topic),
                                              s3.NotificationKeyFilter(prefix="reader_dlq/")
                                              )
        
        read_dlq = sqs.Queue(self, id="ReadMatchDLQ")
        read_queue = sqs.Queue(self, "ReadMatchQueue",
                               visibility_timeout=Duration.seconds(60),
                               dead_letter_queue = sqs.DeadLetterQueue(max_receive_count=1, queue=read_dlq))
        sqs_notification = s3n.SqsDestination(read_queue)
        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, sqs_notification, s3.NotificationKeyFilter(prefix="intake/"))
        
#        notification = s3n.LambdaDestination(read_match)
#        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification, s3.NotificationKeyFilter(prefix="intake/"))

        self.storage_bucket = storage_bucket
        self.read_queue = read_queue
        self.read_dlq = read_dlq
