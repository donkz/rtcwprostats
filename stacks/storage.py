from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_s3_notifications as s3n,
    core
)


class StorageStack(core.Stack):
    
    def __init__(self, scope: core.Construct, id: str, save_payload: _lambda.IFunction, read_match: _lambda.IFunction, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        storage_bucket = s3.Bucket(self, "MainStorage",
                            bucket_name="rtcwprostats",
                            versioned= True,
                            encryption=s3.BucketEncryption.S3_MANAGED,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                            removal_policy = core.RemovalPolicy.RETAIN,
                            lifecycle_rules=[
                                    s3.LifecycleRule(id="ExpireDebugFiles", expiration=core.Duration.days(30),prefix="debug/"),
                                    s3.LifecycleRule(id="ExpireOldVersions", noncurrent_version_expiration =core.Duration.days(30)),
                                    s3.LifecycleRule(id="Transitions", 
                                                     transitions = [
                                                             s3.Transition(storage_class=s3.StorageClass.ONE_ZONE_INFREQUENT_ACCESS, transition_after=core.Duration.days(30))
                                                             ]
                                                     )
                                    ]
                        )
        
        core.Tags.of(storage_bucket).add("purpose", "rtcwpro")
        
        user = iam.User(self, "rtcwproadmin")
        storage_bucket.grant_read(user, "*")
        
        storage_bucket.grant_put(save_payload, ["intake/*"])
        storage_bucket.grant_put(save_payload, ["intake_dlq/*"])
        
        storage_bucket.grant_read(read_match, "intake/*")
        storage_bucket.grant_put(read_match, "process_dlq/*")
               
        
#        storage_bucket.add_to_resource_policy(
#                iam.PolicyStatement(
#                        actions=["s3:GetObject"],
#                        resources=[
#                                storage_bucket.arn_for_objects("intake/*")
#                                ],
#                        principals=[iam.AccountRootPrincipal()]
#                ))
        
        ops_topic = sns.Topic(self, "RTCWPro-notifications")
        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SnsDestination(ops_topic), 
                                              s3.NotificationKeyFilter(
                                                      prefix="intake_dlq/",
                                                      suffix="*.json")
                                              )
        #storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, read_match, s3.NotificationKeyFilter(prefix="intake/")) 
        
        lambda_start_sf_role = iam.Role(self, "S3LambdaTriggerRole",
                role_name = 'rtcwpro-lambda-start-sf-role',
                assumed_by= iam.ServicePrincipal("lambda.amazonaws.com")
                )
        lambda_start_sf_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        
        lambda_start_sf = _lambda.Function(
            self, 'lambda_start_sf',
            function_name  = 'rtcwpro-lambda-start-sf',
            handler='rtcwpro-lambda-start-sf.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/storage'),
            role=lambda_start_sf_role
         )
        notification = s3n.LambdaDestination(lambda_start_sf)
        storage_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification, s3.NotificationKeyFilter(prefix="intake/"))
        
        self.storage_bucket = storage_bucket