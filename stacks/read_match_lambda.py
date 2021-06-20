from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    core
)

from aws_cdk.aws_dynamodb import (
    Table
)

from aws_cdk.aws_lambda_event_sources import SqsEventSource


class ReadMatchStack(core.Stack):
    """Lambda to react to incoming files."""

    def __init__(self, scope: core.Construct, id: str,
                 lambda_tracing,
                 ddb_table: Table,
                 read_queue: sqs.Queue,
                 read_dlq: sqs.Queue,
                 storage_bucket: s3.Bucket,
                 postproc_state_machine: sfn.StateMachine,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        read_match_role = iam.Role(self, "ReadMatchRole",
                                   role_name='rtcwpro-lambda-read-match-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )
        read_match_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        read_match = _lambda.Function(
            self, 'read_match',
            function_name='rtcwpro-read-match',
            handler='read_match.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/storage/read_match'),
            role=read_match_role,
            tracing=lambda_tracing,
            timeout=core.Duration.seconds(10),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                'RTCWPROSTATS_MATCH_STATE_MACHINE': postproc_state_machine.state_machine_arn
            }
        )

        ddb_table.grant_read_write_data(read_match)
        read_queue.grant_consume_messages(read_match)
        postproc_state_machine.grant_start_execution(read_match)
        storage_bucket.grant_read(read_match, "intake/*")
        
        read_match.add_event_source(SqsEventSource(read_queue, batch_size=1))
        
        read_dlq_role = iam.Role(self, "ReadDLQRole",
                                   role_name='rtcwpro-lambda-read-dlq-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )
        read_dlq_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        read_match_dlq = _lambda.Function(
            self, 'read_match_dlq',
            function_name='rtcwpro-read-match-dlq',
            handler='read_match_dlq.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/storage/read_match_dlq'),
            role=read_dlq_role,
            tracing=lambda_tracing,
            timeout=core.Duration.seconds(10),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                'RTCWPROSTATS_MATCH_STATE_MACHINE': postproc_state_machine.state_machine_arn
            }
        )

        read_dlq.grant_consume_messages(read_dlq_role)
        storage_bucket.grant_read(read_dlq_role, "intake/*")
        storage_bucket.grant_delete(read_dlq_role, "intake/*")
        storage_bucket.grant_write(read_dlq_role, "reader_dlq/*")
        
        read_match_dlq.add_event_source(SqsEventSource(read_dlq, batch_size=1))
        
        self.read_match = read_match