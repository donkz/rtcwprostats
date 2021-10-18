from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions as sfn


from aws_cdk.aws_dynamodb import Table



class DeliveryWriterStack(Stack):
    """Public API for retrieving match and player data."""

    def __init__(self, scope: Construct, id: str, ddb_table: Table, funnel_sf: sfn.StateMachine,  lambda_tracing, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        delivery_writer_role = iam.Role(self, "RtcwproDeliveryWriter",
                                  role_name='rtcwpro-lambda-delivery-writer-role',
                                  assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                  )
        delivery_writer_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        delivery_writer = _lambda.Function(
            self, 'delivery-writer',
            function_name='rtcwpro-delivery-writer',
            handler='delivery_writer.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('lambdas/delivery/delivery_writer'),
            role=delivery_writer_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(10),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                'RTCWPROSTATS_FUNNEL_STATE_MACHINE': funnel_sf.state_machine_arn
            }
        )

        ddb_table.grant_write_data(delivery_writer_role)
        funnel_sf.grant_start_execution(delivery_writer)
        self.delivery_writer_lambda = delivery_writer
