from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam


from aws_cdk.aws_dynamodb import Table


class DeliveryRetrieverStack(Stack):
    """Public API for retrieving match and player data."""

    def __init__(self, scope: Construct, id: str, ddb_table: Table, lambda_tracing, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        retriever_role = iam.Role(self, "RtcwproRetriever",
                                  role_name='rtcwpro-lambda-retriever-role',
                                  assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                  )
        retriever_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        retriever = _lambda.Function(
            self, 'retriever',
            function_name='rtcwpro-retriever',
            handler='retriever.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset('lambdas/delivery/retriever'),
            role=retriever_role,
            tracing=lambda_tracing,
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

        ddb_table.grant_read_data(retriever_role)
        self.retriever_lambda = retriever
