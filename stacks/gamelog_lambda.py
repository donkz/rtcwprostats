from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda

from aws_cdk.aws_dynamodb import Table


class GamelogLambdaStack(Stack):
    """Lambda to react to incoming files."""

    def __init__(self, scope: Construct, id: str,
                 lambda_tracing,
                 ddb_table: Table,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ddb_lambda_role = iam.Role(self, "Lambda-gamelog-role",
                                   role_name='rtcwpro-lambda-gamelog-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )
        ddb_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(ddb_lambda_role)

        gamelog_lambda = _lambda.Function(
            self, 'gamelog-lambda',
            function_name='rtcwpro-gamelog',
            code=_lambda.Code.from_asset('lambdas/postprocessing/gamelog'),
            handler='gamelog.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=ddb_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(30),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

        self.gamelog_lambda = gamelog_lambda
