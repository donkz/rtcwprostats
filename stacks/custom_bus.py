from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam

from aws_cdk.aws_dynamodb import Table


class CustomBusStack(Stack):
    """Make an event bus with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str, lambda_tracing, ddb_table: Table, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        event_lambda_role = iam.Role(self, "Lambda-funnel-role",
                                     role_name='rtcwpro-lambda-event-role',
                                     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                     )
        event_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(event_lambda_role)

        discord_event_lambda = _lambda.Function(
            self, 'discord-event-lambda',
            function_name='rtcwpro-discord-event',
            code=_lambda.Code.from_asset('lambdas/events/discord_event'),
            handler='discord_event.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=event_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(10),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

        bus = events.EventBus(self, "bus")

        eventPattern = events.EventPattern(source=["rtcwpro-pipeline"],
                                           detail_type=["Discord notification"]
                                           )
        lambda_target_discord_event = targets.LambdaFunction(handler=discord_event_lambda)

        events.Rule(self,
                    id="Discord event",
                    rule_name="rtcwpro-discord-event",
                    targets=[lambda_target_discord_event],
                    description="Announce events to discord",
                    event_bus=bus,
                    event_pattern=eventPattern,
                    )

        self.custom_bus = bus
