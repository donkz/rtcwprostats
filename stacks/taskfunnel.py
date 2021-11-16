from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_sns as sns

from aws_cdk.aws_dynamodb import Table


class TaskFunnelStack(Stack):
    """Make a step function state machine with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str, lambda_tracing, ddb_table: Table, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        fail_topic = sns.Topic(self, "Funnel Failure Topic")

        funnel_lambda_role = iam.Role(self, "Lambda-funnel-role",
                                      role_name='rtcwpro-lambda-funnel-role',
                                      assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                      )
        funnel_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(funnel_lambda_role)

        group_cacher_lambda = _lambda.Function(
            self, 'gather-cacher-lambda',
            function_name='rtcwpro-gather-cacher',
            code=_lambda.Code.from_asset('lambdas/tasks/group_cacher'),
            handler='group_cacher.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=funnel_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(30),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

        send_failure_notification = tasks.SnsPublish(self, "Funnel Failure",
                                                     topic=fail_topic,
                                                     integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
                                                     message=sfn.TaskInput.from_text("Process Failure")
                                                     )

        group_cacher_task = tasks.LambdaInvoke(self, "Group Cache Task", lambda_function=group_cacher_lambda)
        group_cacher_task.add_catch(send_failure_notification)

        choice = sfn.Choice(self, "What task?")

        choice.when(sfn.Condition.string_equals("$.tasktype", "group_cacher"), group_cacher_task)
        # choice.when(sfn.Condition.number_equals("$.tasktype", "new_player"), new_player_task)
        # choice.when(sfn.Condition.number_equals("$.tasktype", "new_server"), new_server_task)
        choice.otherwise(send_failure_notification)

        funnel_state_machine = sfn.StateMachine(self, "ProcessMatchData",
                                                definition=choice,
                                                timeout=Duration.minutes(5),
                                                state_machine_type=sfn.StateMachineType.EXPRESS
                                                )

        self.funnel_state_machine = funnel_state_machine
