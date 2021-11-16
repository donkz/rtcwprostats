from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_sns as sns

from aws_cdk.aws_dynamodb import Table


class PostProcessStack(Stack):
    """Make a step function state machine with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str, lambda_tracing, ddb_table: Table, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        fail_topic = sns.Topic(self, "Postprocessing Failure Topic")
        # success_topic = sns.Topic(self, "Postprocessing Success Topic")

        ddb_lambda_role = iam.Role(self, "Lambda-ddb-role",
                                   role_name='rtcwpro-lambda-postprocessing-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )
        ddb_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(ddb_lambda_role)

        elo_lambda = _lambda.Function(
            self, 'elo-lambda',
            function_name='rtcwpro-elo',
            code=_lambda.Code.from_asset('lambdas/postprocessing/elo'),
            handler='elo.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=ddb_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(30),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

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

        summary_lambda = _lambda.Function(
            self, 'summary-lambda',
            function_name='rtcwpro-stats-summary',
            code=_lambda.Code.from_asset('lambdas/postprocessing/summary'),
            handler='summary.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=ddb_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(30),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

        discord_match_notify_lambda = _lambda.Function(
            self, 'discord-match-notify-lambda',
            function_name='rtcwpro-discord-match-notify',
            code=_lambda.Code.from_asset('lambdas/postprocessing/discord'),
            handler='discord-match-notify.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=ddb_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(10),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
            }
        )

# =============================================================================
#         wsummary_lambda = _lambda.Function(
#             self, 'wsummary-lambda',
#             function_name='rtcwpro-wstats-summary',
#             code=_lambda.Code.asset('lambdas/postprocessing/wsummary'),
#             handler='wsummary.handler',
#             runtime=_lambda.Runtime.PYTHON_3_8,
#             role=ddb_lambda_role,
#             tracing=lambda_tracing
#         )
# =============================================================================

        send_failure_notification = tasks.SnsPublish(self, "Postprocessing Failure",
                                                     topic=fail_topic,
                                                     integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
                                                     message=sfn.TaskInput.from_text("Process Failure")
                                                     )

        # success = tasks.SnsPublish(self, "Postprocessing Success",
        #                            topic=success_topic,
        #                            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        #                            message=sfn.TaskInput.from_text("Process success!")
        #                            )

        Round1Processing = tasks.LambdaInvoke(self, "Discord round1 notify", lambda_function=discord_match_notify_lambda)

        Discordmatch = tasks.LambdaInvoke(self, "Discord match notify", input_path="$.matchid", lambda_function=discord_match_notify_lambda)
        ELO = tasks.LambdaInvoke(self, "Calculate Elo", input_path="$.matchid", result_path="$.Payload", lambda_function=elo_lambda).next(Discordmatch)
        Summary = tasks.LambdaInvoke(self, "Summarize stats", input_path="$.matchid", lambda_function=summary_lambda)
        Gamelog = tasks.LambdaInvoke(self, "Process gamelog", input_path="$.matchid", lambda_function=gamelog_lambda)

        Round2Processing = sfn.Parallel(self, "Do the work in parallel")
        Round2Processing.branch(ELO)
        Round2Processing.branch(Summary)
        Round2Processing.branch(Gamelog)

        Round2Processing.add_catch(send_failure_notification)
        # Round2Processing.next(success)

        choice = sfn.Choice(self, "Round 1 or 2")

        choice.when(sfn.Condition.number_equals("$.roundid", 1), Round1Processing)
        choice.when(sfn.Condition.number_equals("$.roundid", 2), Round2Processing)
        choice.otherwise(send_failure_notification)

        postproc_state_machine = sfn.StateMachine(self, "ProcessMatchData",
                                                  definition=choice,
                                                  timeout=Duration.minutes(5),
                                                  state_machine_type=sfn.StateMachineType.EXPRESS
                                                  )

        self.postproc_state_machine = postproc_state_machine
