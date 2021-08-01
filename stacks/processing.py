from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam


class ProcessingStack(Stack):
    """Processes that take place after the match had been saved."""

    def __init__(self, scope: Construct, construct_id: str, lambda_tracing, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        classifier_role = iam.Role(self, "ClassifierRole",
                                   role_name='rtcwpro-lambda-classifier-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )

        classifier_lambda = _lambda.Function(
            self, 'classifier',
            function_name='rtcwpro-classifier',
            code=_lambda.Code.from_asset('lambdas/processing/classifier'),
            handler='classifier.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=classifier_role,
            tracing=lambda_tracing
        )

        # Run every hour
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
        rule = events.Rule(
            self, "Rule",
            rule_name="hourly_cleanser",
            schedule=events.Schedule.cron(
                minute='0',
                hour='0',  # change this to */1 for hourly
                month='*/1',  # tmp
                week_day='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(classifier_lambda))
