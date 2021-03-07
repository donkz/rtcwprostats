"""Import aws cdk modules."""
from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    core
)


class ProcessingStack(core.Stack):
    """Processes that take place after the match had been saved."""

    def __init__(self, scope: core.Construct, construct_id: str, lambda_tracing, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        classifier_role = iam.Role(self, "ClassifierRole",
                                   role_name='rtcwpro-lambda-classifier-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )

        classifier_lambda = _lambda.Function(
            self, 'classifier',
            function_name='rtcwpro-classifier',
            code=_lambda.Code.asset('lambdas/processing/classifier'),
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
