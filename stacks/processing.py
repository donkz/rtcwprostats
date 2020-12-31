from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    core
)

class ProcessingStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cleanser_role = iam.Role(self, "ClassifierRole",
                role_name = 'rtcwpro-lambda-classifier-role',
                assumed_by= iam.ServicePrincipal("lambda.amazonaws.com")
                )
        
        cleanser_lambda = _lambda.Function(
            self, 'read_match',
            function_name  = 'rtcwpro-classifier',
            code=_lambda.Code.asset('lambdas/processing/classifier'),
            handler='rtcwpro-classifier.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role = cleanser_role
         )

        # Run every hour
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
        rule = events.Rule(
            self, "Rule",
            rule_name = "hourly_cleanser",
            schedule=events.Schedule.cron(
                minute='0',
                hour='*/1',
                month='*',
                week_day='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(cleanser_lambda))

