from aws_cdk import (
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    core
)

from aws_cdk.aws_dynamodb import (
    Table
)


class DeliveryStack(core.Stack):
    """Public API for retrieving match and player data."""

    def __init__(self, scope: core.Construct, id: str, ddb_table: Table, api: apigw.RestApi, lambda_tracing, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ####################################
        # Lambda role and function
        ####################################
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
            code=_lambda.Code.asset('lambdas/delivery/retriever'),
            role=retriever_role,
            tracing=lambda_tracing
        )

        retriever_integration = apigw.LambdaIntegration(retriever)

        ddb_table.grant_read_data(retriever_role)

        ####################################
        # Query APIs
        ####################################

# =============================================================================
#         API
#         ---submit (defined in api stack)
# 1       ---matches
# 2       |------{proxy+}
# 3       ---stats/{match_id}
# 4       |-------/player/{guid}
# 5       |-------/type/{type}
# 6       ---wstats/{match_id}
# 7       |-------/player/{player_guid}
# 8       |-------/player/{player_guid}/match/{match_id}
# 9       ---gamelogs/{match_round_id}

# =============================================================================

        # 1
        matches = api.root.add_resource("matches")

        # 2
        matches_proxy = matches.add_proxy(default_integration=retriever_integration, any_method=False)
        matches_proxy.add_method("GET")
        # 3
        statsall = api.root.add_resource("stats")

        stats_match_id = statsall.add_resource("{match_id}")
        stats_match_id.add_method("GET", retriever_integration)
        # 4
        stats_player = statsall.add_resource("player")
        # stats_player.add_method("GET", retriever_integration)

        stats_player_guid = stats_player.add_resource("{player_guid}")
        stats_player_guid.add_method("GET", retriever_integration)
        # 5
        stats_type = statsall.add_resource("type")

        stats_type_type = stats_type.add_resource("{type}")
        stats_type_type.add_method("GET", retriever_integration)

        # 6
        wstatsall = api.root.add_resource("wstats")
        wstats_match_id = wstatsall.add_resource("{match_id}")
        wstats_match_id.add_method("GET", retriever_integration)
        # 7
        wstats_player = wstatsall.add_resource("player")
        wstats_player_guid = wstats_player.add_resource("{player_guid}")
        wstats_player_guid.add_method("GET", retriever_integration)

        # 8
        wstats_player_guid_match = wstats_player_guid.add_resource("match")
        wstats_player_guid_match_id = wstats_player_guid_match.add_resource("{match_id}")
        wstats_player_guid_match_id.add_method("GET", retriever_integration)
        # 9
        gamelogs = api.root.add_resource("gamelogs")
        gamelog_match_id = gamelogs.add_resource("{match_round_id}")
        gamelog_match_id.add_method("GET", retriever_integration)
