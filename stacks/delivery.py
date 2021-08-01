from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_apigateway as apigw


class DeliveryStack(Stack):
    """Public API for retrieving match and player data."""

    def __init__(self, scope: Construct, id: str, api: apigw.RestApi, retriever: _lambda.IFunction, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        retriever_integration = apigw.LambdaIntegration(retriever)

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
#10       ---/player/{player_guid}
#20       |---------/search/{begins_with}
        
          

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
        
        #10
        player = api.root.add_resource("player")
        
        player_info = player.add_resource("{player_guid}")
        player_info.add_method("GET", retriever_integration)
        
        #20
        player_search = player.add_resource("search")
        player_search_guid = player_search.add_resource("{begins_with}")
        player_search_guid.add_method("GET", retriever_integration)
        
        
