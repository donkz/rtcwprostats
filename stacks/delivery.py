from aws_cdk import Stack
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_apigateway as apigw


class DeliveryStack(Stack):
    """Public API for retrieving match and player data."""

    def __init__(self, scope: Construct, id: str, api: apigw.RestApi, retriever: _lambda.IFunction, delivery_writer: _lambda.IFunction, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        retriever_integration = apigw.LambdaIntegration(retriever)
        delivery_writer_integration = apigw.LambdaIntegration(delivery_writer)

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
#30       ---/server
#31       |---------/region/{region}
        
          

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
        
        stats_player_guid_region_type = stats_player_guid.add_resource("region").add_resource("{region}").add_resource("type").add_resource("{type}")
        stats_player_guid_region_type.add_method("GET", retriever_integration)
        
        stats_group = statsall.add_resource("group").add_resource("{group_name}")
        stats_group.add_method("GET", retriever_integration)

        # 5
        # stats_type = statsall.add_resource("type")

        # stats_type_type = stats_type.add_resource("{type}")
        # stats_type_type.add_method("GET", retriever_integration)

        # 6
        wstatsall = api.root.add_resource("wstats")
        wstats_match_id = wstatsall.add_resource("{match_id}")
        wstats_match_id.add_method("GET", retriever_integration)
        
        wstats_group = wstatsall.add_resource("group").add_resource("{group_name}")
        wstats_group.add_method("GET", retriever_integration)
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
        
        #30
        servers = api.root.add_resource("servers")
        servers.add_method("GET", retriever_integration)
        
        #31
        servers_region = servers.add_resource("region")
        servers_region_name = servers_region.add_resource("{region}")
        servers_region_name.add_method("GET", retriever_integration)
        
        #31.5
        servers_region_name_active = servers_region_name.add_resource("active")
        servers_region_name_active.add_method("GET", retriever_integration)
        
        #32
        # servers_detail = servers.add_resource("detail")
        # servers_detail.add_method("GET", retriever_integration)
        
        #40
        groups = api.root.add_resource("groups")
        groups.add_method("GET", retriever_integration)
        
        #41
        groups_proxy = groups.add_proxy(default_integration=retriever_integration, any_method=False)
        groups_proxy.add_method("GET")
        
        #49
        groups_add = groups.add_resource("add")
        groups_add.add_method("POST", delivery_writer_integration, api_key_required=True)
        
        #50
        leaders_cat_region_name_type_name = api.root.add_resource("leaders").add_resource("{category}").add_resource("region").add_resource("{region}").add_resource("type").add_resource("{type}")
        leaders_cat_region_name_type_name.add_method("GET", retriever_integration)
        
        #51
        leaders_cat_region_name_type_name_limit = leaders_cat_region_name_type_name.add_resource("limit").add_resource("{limit}")
        leaders_cat_region_name_type_name_limit.add_method("GET", retriever_integration)
        
        #60
        eloprogress = api.root.add_resource("eloprogress")
        eloprogress_player = eloprogress.add_resource("player").add_resource("{player_guid}").add_resource("region").add_resource("{region}").add_resource("type").add_resource("{type}")
        eloprogress_player.add_method("GET", retriever_integration)
        
        #61
        eloprogress_match = eloprogress.add_resource("match").add_resource("{match_id}")
        eloprogress_match.add_method("GET", retriever_integration)
        
        #70
        aliases = api.root.add_resource("aliases")
        aliases_player = aliases.add_resource("player").add_resource("{player_guid}")
        aliases_player.add_method("GET", retriever_integration)
        
        #71
        aliases_search = aliases.add_resource("search").add_resource("{begins_with}")
        aliases_search.add_method("GET", retriever_integration)
        
        #72
        aliases_recent = aliases.add_resource("recent").add_resource("limit").add_resource("{limit}")
        aliases_recent.add_method("GET", retriever_integration)
        
        
        
        
        

        



        
        
