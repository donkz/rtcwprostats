from aws_cdk import (
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    core
)

from aws_cdk.aws_dynamodb import (
    Table,
    Attribute,
    AttributeType,
#    StreamViewType,
    BillingMode,
    TableEncryption
)


class DeliveryStack(core.Stack):
    
    def __init__(self, scope: core.Construct, id: str, ddb_table: Table, api : apigw.RestApi, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        ####################################
        #Lambda role and function
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
            handler='rtcwpro-retriever.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/delivery'),
            role=retriever_role
        )
        
        retriever_integration = apigw.LambdaIntegration(retriever)
        
        ddb_table.grant_read_data(retriever_role)
        
        ####################################
        #LQuery APIs
        ####################################
     
# =============================================================================
#         API
#         ---submit (defined in api stack)
#         ---matches
#         |
#         ---stats
#         |------player
#         |------type
# =============================================================================
        
        matches = api.root.add_resource("matches")
        matches.add_method("GET", retriever_integration)
        
        matches_proxy = matches.add_proxy(default_integration=retriever_integration,any_method=False)
        matches_proxy.add_method("GET")
        
        statsall = api.root.add_resource("stats")
        statsall.add_method("GET", retriever_integration)
        
        stats_player = statsall.add_resource("player")
        stats_player.add_method("GET", retriever_integration)
        
        stats_type = statsall.add_resource("type")
        stats_type.add_method("GET", retriever_integration)
        
        


