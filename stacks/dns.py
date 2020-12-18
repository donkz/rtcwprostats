from aws_cdk import (
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_apigateway as apigw,
    core
)

class DNSStack(core.Stack):
    
    def __init__(self, scope: core.Construct, construct_id: str, api : apigw.LambdaRestApi, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        zone = route53.HostedZone.from_hosted_zone_attributes(self, "donkanator.com", hosted_zone_id = "Z13WGYL3NAVHE8", zone_name="donkanator.com",)
        
        route53.ARecord(self, 'AliasRecord',
            record_name = "rtcwproapi",
            target = route53.RecordTarget.from_alias(targets.ApiGateway(api)), 
            zone = zone)
        
#        my_alias = route53.IAliasRecordTarget.bind(
#            self,
#            record=route53.AliasRecordTargetConfig(
#                hosted_zone_id='Z1UJRXOUMOOFQ8',
#                dns_name="fuck.me.com"
#            )  
#        )
#        
#        route53.ARecord(
#            self,
#            'rtcwpro-api',
#            comment = "managed through CDK",
#            record_name = "rtcwproapi",
#            target = route53.RecordTarget(alias_target=my_alias),
#            zone = zone
#            )
        
#        route53.ARecord(self, "AliasRecord",
#                        zone=zone,
#                        target=route53.RecordTarget.from_alias(alias.ApiGateway(rest_api))
#                        )
#        
#        self.alias = my_alias