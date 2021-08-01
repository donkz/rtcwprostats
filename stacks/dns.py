from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_route53 as route53
import aws_cdk.aws_route53_targets as targets
import aws_cdk.aws_apigateway as apigw



class DNSStack(Stack):
    """Make a A Alias record in my zone to API Gateway domain."""

    def __init__(self, scope: Construct, construct_id: str, api: apigw.LambdaRestApi, dns_resource_name: str,hosted_zone_id: str, zone_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        zone = route53.HostedZone.from_hosted_zone_attributes(self, dns_resource_name, hosted_zone_id=hosted_zone_id, zone_name=zone_name)

        route53.ARecord(self, 'AliasRecord',
                        record_name="rtcwproapi",
                        target=route53.RecordTarget.from_alias(targets.ApiGateway(api)),
                        zone=zone)
