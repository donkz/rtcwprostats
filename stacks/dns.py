from aws_cdk import (
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_apigateway as apigw,
    core
)


class DNSStack(core.Stack):
    """Make a A Alias record in my zone to API Gateway domain."""

    def __init__(self, scope: core.Construct, construct_id: str, api: apigw.LambdaRestApi, dns_resource_name: str,hosted_zone_id: str, zone_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        zone = route53.HostedZone.from_hosted_zone_attributes(self, dns_resource_name, hosted_zone_id=hosted_zone_id, zone_name=zone_name)

        route53.ARecord(self, 'AliasRecord',
                        record_name="rtcwproapi",
                        target=route53.RecordTarget.from_alias(targets.ApiGateway(api)),
                        zone=zone)
