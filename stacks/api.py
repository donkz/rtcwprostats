from aws_cdk import (
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    aws_certificatemanager as acm,
    aws_s3 as s3,
    core
)


class APIStack(core.Stack):
    """Begin API and start with a submit method."""

    def __init__(self, scope: core.Construct, construct_id: str, lambda_tracing, api_key: str, cert_arn: str, storage_bucket: s3.Bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ####################################
        # Lambda role and function
        ####################################
        save_payload_role = iam.Role(self, "SavePayloadRole",
                                     role_name='rtcwpro-lambda-intake-role',
                                     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                     )
        save_payload_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        save_payload = _lambda.Function(
            self, 'save_payload',
            function_name='rtcwpro-save-payload',
            handler='save_payload.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/pipeline/save_payload'),
            role=save_payload_role,
            tracing=lambda_tracing,
            timeout=core.Duration.seconds(10)
        )

        storage_bucket.grant_put(save_payload, ["intake/*"])
        storage_bucket.grant_put(save_payload, ["intake_dlq/*"])

        save_payload_integration = apigw.LambdaIntegration(save_payload)

        ####################################
        # End of Lambda role and function
        ####################################

        cert = acm.Certificate.from_certificate_arn(self, "Certificate", cert_arn)

        api = apigw.RestApi(self, "rtcwpro",
                            domain_name={
                                "domain_name": "rtcwproapi.donkanator.com",
                                "certificate": cert
                                },
                            default_cors_preflight_options={
                                "allow_origins": apigw.Cors.ALL_ORIGINS,
                                "allow_methods": apigw.Cors.ALL_METHODS
                                }
                            )

        submit = api.root.add_resource("submit")
        submit.add_method("POST", save_payload_integration, request_parameters={"method.request.header.matchid": True}, api_key_required=True)

        api_key_obj = api.add_api_key(
            id="ApiKey",
            api_key_name="rtcwpro-api-key",
            value=api_key
        )

        plan = api.add_usage_plan("UsagePlan",
                                  name="Easy",
                                  api_key=api_key_obj,
                                  throttle={
                                      "rate_limit": 1,  # 1 request per second
                                      "burst_limit": 2
                                  }
                                  )
        plan.add_api_stage(
            stage=api.deployment_stage,
            cache_cluster_size=0.5,
            caching_enabled = True,
            cache_ttl = core.Duration.minutes(20),
            cache_data_encrypted=False
        )

        self.save_payload = save_payload
        self.api = api
