from aws_cdk import (
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    aws_certificatemanager as acm,
    core
)

class PipelineStack(core.Stack):
    
    @property
    def handler(self):
        return self.save_payload
    
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        save_payload_role = iam.Role(self, "SavePayloadRole",
                role_name = 'rtcwpro-lambda-intake-role',
                assumed_by= iam.ServicePrincipal("lambda.amazonaws.com")
                )
        save_payload_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        
        read_match_role = iam.Role(self, "IntakeRole",
                role_name = 'rtcwpro-lambda-read-match-role',
                assumed_by= iam.ServicePrincipal("lambda.amazonaws.com")
                )
        read_match_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        
        save_payload = _lambda.Function(
            self, 'save_payload',
            function_name  = 'rtcwpro-save-payload',
            handler='rtcwpro-save-payload.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/pipeline/save_payload'),
            role=save_payload_role
         )
        
        read_match = _lambda.Function(
            self, 'read_match',
            function_name  = 'rtcwpro-read-match',
            handler='rtcwpro-read-match.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('lambdas/pipeline/read_match'),
            role = read_match_role
         )
        
#        cert = acm.Certificate(self, "Certificate",   
#                        domain_name="hello.example.com", 
#                        validation=acm.CertificateValidation.from_dns(my_hosted_zone)
#                        )
        cert = acm.Certificate.from_certificate_arn(self, "Certificate", "arn:aws:acm:us-east-1:793070529856:certificate/b1e40e16-b716-4bf6-8191-2510afbe99e5")
        
        api = apigw.LambdaRestApi(
            self, 'rtcwpro',
            handler=save_payload,
            proxy = False,
            
            domain_name = {
                    "domain_name": "rtcwproapi.donkanator.com",
                    "certificate": cert
                    }
        )
        
        api_key = api.add_api_key(
                id="ApiKey",
                api_key_name="rtcwpro-api-key",
                value="rtcwproapikeythatisjustforbasicauthorization"
                )
        
        plan = api.add_usage_plan("UsagePlan",
                                  name="Easy",
                                  api_key=api_key,
                                  throttle={
                                          "rate_limit": 1, #1 request per second
                                          "burst_limit": 2
                                          }
                                  )
        plan.add_api_stage(
                stage=api.deployment_stage
                )
        
        matches = api.root.add_resource("matches")
        matches.add_method("GET")
        matches.add_method("POST",request_parameters={"method.request.header.matchid": True}, api_key_required=True)

        match = matches.add_resource("{id}")
        match.add_method("GET")# GET /matches/{id}
    
        self.save_payload = save_payload
        self.read_match = read_match
        self.api = api