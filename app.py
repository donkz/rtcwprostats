#!/usr/bin/env python3

from aws_cdk import core
from aws_cdk import aws_lambda as _lambda

from stacks.api import APIStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.processing import ProcessingStack
from stacks.database import DatabaseStack
from stacks.delivery_retriever import DeliveryRetrieverStack
from stacks.delivery import DeliveryStack

from stacks.settings import (
    cert_arn,
    api_key,
    env,
    enable_tracing,
    dns_resource_name,
    hosted_zone_id,
    zone_name
)

app = core.App()

lambda_tracing = _lambda.Tracing.DISABLED
if enable_tracing:
    lambda_tracing = _lambda.Tracing.ACTIVE


database = DatabaseStack(app, "rtcwprostats-database", env=env)
storage = StorageStack(app, "rtcwprostats-storage", ddb_table=database.ddb_table, env=env, lambda_tracing=lambda_tracing)

retriever = DeliveryRetrieverStack(app, "rtcwprostats-retriever", ddb_table=database.ddb_table, env=env, lambda_tracing=lambda_tracing)

apistack = APIStack(app, "rtcwprostats-API", cert_arn=cert_arn, api_key=api_key, storage_bucket=storage.storage_bucket, env=env, lambda_tracing=lambda_tracing)
DNSStack(app, "rtcwprostats-DNS", api=apistack.api, env=env, dns_resource_name=dns_resource_name,hosted_zone_id=hosted_zone_id, zone_name=zone_name)

ProcessingStack(app, "rtcwprostats-processing", env=env, lambda_tracing=lambda_tracing)
DeliveryStack(app, "rtcwprostats-delivery", api=apistack.api, retriever=retriever.retriever_lambda, env=env)

core.Tags.of(app).add("purpose", "rtcwpro")

app.synth()
