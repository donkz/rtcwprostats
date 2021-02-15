#!/usr/bin/env python3

from aws_cdk import core

from stacks.api import APIStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.processing import ProcessingStack
from stacks.database import DatabaseStack
from stacks.delivery import DeliveryStack
from stacks.settings import cert_arn

from aws_cdk import aws_lambda as _lambda

app = core.App()

env={'region': 'us-east-1'}
enable_tracing = True

lambda_tracing = _lambda.Tracing.DISABLED
if enable_tracing:
    lambda_tracing = _lambda.Tracing.ACTIVE


apistack = APIStack(app, "rtcwprostats-API", cert_arn = cert_arn, env=env, lambda_tracing = lambda_tracing)
DNSStack(app,"rtcwprostats-DNS", api = apistack.api, env=env)
storage = StorageStack(app, "rtcwprostats-storage", save_payload = apistack.save_payload, env=env, lambda_tracing=lambda_tracing)
database = DatabaseStack(app, "rtcwprostats-database", read_match = storage.read_match)
ProcessingStack(app, "rtcwprostats-processing", env=env, lambda_tracing = lambda_tracing)
DeliveryStack(app, "rtcwprostats-delivery", ddb_table = database.ddb_table, api = apistack.api, lambda_tracing= lambda_tracing)


app.synth()

#print(storage.storage_bucket.url_for_object("objectname"))# Path-Style URL
