#!/usr/bin/env python3

import aws_cdk as cdk
import aws_cdk.aws_lambda as _lambda

from stacks.api import APIStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.processing import ProcessingStack
from stacks.database import DatabaseStack
from stacks.delivery_retriever import DeliveryRetrieverStack
from stacks.delivery_writer import DeliveryWriterStack
from stacks.delivery import DeliveryStack
from stacks.postprocess import PostProcessStack
from stacks.read_match_lambda import ReadMatchStack
from stacks.taskfunnel import TaskFunnelStack

from stacks.settings import (
    cert_arn,
    api_key,
    env,
    enable_tracing,
    dns_resource_name,
    hosted_zone_id,
    zone_name
)

app = cdk.App()

lambda_tracing = _lambda.Tracing.DISABLED
if enable_tracing:
    lambda_tracing = _lambda.Tracing.ACTIVE

database = DatabaseStack(app, "rtcwprostats-database", env=env)

storage = StorageStack(app, "rtcwprostats-storage", env=env, lambda_tracing=lambda_tracing)

task_funnel_stack = TaskFunnelStack(app, "rtcwprostats-taskfunnel", lambda_tracing=lambda_tracing, ddb_table=database.ddb_table, env=env)
post_process_stack = PostProcessStack(app, "rtcwprostats-postprocess", lambda_tracing=lambda_tracing, ddb_table=database.ddb_table, env=env)

reader = ReadMatchStack(app, "rtcwprostats-reader", storage_bucket=storage.storage_bucket, ddb_table=database.ddb_table, read_queue=storage.read_queue, read_dlq=storage.read_dlq, postproc_state_machine=post_process_stack.postproc_state_machine, lambda_tracing=lambda_tracing, env=env)
retriever = DeliveryRetrieverStack(app, "rtcwprostats-retriever", ddb_table=database.ddb_table, env=env, lambda_tracing=lambda_tracing)
delivery_writer = DeliveryWriterStack(app, "rtcwprostats-delivery-writer", ddb_table=database.ddb_table, funnel_sf=task_funnel_stack.funnel_state_machine, env=env, lambda_tracing=lambda_tracing)

apistack = APIStack(app, "rtcwprostats-API", cert_arn=cert_arn, api_key=api_key, storage_bucket=storage.storage_bucket, env=env, lambda_tracing=lambda_tracing)
DNSStack(app, "rtcwprostats-DNS", api=apistack.api, env=env, dns_resource_name=dns_resource_name, hosted_zone_id=hosted_zone_id, zone_name=zone_name)

ProcessingStack(app, "rtcwprostats-processing", env=env, lambda_tracing=lambda_tracing)
DeliveryStack(app, "rtcwprostats-delivery", api=apistack.api, retriever=retriever.retriever_lambda, delivery_writer=delivery_writer.delivery_writer_lambda, env=env)


cdk.Tags.of(app).add("purpose", "rtcwpro")

app.synth()
