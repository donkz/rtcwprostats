#!/usr/bin/env python3

from aws_cdk import core

from stacks.pipeline import PipelineStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.processing import ProcessingStack

app = core.App()

env={'region': 'us-east-1'}
pipeline = PipelineStack(app, "PipeLine", env=env)
storage = StorageStack(app, "Storage", save_payload = pipeline.save_payload, read_match = pipeline.read_match, env=env)
DNSStack(app,"DNSStack", api = pipeline.api, env=env)
ProcessingStack(app, "ProcessingStack", env=env)

app.synth()

#print(storage.storage_bucket.url_for_object("objectname"))# Path-Style URL
