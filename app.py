#!/usr/bin/env python3

from aws_cdk import core

from stacks.intake import IntakeStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.processing import ProcessingStack
from stacks.database import DatabaseStack

app = core.App()

env={'region': 'us-east-1'}
intake = IntakeStack(app, "PipeLine", env=env)
DNSStack(app,"DNSStack", api = intake.api, env=env)
storage = StorageStack(app, "Storage", save_payload = intake.save_payload, env=env)
database = DatabaseStack(app, "Database", read_match = storage.read_match)
ProcessingStack(app, "ProcessingStack", env=env)


app.synth()

#print(storage.storage_bucket.url_for_object("objectname"))# Path-Style URL
