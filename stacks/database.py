from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_s3_notifications as s3n,
    core
)

from aws_cdk.aws_dynamodb import (
    Table,
    Attribute,
    AttributeType,
    StreamViewType,
    BillingMode,
    TableEncryption
)


class DatabaseStack(core.Stack):
    
    def __init__(self, scope: core.Construct, id: str, read_match: _lambda.IFunction, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        table_name = "rtcwprostats"

        ddb_table = Table(
            self, 'DDBTable',
            table_name=table_name,
            partition_key=Attribute(name="pk",type=AttributeType.STRING),
            sort_key=Attribute(name="sk", type=AttributeType.STRING),
            billing_mode=BillingMode.PAY_PER_REQUEST,
            #stream=StreamViewType.NEW_IMAGE,
            encryption=TableEncryption.AWS_MANAGED,
            removal_policy=core.RemovalPolicy.DESTROY # NOT recommended for production code
        )

        ddb_table.add_global_secondary_index(
            partition_key=Attribute(name="gsi1pk", type=AttributeType.STRING),
            sort_key=Attribute(name="gsi1sk", type=AttributeType.STRING),
            index_name = "gsi1",
            #non_key_attributes=[],
            #projection_type = ProjectionType.ALL
        )

        ddb_table.add_global_secondary_index(
            partition_key=Attribute(name="gsi2pk", type=AttributeType.STRING),
            sort_key=Attribute(name="gsi2sk", type=AttributeType.STRING),
            index_name = "gsi2",
            #non_key_attributes=[],
            #projection_type = ProjectionType.ALL
        )

        ddb_table.add_local_secondary_index(
            sort_key=Attribute(name="lsipk", type=AttributeType.STRING),
            index_name = "lsi",
            # non_key_attributes=[],
            # projection_type = ProjectionType.ALL
        )

        ddb_table.grant_read_write_data(read_match)
        core.Tags.of(ddb_table).add("purpose", "rtcwpro")

        self.ddb_table = ddb_table
