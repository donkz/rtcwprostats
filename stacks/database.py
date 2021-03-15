from aws_cdk import (
    core
)

from aws_cdk.aws_dynamodb import (
    Table,
    Attribute,
    AttributeType,
    BillingMode,
    TableEncryption
)


class DatabaseStack(core.Stack):
    """Make a database for storing transactional data."""

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ddb_table = Table(
            self, 'DDBTable',
            partition_key=Attribute(name="pk", type=AttributeType.STRING),
            sort_key=Attribute(name="sk", type=AttributeType.STRING),
            billing_mode=BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            encryption=TableEncryption.AWS_MANAGED,
            removal_policy=core.RemovalPolicy.RETAIN
        )

        ddb_table.add_global_secondary_index(
            partition_key=Attribute(name="gsi1pk", type=AttributeType.STRING),
            sort_key=Attribute(name="gsi1sk", type=AttributeType.STRING),
            index_name="gsi1",
            # non_key_attributes=[],
            # projection_type = ProjectionType.ALL
        )

#        ddb_table.add_global_secondary_index(
#            partition_key=Attribute(name="gsi2pk", type=AttributeType.STRING),
#            sort_key=Attribute(name="gsi2sk", type=AttributeType.STRING),
#            index_name = "gsi2",
#            #non_key_attributes=[],
#            #projection_type = ProjectionType.ALL
#        )

        ddb_table.add_local_secondary_index(
            sort_key=Attribute(name="lsipk", type=AttributeType.STRING),
            index_name="lsi",
            # non_key_attributes=[],
            # projection_type = ProjectionType.ALL
        )

        core.Tags.of(ddb_table).add("purpose", "rtcwpro")

        self.ddb_table = ddb_table
