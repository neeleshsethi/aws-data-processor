import aws_cdk as core
from stacks.pipeline_stack import CaliforniaHousingPipelineStack

app = core.App()

env_name = app.node.try_get_context("env") or "dev"

pipeline_stack = CaliforniaHousingPipelineStack(
    app,
    f"CaliforniaHousingPipeline-{env_name}",
     env=core.Environment(
        account=app.node.try_get_context("account") or core.Aws.ACCOUNT_ID,
        region=app.node.try_get_context("region") or core.Aws.REGION
    ),
    env_name=env_name,
    description="California Housing Data processing pipeline"
)

core.Tags.of(app).add("Project", "CaliforniaHousingPipeline")
core.Tags.of(app).add("Environment", env_name)
core.Tags.of(app).add("ManagedBy", "CDK")

app.synth()