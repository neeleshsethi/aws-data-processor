"""
CDK Stack for California Housing Data Processing Pipeline
"""
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    aws_secretsmanager as secretsmanager,
    aws_logs as logs,
    aws_kms as kms,
)
import aws_cdk as core
from aws_cdk.aws_kms import IKey
from aws_cdk.aws_secretsmanager import ISecret
from aws_cdk.aws_iam import IPrincipal

from constructs import Construct

class CaliforniaHousingPipelineStack(Stack):
    """
    CDK Stack for deploying a data processing pipeline for California Housing data.
    The pipeline consists of:
    - S3 bucket for input data
    - Lambda function for data processing
    - RDS PostgreSQL database for storing results
    - VPC for network security
    - IAM roles and permissions
    - Secrets management for database credentials
    """
    def __init__(self, scope: Construct, construct_id: str, env_name: str = "dev", **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        self.env_name = env_name

        key = kms.Key(
            self,
            "EncryptionKey",
            alias=f"alias/california-housing-{self.env_name}",
            description=f"KMS key for california housing pipeline {self.env_name}",
            enable_key_rotation=True,

        )
        encryption_key = kms.Key.from_key_arn(self, "ImportedKey", key.key_arn)

        self.vpc = self._create_vpc()
        
        self.s3_endpoint, self.secrets_endpoint, self.logs_endpoint = self._create_vpc_endpoints()

        self.data_bucket = self._create_data_bucket(encryption_key)

        self.db_security_group , self.lambda_security_group = self._create_security_group()

        self.db_credentials = self._create_db_credentials(encryption_key)

        # Create RDS PostgreSQL instance

        self.database = self._create_database()

        # Create Lambda function and role
        self.lambda_role = self._create_lambda_role()
        self.lambda_layer = self._create_lambda_layer()

        self.processing_lambda = self._create_lambda_function()

        self._configure_s3_trigger()

        self._create_outputs()

    def _create_vpc(self) -> ec2.Vpc:

        return ec2.Vpc(
            self,
            "HousingDataVPC",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="Public", 
                                        subnet_type=ec2.SubnetType.PUBLIC, 
                                        cidr_mask=24),
                ec2.SubnetConfiguration(name="Private", 
                                        subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS if self.env_name == "prod" else ec2.SubnetType.PRIVATE_ISOLATED, 
                                        cidr_mask=24),
                ec2.SubnetConfiguration(name="Isolated", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED, cidr_mask=24),


            ]

        )
   
    def _create_vpc_endpoints(self):
        # Create VPC endpoint for S3
        s3_endpoint = ec2.GatewayVpcEndpoint(
            self,
            "S3Endpoint",
            vpc=self.vpc,
            service=ec2.GatewayVpcEndpointAwsService.S3
        )
        
        # Create VPC endpoint for Secrets Manager (needed to access DB credentials)
        secrets_manager_endpoint = ec2.InterfaceVpcEndpoint(
            self,
            "SecretsManagerEndpoint",
            vpc=self.vpc,
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            private_dns_enabled=True,
            subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            )
        )
        
        # If using CloudWatch Logs, you'll also need this endpoint
        logs_endpoint = ec2.InterfaceVpcEndpoint(
            self,
            "CloudWatchLogsEndpoint",
            vpc=self.vpc,
            service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            private_dns_enabled=True,
            subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            )
        )
        
        return s3_endpoint, secrets_manager_endpoint, logs_endpoint

    def _create_data_bucket(self, encryption_key: IKey):
        return s3.Bucket(self,
                        "CaliforniaHousingDataBucket",
                        bucket_name=f"california-housing-data-{self.env_name}-{core.Aws.ACCOUNT_ID}",
                        removal_policy=core.RemovalPolicy.RETAIN if self.env_name == "prod" else core.RemovalPolicy.DESTROY,
                        encryption=s3.BucketEncryption.KMS,
                        encryption_key=encryption_key,
                        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                        versioned=True,
                        lifecycle_rules=[
                        s3.LifecycleRule(
                            id="ArchiveAfter90Days",
                            transitions=[
                            s3.Transition(
                                storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                                transition_after=core.Duration.days(90)
                            )
                        ]
                    )
                ]
            )

    def _create_security_group(self):
        db_security_group = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=self.vpc,
            description="Security group for RDS PostgreSQL",
            allow_all_outbound=False
        )

        lambda_security_group = ec2.SecurityGroup(
            self,
            "LambdaSecurityGroup",
            vpc=self.vpc,
            description="Security group for Lambda function",
            allow_all_outbound=True
        )

        db_security_group.add_ingress_rule(
            lambda_security_group,
            ec2.Port.tcp(5432),
            "Allow lambda to access postgreSQl"
        )

        return db_security_group, lambda_security_group
    

    def _create_db_credentials(self, encryption_key: IKey) -> ISecret:
        secret = secretsmanager.Secret(
            self,
            "DBCredentials",
            secret_name=f"california-housing-db-credentials-{self.env_name}",
            description="Credentials for california rds database",
            encryption_key=encryption_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "db_admin_user"}',
                generate_string_key="password",
                exclude_characters="\"@/\\"
            )
        )
        return secretsmanager.Secret.from_secret_name_v2(self, "ImportedSecret", secret.secret_name)

    def _create_database(self) -> rds.DatabaseInstance:
        return rds.DatabaseInstance(
            self,
            "HousingDataDatabase",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_14),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3,
                ec2.InstanceSize.SMALL if self.env_name == "prod" else ec2.InstanceSize.MICRO
            ),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[self.db_security_group],
            credentials=rds.Credentials.from_secret(self.db_credentials),
            database_name="housing_data",
            allocated_storage=20,
            storage_encrypted=True,
            backup_retention=core.Duration.days(7) if self.env_name == "prod" else core.Duration.days(1),
            deletion_protection=self.env_name == "prod",
            removal_policy=core.RemovalPolicy.SNAPSHOT if self.env_name == "prod" else core.RemovalPolicy.DESTROY,
            parameter_group=rds.ParameterGroup.from_parameter_group_name(
                self, "ParameterGroup", "default.postgres14"
            ),
            multi_az=self.env_name == "prod"




        )

    def _create_lambda_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com").grant_principal,
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole")
            ]
        )
        self.data_bucket.grant_read(role)
        self.db_credentials.grant_read(role)
        return role
    

    def _create_lambda_layer(self) -> lambda_.LayerVersion:
        return lambda_.LayerVersion(
            self,
            f"CalHousingDependenciesLayer{self.env_name.capitalize()}",  # Using a unique name with environment
            layer_version_name=f"california-housing-dependencies-{self.env_name}",
            code=lambda_.Code.from_asset(
            ".",
            bundling=core.BundlingOptions(
                image=lambda_.Runtime.PYTHON_3_11.bundling_image,
                command=[
                    "bash", "-c",
                    "pip install --no-cache-dir -r lambda-requirements.txt --only-binary=:all: " +
                    "--platform manylinux2014_x86_64 --target /asset-output/python && " +
                    "find /asset-output -type d -name '__pycache__' -exec rm -rf {} +; " +
                    "find /asset-output -type d -name '*.dist-info' -exec rm -rf {} +; " +
                    "find /asset-output -type d -name '*.egg-info' -exec rm -rf {} +; " +
                    "find /asset-output -name '*.pyc' -delete"
                ]
            )
        ),
        compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
        description="Dependencies for California Housing data processing",

        )


    def _create_lambda_function(self) -> lambda_.Function:
        dependencies_layer = self.lambda_layer

        return lambda_.Function(
            self,
            "CaliforniaHousingProcessorLambda",
            function_name=f"california-housing-processor-{self.env_name}",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("src",  exclude=["__pycache__", "*.pyc"]),
            handler="lambda_functions.handler.handler",
            timeout=core.Duration.minutes(5),
            memory_size=1024,
            layers=[dependencies_layer],
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS if self.env_name == "prod" else ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[self.lambda_security_group],
            environment={
                "DB_SECRET_NAME": self.db_credentials.secret_name,
                "LOG_LEVEL": "INFO",
                "ENV": self.env_name
            },
            role=self.lambda_role,
            log_retention=logs.RetentionDays.ONE_MONTH,
           # reserved_concurrent_executions=5 if self.env_name == "prod" else 2,
            tracing=lambda_.Tracing.ACTIVE
        )

    def _configure_s3_trigger(self) -> None:
        """Set up S3 event notification to trigger Lambda."""
        self.data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.processing_lambda),
            s3.NotificationKeyFilter(suffix=".csv")
        )

    def _create_outputs(self) -> None:
        """Create stack outputs."""
        core.CfnOutput(
            self, "DataBucketName",
            value=self.data_bucket.bucket_name,
            description="S3 Bucket for uploading California Housing data"
        )
        
        core.CfnOutput(
            self, "DatabaseEndpoint",
            value=self.database.db_instance_endpoint_address,
            description="RDS PostgreSQL instance endpoint"
        )
        
        core.CfnOutput(
            self, "DatabaseSecretArn",
            value=self.db_credentials.secret_arn,
            description="ARN of the database credentials secret"
        )
        
        core.CfnOutput(
            self, "LambdaFunctionName",
            value=self.processing_lambda.function_name,
            description="Name of the Lambda function processing the data"
        )






        



