California Housing Data Processing Pipeline
===========================================

A scalable, secure, and cost-effective AWS pipeline for processing California Housing Prices data. This solution automatically ingests CSV data, processes it with Python, and stores summary statistics in an RDS PostgreSQL database.

Table of Contents
-----------------

-   [Architecture Overview](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#architecture-overview)
-   [Features](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#features)
-   [Prerequisites](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#prerequisites)
-   [Deployment Instructions](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#deployment-instructions)
-   [Testing the Solution](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#testing-the-solution)
-   [Architecture Decisions and Trade-offs](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#architecture-decisions-and-trade-offs)
-   [Security Considerations](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#security-considerations)
-   [Operational Considerations](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#operational-considerations)
-   [Future Enhancements](https://claude.ai/chat/507ba028-ea5a-429e-8f2c-459922d549ec#future-enhancements)

Architecture Overview
---------------------

![Architecture Diagram](https://placeholder-for-architecture-diagram.png/)

The pipeline consists of these core components:

1.  **S3 Bucket**: Serves as the entry point for uploading California Housing data CSV files
2.  **Lambda Function**: Triggered by S3 uploads, processes the data using pandas
3.  **RDS PostgreSQL**: Stores the processed summary statistics
4.  **VPC & Security Groups**: Provides network isolation and security
5.  **Secrets Manager**: Securely manages database credentials
6.  **KMS**: Handles encryption for sensitive data
7.  **IAM Roles**: Implements the principle of least privilege

Features
--------

-   **Event-driven Architecture**: Automatic processing when new files are uploaded
-   **Infrastructure as Code**: Complete AWS CDK deployment
-   **Environment Support**: Different configurations for dev and prod environments
-   **Data Processing**: Calculates average median house values by ocean proximity category
-   **Security**: Encryption in transit and at rest, network isolation, least privilege access
-   **Monitoring**: CloudWatch logging and Lambda monitoring
-   **Unit Tests**: Test suite for data processing logic

Prerequisites
-------------

-   AWS CLI configured with appropriate permissions
-   AWS CDK v2 installed (`npm install -g aws-cdk`)
-   Python 3.11+
-   Docker (for local testing and CDK asset bundling)
-   PostgreSQL client (for testing database connectivity)

Deployment Instructions
-----------------------

### 1\. Clone the Repository

```
git clone <repository-url>
cd california-housing-pipeline

```

### 2\. Set Up Python Environment

```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
uv pip install -r requirements-dev.txt

```

> Note: We use `uv` instead of traditional `pip` for faster dependency resolution and installation.

### 3\. Bootstrap CDK (First-time only)

```
cdk bootstrap aws://<account-id>/<region>

```

### 4\. Configure the Deployment

Modify context values in `cdk.json` if needed, or provide them as command-line parameters.

### 5\. Deploy the Stack

For development environment:

```
cdk deploy --context env=dev

```

For production environment:

```
cdk deploy --context env=prod --context account=<account-id> --context region=<region>

```

### 6\. Note the Outputs

After deployment, CDK will display important outputs:

-   S3 Bucket name for uploading data
-   Database endpoint
-   Secrets Manager ARN for database credentials
-   Lambda function name

Testing the Solution
--------------------

### 1\. Upload a Sample CSV File

```
# Download the California Housing dataset if needed
# curl -O https://raw.githubusercontent.com/ageron/handson-ml/master/datasets/housing/housing.csv

# Upload to your S3 bucket
aws s3 cp housing.csv s3://california-housing-data-dev-<account-id>/

```

### 2\. Monitor Lambda Execution

```
# Check Lambda logs
aws logs get-log-events --log-group-name /aws/lambda/california-housing-processor-dev --log-stream-name <latest-stream>

```

### 3\. Verify Database Results

```
# Retrieve database credentials from Secrets Manager
aws secretsmanager get-secret-value --secret-id california-housing-db-credentials-dev

# Connect to the database (using retrieved credentials)
psql -h <db-endpoint> -U housing_admin -d housing_data

# Query the results
SELECT * FROM housing_summary_statistics;

```

### 4\. Run Unit Tests

```
pytest tests/

```

Architecture Decisions and Trade-offs
-------------------------------------

### Data Processing Strategy

**Decision**: Use Lambda for data processing triggered by S3 events

-   **Pros**: Serverless, scales automatically, pay-per-use, simple integration with S3
-   **Cons**: Limited execution time (15 min max), memory constraints
-   **Trade-off**: For very large datasets, consider Step Functions or EMR instead

### Database Selection

**Decision**: PostgreSQL on RDS

-   **Pros**: Familiar SQL interface, reliable, supports complex queries
-   **Cons**: Higher cost compared to DynamoDB, requires more management
-   **Trade-off**: Relational data model better suited for analytics compared to NoSQL options

### Infrastructure as Code Tool

**Decision**: AWS CDK with Python

-   **Pros**: Type safety, familiar language (Python), higher-level abstractions than CloudFormation
-   **Cons**: Adds dependency on CDK, learning curve for new team members
-   **Trade-off**: Productivity gains outweigh the added complexity

### Network Architecture

**Decision**: VPC with private subnets for RDS and Lambda

-   **Pros**: Enhanced security, network isolation
-   **Cons**: More complex setup, additional NAT Gateway costs in production
-   **Trade-off**: Security benefits outweigh the added complexity and cost

### Storage Tiering

**Decision**: Intelligent tiering after 90 days

-   **Pros**: Cost optimization for infrequently accessed data
-   **Cons**: Slightly slower access for archived data
-   **Trade-off**: Cost savings with minimal performance impact

Security Considerations
-----------------------

### Data Protection

-   **Encryption at Rest**:

    -   S3 bucket uses KMS encryption
    -   RDS database has storage encryption enabled
    -   Secrets Manager uses KMS for credential encryption
-   **Encryption in Transit**:

    -   TLS for all communications between services
    -   Secure HTTPS API endpoints

### Access Control

-   **Principle of Least Privilege**:

    -   Custom IAM roles with minimal permissions for Lambda
    -   S3 bucket blocks all public access
    -   Security groups restrict network access
-   **Secrets Management**:

    -   Database credentials stored in AWS Secrets Manager
    -   No hardcoded credentials in code or configuration

### Network Security

-   **Network Isolation**:
    -   RDS deployed in isolated private subnets
    -   Lambda functions in private subnets with controlled egress
    -   Security groups restricting traffic flow between resources

### Compliance and Governance

-   **Logging and Monitoring**:
    -   Lambda logging configured with CloudWatch
    -   Database audit logging available
    -   Resource tagging for governance

Operational Considerations
--------------------------

### Monitoring and Logging

-   CloudWatch logs for Lambda functions
-   CloudWatch metrics for pipeline performance
-   Database performance monitoring via RDS metrics

### Disaster Recovery

-   Automated RDS backups (7 days for prod, 1 day for dev)
-   S3 versioning enabled for source data resilience
-   Multi-AZ deployment for production database

### Cost Optimization

-   Lambda concurrency limits to control costs
-   RDS instance sizing appropriate for environment (t3.micro for dev, t3.small for prod)
-   S3 lifecycle policies for intelligent tiering of older data

Future Enhancements
-------------------

1.  **Data Quality Checks**: Add validation rules and data quality monitoring
2.  **CI/CD Pipeline**: Implement automated testing and deployment
3.  **Dashboard**: Create CloudWatch dashboard for pipeline monitoring
4.  **Data API**: Add API Gateway to expose the processed data
5.  **Advanced Analytics**: Integrate with SageMaker for ML model training