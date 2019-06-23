"""Functions to state maintain redshift clauster on AWS. 

This file can also be imported as a module and contains the following
functions:

    * create_iam_role - Creating IAM role on AWS and attching appropriate policy.   
    * create_redshift_cluster - Create redshift cluster and starting it. 
    * redshiftProps - Returns properties of Redshift.
    * delete_redshift_cluster - Deleting the redshift cluster from AWS.
"""

def create_iam_role():
    """Creating IAM role and adding rwad only access on S3 bucket. 
    
    Parameters
    ___________
        None    
    Returns
    ___________
        arn - Amazon resource name
    """
    
    import boto3
    import configparser
    import json
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        iam = boto3.client('iam',
                           aws_access_key_id=config.get('AWS','KEY'),
                           aws_secret_access_key=config.get('AWS','SECRET'),
                           region_name='us-west-2'
                          )
    except Exception as e:
        print(e)
  
    try:
        dwhRole = iam.create_role(
                                 Path='/',
                                 RoleName=config.get("IAM_ROLE","ROLE_NAME"),
                                 Description = "Allows Redshift clusters to call AWS services on your behalf.",
                                 AssumeRolePolicyDocument=json.dumps(
                                                                    {'Statement': [{'Action': 'sts:AssumeRole',
                                                                     'Effect': 'Allow',
                                                                     'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                     'Version': '2012-10-17'})
                                 )    
    except Exception as e:
        print(e)
    
    try:    
        iam.attach_role_policy(RoleName=config.get("IAM_ROLE","ROLE_NAME"),
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)
    
    try:   
        roleArn = iam.get_role(RoleName=config.get("IAM_ROLE","ROLE_NAME"))['Role']['Arn']
    except Exception as e:
        print(e)
    
    return roleArn

def create_redshift_cluster(arn):
    """Creating redshift cluster and starting it. 
    
    Parameters
    ___________
        None    
    Returns
    ___________
        None
    """
    
    import boto3
    import configparser

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        redshift = boto3.client('redshift',
                                region_name="us-west-2",
                                aws_access_key_id=config.get('AWS','KEY'),
                                aws_secret_access_key=config.get('AWS','SECRET')
                               )
    except Exception as e:
        print(e)
    
    try:
        response = redshift.create_cluster(
                                           ClusterType=config.get('HARDWARE','CLUSTER_TYPE'),
                                           NodeType=config.get('HARDWARE','NODE_TYPE'),
                                           NumberOfNodes=int(config.get('HARDWARE','NUM_NODES')),
                                           DBName=config.get('DB','DB_NAME'),
                                           ClusterIdentifier=config.get('CLUSTER','CLUSTER_IDENTIFIER'),
                                           MasterUsername=config.get('DB','DB_NAME'),
                                           MasterUserPassword=config.get('DB','DB_PASSWORD'),
                                           IamRoles=[arn]
                                          )
    except Exception as e:
        print(e)
        
    
def redshiftProps():
    """Accessing properties of redshift cluster. 
    
    Parameters
    ___________
        None    
    Returns
    ___________
        Pandas Data frame - Properties of redshift cluster. 
    """
    
    import pandas as pd
    import boto3
    import configparser

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        redshift = boto3.client('redshift',
                                region_name="us-west-2",
                                aws_access_key_id=config.get('AWS','KEY'),
                                aws_secret_access_key=config.get('AWS','SECRET')
                               )
    except Exception as e:
        print(e)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER','CLUSTER_IDENTIFIER'))['Clusters'][0]
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in myClusterProps.items() if k in keysToShow]
    host = myClusterProps['Endpoint']['Address']
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def delete_redshift_cluster():
    """Deleting already running redshift cluster. 
    
    Parameters
    ___________
        None    
    Returns
    ___________
        None 
    """
    
    import boto3
    import configparser
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        redshift = boto3.client('redshift',
                                region_name="us-west-2",
                                aws_access_key_id=config.get('AWS','KEY'),
                                aws_secret_access_key=config.get('AWS','SECRET')
                               )
    except Exception as e:
        print(e)
        
    redshift.delete_cluster(ClusterIdentifier=config.get('CLUSTER','CLUSTER_IDENTIFIER'),  SkipFinalClusterSnapshot=True)
