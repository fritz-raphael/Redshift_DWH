import pandas as pd
import boto3
import json
import configparser
import time
import sys

def delete_cluster(cluster, CLUSTER_IDENTIFIER):
    """Delete Redshift-Cluster."""
    cluster.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER,
                            SkipFinalClusterSnapshot=True)

    print('Deleting...', end='')
    try:
        while cluster.describe_clusters(\
                    ClusterIdentifier=CLUSTER_IDENTIFIER)\
                    ['Clusters'][0]['ClusterStatus'] == 'deleting':
            time.sleep(5)
            print(".", end="")
    except Exception as e:
        print("\nCluster deleted.")


def delete_iam_role(iam, IAM_ROLE_NAME):
    """Delete IAM role."""
    print("Deleting role.")
    iam.detach_role_policy(RoleName=IAM_ROLE_NAME,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    print("Role deleted.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    # HOST = config.get('CLUSTER', 'HOST')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
    NUM_NODES = config.get('CLUSTER', 'NUM_NODES')
    NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
    CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    IAM_ROLE_NAME = config.get('IAM_ROLE', 'IAM_ROLE_NAME')
    ARN = config.get('IAM_ROLE', 'ARN')

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                      )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    myClusterProps = redshift.describe_clusters(
                        ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    print(f"Do you really want to delete cluster '{CLUSTER_IDENTIFIER}'?")
    decision = input("[y / n] > ")
    if decision.lower() == "y":
        delete_cluster(redshift, CLUSTER_IDENTIFIER)
    else:
        sys.exit(0)

    print(f"And do you really want to delete the role '{IAM_ROLE_NAME}'?")
    decision = input("[y / n] > ")
    if decision.lower() == "y":
        delete_iam_role(iam, IAM_ROLE_NAME)


if __name__ == '__main__':
    main()
