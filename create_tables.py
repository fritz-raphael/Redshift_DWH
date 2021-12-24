import boto3
import configparser
import psycopg2
import json
import time
import sys
from sql_queries import create_table_queries,\
                        drop_table_queries,\
                        scan_existing_tables
from botocore.exceptions import ClientError

def create_iam_role(iam, IAM_ROLE_NAME):
    """Create a new IAM role and attach policy."""
    print("\n1.1 Creating a new IAM Role")
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )
        print(f"New role '{IAM_ROLE_NAME}' created.\n")
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
    print("Policy attached.\n")

    print("1.3 Get the IAM role ARN\n")
    role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    return role_arn


def write_specs_to_config(endpoint, role_arn):
    """Write HOST and ARN to config-file."""
    with open('dwh.cfg', 'r') as config_file:
        lines = config_file.readlines()

    position_host = False
    position_arn = False

    # Identify current position of HOST and ARN:
    for index, line in enumerate(lines):
        if 'HOST' in line:
            # print(f"HOST is in line {index}. \n")
            position_host = index
        elif 'ARN' in line:
            # print(f"ARN is in line {index}. \n")
            position_arn = index
        else:
            continue

    # Update lines with new HOST
    if position_host:
        lines[position_host] = f"HOST={endpoint}\n"
    else:
        lines.append(f"HOST={endpoint}\n")

    # Update lines with new ARN
    if position_arn:
        lines[position_arn] = f"ARN={role_arn}\n"
    else:
        lines.append(f"ARN={role_arn}\n")

    # Write lines to config-file.
    try:
        with open('dwh.cfg', 'w') as config_file:
            config_file.writelines(lines)
            print("2.2 Endpoint and role_arn written to config-file.\n")
    except Exception as e:
        print(e)


def create_cluster(config):
    """Create Redshift cluster and establish connection."""
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
    NUM_NODES = config.get('CLUSTER', 'NUM_NODES')
    NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
    CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    IAM_ROLE_NAME = config.get('IAM_ROLE', 'IAM_ROLE_NAME')

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

    role_arn = create_iam_role(iam, IAM_ROLE_NAME)

    try:
        print("2.1 Creating cluster.")
        response = redshift.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            IamRoles=[role_arn]
        )
        print(f"Cluster '{CLUSTER_IDENTIFIER}' is being created.\n")
    except Exception as e:
        print("Cluster creation failed:\n")
        print(e)

    print("Please wait ", end="")
    while redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER) \
            ['Clusters'][0]['ClusterStatus'] != 'available':
        time.sleep(5)
        print(".", end="")

    print("\nCluster created and available.\n")
    myClusterProps = redshift.describe_clusters(
                        ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

    endpoint = myClusterProps['Endpoint']['Address']

    write_specs_to_config(endpoint, role_arn)


def drop_tables(cur, conn):
    """Drop all existing tables if there are any."""
    print("3.1 Checking for existing tables.")
    cur.execute(scan_existing_tables)
    results = cur.fetchall()
    if results:
        for query in drop_table_queries:
            table = query.split(" ")[4]
            print(f"Dropping table '{table}'.")
            cur.execute(query)
            conn.commit()
            print("Table dropped.")
        print()
    else:
        print("No tables found.\n")


def create_tables(cur, conn):
    """Create staging and star schema tables."""
    print("3.2 Creating tables for staging and star schema.")
    for query in create_table_queries:
        table = query.split(" ")[5]
        print(f"Creating table '{table}'.")
        cur.execute(query)
        conn.commit()
    print()


def main():
    """Create cluster, connect to Redshift, and create tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    if not KEY or not SECRET:
        print("You need to insert both your AWS access key"
              " and your AWS secret access key into the config-file.")
        sys.exit()

    create_cluster(config)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
