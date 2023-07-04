import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

def main():
    """
    Destroys AWS IAM and Cluster resources used in this project.
    
    Raises:
        Exceptions in cases the resources do not exist anymore or have dependencies to be deleted. In these cases, the rest of the code will continue to run and reading the logs will be necessary for troubleshooting. 
    """
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    redshift = boto3.client('redshift',
                            region_name="us-east-1",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                       region_name='us-east-1',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

    # DELETE CLUSTER
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(f"Error deleting cluster and IAM role: {str(e)}")

    # DETACH POLICIES FROM ROLE
    policies_to_detach = ["AmazonS3ReadOnlyAccess", "AmazonRedshiftFullAccess"]

    for policy in policies_to_detach:
        try:
            iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=f"arn:aws:iam::aws:policy/{policy}")
        except Exception as e:
            print(f"Error detaching policy {policy} from role {DWH_IAM_ROLE_NAME}: {str(e)}")

    # DELETE ROLE
    try:
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(f"Error deleting IAM role {DWH_IAM_ROLE_NAME}: {str(e)}")

    print("Cluster and IAM role have been deleted")


if __name__ == "__main__":
    main()