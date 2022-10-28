import boto3

class S3Helper(object):
    def __init__(self):
        self.client = boto3.client("s3")

    def get_objects(self, bucket):
        response = self.client.list_objects_v2(
            Bucket=bucket
        )
        # print(response["Contents"])
        data = {}
        for obj in response["Contents"]:
            data[obj["Key"]] = obj
        return data

    def get_object(self, bucket, key):
        data = self.client.get_object(
            Bucket=bucket,
            Key=key
        )
        return data["Body"].read()


def main():
    s3 = S3Helper()
    # s3.get_objects("hackmeetingfs")
    s3.get_object("hackmeetingfs", "bar")


if __name__ == "__main__":
    main()
