import boto3
from logging import getLogger, FileHandler, Formatter
from argparse import ArgumentParser
from botocore import exceptions
from botocore import config

logger = getLogger(__name__)
handle = FileHandler("/tmp/restore.log")
logger.setLevel("INFO")
formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
handle.setFormatter(formatter)
logger.addHandler(handle)

client_config = config.Config(
    signature_version="s3",
    max_pool_connections=5,
    retries=dict(max_attempts=0),
    read_timeout=700)

global client

sucess_list = []
fail_list = []


def get_object_versions(bucket, prefix, max_key, key_marker):
    kwargs = dict(
        Bucket=bucket,
        EncodingType='url',
        MaxKeys=max_key,
        Prefix=prefix
    )

    if key_marker:
        kwargs['KeyMarker'] = key_marker

    response = client.list_object_versions(**kwargs)

    return response


def get_delete_markers_info(bucket, prefix, key_marker):
    markers = []
    max_markers = 500
    version_batch_size = 500

    while True:
        response = get_object_versions(bucket, prefix, version_batch_size,
                                       key_marker)
        key_marker = response.get('NextKeyMarker')
        delete_markers = response.get('DeleteMarkers', [])

        markers = markers + [dict(
            Key=x.get('Key'),
            VersionId=x.get('VersionId')) for x in delete_markers if
            x.get('IsLatest')]

        logger.info('{0} -- {1} delete '
                    'markers ...'.format(key_marker, len(markers)))

        if len(markers) >= max_markers or key_marker is None:
            break

    return {"delete_markers": markers, "key_marker": key_marker}


def delete_delete_markers(bucket, prefix):
    key_marker = None

    while True:
        info = get_delete_markers_info(bucket, prefix, key_marker)
        key_marker = info.get('key_marker')
        delete_markers = info.get('delete_markers', [])

        if len(delete_markers) > 0:
            response = client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': delete_markers,
                    'Quiet': True
                }
            )

            logger.info('Deleting {0} delete '
                        'markers ... '.format(len(delete_markers)))
            logger.info('Done with status {0}'.format(
                response.get('ResponseMetadata', {}).get('HTTPStatusCode')))
        else:
            logger.info('No more delete markers found\n')
            break


def get_delete_markers_info_key(bucket, key, key_marker):
    markers = []
    max_markers = 500
    version_batch_size = 500
    object_true = False
    while True:
        response = get_object_versions(bucket, key, version_batch_size,
                                       key_marker)
        key_marker = response.get('NextKeyMarker')
        delete_markers = response.get('DeleteMarkers', [])

        markers = markers + [dict(
            Key=x.get('Key'),
            VersionId=x.get('VersionId')) for x in delete_markers if
            x.get('Key') == key and x.get('IsLatest')]
        for x in delete_markers:
            if key == x.get('Key') and not x.get('IsLatest'):
                object_true = True
        logger.info('{0} -- {1} delete '
                    'markers ...'.format(key_marker, len(markers)))
        if len(markers) >= max_markers or key_marker is None or \
                len(markers) == 0:
            break

    return {"delete_markers": markers, "key_marker": key_marker,
            "object_true": object_true}


def delete_delete_markers_single(bucket, key):
    logger.info("Check the key {0} whether existed".format(key))
    try:
        client.head_object(Bucket=bucket, Key=key)
        logger.info("Object {0} is existed in storage".format(key))
        logger.info("No need to restore object {0}".format(key))
        print "{0} {1} {2}".format(
            key, "NoNeed", "object_exist")
    except exceptions.ClientError, e:
        logger.info("Head object {0} failed".format(key))
        logger.info(e.message)
        key_marker = None
        while True:
            info = get_delete_markers_info_key(bucket, key, key_marker)
            key_marker = info.get('key_marker')
            delete_markers = info.get('delete_markers', [])
            if len(delete_markers) > 0:
                logger.info(delete_markers)
                try:
                    response = client.delete_objects(
                        Bucket=bucket,
                        Delete={
                            'Objects': delete_markers,
                            'Quiet': True
                        }
                    )
                    logger.info(
                        'Deleting {0} delete markers ... '.format(key))
                    logger.info('Done with status {0}'.format(
                        response.get('ResponseMetadata', {}).get(
                            'HTTPStatusCode')))
                    if response.get('ResponseMetadata', {}).get(
                            'HTTPStatusCode') is not 200:
                        fail_list.append(key)
                        print "{0} {1} {2}".format(
                            key,
                            "Fail",
                            response.get("Body", {}).get("Content"))
                    else:
                        sucess_list.append(key)
                        print "{0} {1}".format(
                            key,
                            "Success")

                except exceptions:
                    logger.error(
                        "Deleting {0} delete marker failed".format(key))
                    logger.error(exceptions.ClientError.message)
                    print "{0} {1} {2}".format(
                        key,
                        "Fail",
                        exceptions.ClientError.message)
            elif len(delete_markers) == 0 and not info.get('object_true'):
                logger.info("object {0} not found".format(key))
                print "{0} {1}".format(
                    key,
                    "NoFound")
                break
            else:
                break


if __name__ == "__main__":
    parser = ArgumentParser(
        description='restore objects from file')
    parser.add_argument('--user', dest='user', required=True, help='s3 user')
    parser.add_argument('--password', dest='password', required=True,
                        help='password for special s3 user')
    parser.add_argument('--bucket', dest='bucket', required=True,
                        help='bucket name')
    parser.add_argument('--key', dest='key', default=None,
                        help='retore key name')
    parser.add_argument('--file', dest='file_name', default=None,
                        help='the file which restore files name')
    parser.add_argument('--endpoint', dest='server', required=True,
                        help='The address of server')
    parser.add_argument('--prefix', dest='prefix', default=None,
                        help='the prefix of object')
    args = parser.parse_args()
    client = boto3.client(
        "s3", endpoint_url=args.server,
        aws_access_key_id=args.user,
        aws_secret_access_key=args.password,
        config=client_config)
    if args.file_name:
        print "{0} {1} {2}".format(
            "Name", "Status", "Detail")
        file_ids = []
        with open(args.file_name, 'r') as f:
            for line in f:
                file_ids.append(line.strip())
        for id in file_ids:
            delete_delete_markers_single(bucket=args.bucket,
                                         key=id)
        logger.info("total number os delete marker succeed: "
                    "{0}".format(len(sucess_list)))
        print "total number of delete marker succeed: " \
              "{0}".format(len(sucess_list))
        logger.info("succeed list {0}".format(" ".join(sucess_list)))
        logger.info("total number of delete marker failed: "
                    "{0}".format(len(fail_list)))
        logger.info("succeed list {0}".format(" ".join(fail_list)))

    elif args.key:
        print "{0} {1} {2}".format(
            "Name", "Status", "Detail")
        delete_delete_markers_single(
            bucket=args.bucket, key=args.key)
    elif not args.file_name and not args.key:
        if not args.prefix:
            delete_delete_markers(bucket=args.bucket, prefix="")
        else:
            delete_delete_markers(bucket=args.bucket, prefix=args.prefix)
