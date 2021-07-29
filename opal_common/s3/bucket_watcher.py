from opal_server.config import opal_server_config
from opal_common.logger import logger
from boto3 import session
import time


class BucketWatcher(object):
    """
    Watches s3 bucket for changes and can trigger callbacks
    when detecting new versions of objects in the tracked bucket.

    """
    def __init__(self, bucket_name=opal_server_config.AWS_BUCKET_NAME, polling_interval=10):
        self.bucket_name = bucket_name
        self._polling_interval = polling_interval
        self._session = session.Session(
            aws_access_key_id=opal_server_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=opal_server_config.AWS_SECRET_ACCESS_KEY,
            region_name=opal_server_config.AWS_REGION_NAME,
        )
        self._s3 = self._session.resource('s3')
        self.bucket = self._s3.Bucket(self.bucket_name)
        self.curr_file = self.get_last_modified_file()
        self.curr_version = self.latest_version(self.curr_file)

    def get_last_modified_file(self):
        """
        Return the newest file in the S3 bucket
        """
        objects = [obj for obj in self.bucket.objects.all()]
        get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
        objects = [obj for obj in sorted(objects, key=get_last_modified)]
        return objects[-1].key

    def download_updated_file(self):
        """
        Simple download of file from S3
        """
        logger.info("FOUND NEW FILE OR VERSION, downloading {} ver {}", self.curr_file, self.curr_version)
        obj = self._s3.Object(self.bucket_name, self.curr_file)
        obj.download_file(f'/tmp/{self.curr_file}', ExtraArgs={'VersionId': self.curr_version})

    def latest_version(self, key):
        """
        return the latest version of a specific file key
        """
        versions = self.bucket.object_versions.filter(Prefix=key)

        for version in versions:
            obj = version.get()
            logger.debug("versionId {} length {} lastmod {} IsLatest {}",
                         obj.get('VersionId'), obj.get('ContentLength'), obj.get('LastModified'), version.is_latest)
            if version.is_latest:
                return version.id

    def updated_file_and_version(self):
        """
        Update the current file and version
        returns a boolean which states if an update occured
        """
        latest_file = self.get_last_modified_file()
        if latest_file != self.curr_file:
            self.curr_file = latest_file
            self.curr_version = self.latest_version(self.curr_file)
            return True

        latest_version = self.latest_version(self.curr_file)
        if latest_version != self.curr_version:
            self.curr_version = latest_version
            return True
        return False

    def do_polling(self):
        """
        task to periodically check the remote for changes
        """
        # TODO: use async instead of running the method directly with time.sleep
        while True:
            logger.info("curr_file {} curr_version {}", self.curr_file, self.curr_version)
            if self.updated_file_and_version():
                self.download_updated_file()
                # TODO: notify the client about new policy bundle and create the bundle
            time.sleep(self._polling_interval)

    # TODO: def S3BundleMaker(self): read new files from /tmp and create a bundle

