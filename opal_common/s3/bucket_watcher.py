from opal_server.config import opal_server_config
import boto3
from boto3.session import Session
import time


class BucketWatcher(object):
    """
    Watches s3 bucket for changes and can trigger callbacks
    when detecting new versions of objects in the tracked bucket.

    """
    def __init__(self, bucket_name=opal_server_config.AWS_BUCKET_NAME, polling_interval=10):
        self.bucket_name = bucket_name
        self._polling_interval = polling_interval
        self._session = Session(
            aws_access_key_id=opal_server_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=opal_server_config.AWS_SECRET_ACCESS_KEY,
            aws_session_token=opal_server_config.AWS_SESSION_TOKEN
        )
        self._s3 = self._session.resource('s3')
        self.bucket = self._s3.Bucket()

    def get_all_objects(self):
        return [obj for obj in self.bucket.objects.all()]

    def get_last_modified_file(self):
        objects = self.get_all_objects()
        get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
        objects = [obj for obj in sorted(objects, key=get_last_modified)]
        return objects[-1].key

    def list_all_files(self):
        for file in self.bucket.objects.all():
            print(file.key)

    def download_file(self, filename):
        self._s3.Object(self.bucket_name, filename).download_file(f'/tmp/{filename}')

    def _do_polling(self):
        """
        task to periodically check the remote for changes
        """
        curr_file = self.get_last_modified_file()
        while True:
            latest_file = self.get_last_modified_file()
            if latest_file != curr_file:
                self.download_file(latest_file)
                curr_file = latest_file
                # TODO: notify the client about new policy bundle
            time.sleep(self._polling_interval)

    #TODO: S3BundleMaker read new files from /tmp and create a bundle

