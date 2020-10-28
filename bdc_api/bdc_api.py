import requests
import json
import os
import six
import base64
import urllib

from bson import ObjectId
from bson.errors import InvalidId
from collections import namedtuple

def b64encode(source):
    """Base-64 encoding method compatible with python2 and python3.
    """
    if six.PY3:
        source = source.encode('utf-8')
    content = base64.b64encode(source).decode('utf-8')
    return content

class BdcApiException(Exception):
    pass

class BdcApi(object):
    """Class responsible for handling all API calls.
    """
    URL_TASK_NUMBERS='minos_restapi/tasks_info'
    URL_DOMAINS='minos_restapi/domains_info'
    URL_DATACOLLECTIONS='minos_restapi/datacollections'
    URL_FILELIST='minos_restapi/files'
    URL_QUERY='minos_restapi/request_data'
    URL_PROGRESS='minos_restapi/progress'
    URL_DOWNLOAD='minos_restapi/download'
    COMPLETE_QUERY = '100%'
    QUERY_ACCEPT_TYPES = ['application/zip', 'application/x-hdf']
    COMPLETE_QUERY_STATUS = ['failed', 'cancelled', 'failed (no read access to any data included)',
                             'failed (no data matched all conditions requested)']
    QueryInfo = namedtuple('QueryInfo', 'progress status')

    def __init__(self, username, api_key, hostname):
        """Initialize username, API key, and session information.

        Parameters:

            :username: valid API username.
            :api_key: valid API key generated from the wesbite under `REST API Key Manager`.
            :hostname: Hostname to be used throughout the session (e.g: `https://minos.lbl.gov`)

        Returns:

            - New `BdcApi` object. 
        
        Raises:

            - None. 
        """
        self._host = hostname
        self.username = username
        self.auth_header = 'Basic {0}'.format(b64encode('{0}:{1}'.format(username, api_key)))
        self.session = requests.Session()

    def get_task_numbers(self):
        """Get the task numbers associated with the logged-in user's organizations.

        Parameters:

            - None. 
        
        Returns:

            - Dictionary with keys in the format `{task name}/{task number}'
              and value being a dict including POCs and the ID of the task. For e.g:

              .. code-block:: json

                    {"Electromagnetic / 2.1": 
                        {"POCS": [{"username": "admin", "laboratory": "LBNL", "fullname": " "}], 
                         "ID": "5d9e26ada81660b57e387f49"}
                         }

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        response = self._send_get(self.URL_TASK_NUMBERS)
        return json.loads(response.content)

    def get_domains(self, task_ID=''):
        """Get the domains associated with all tasks (associated with the user),
            or just those associated with a specified task.

        
        Parameters:

            :task_ID: Optional task ObjectID.
        
        Returns:

            - Dictionary of task names, whose values are their associated domain names.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        if not self._valid_id(task_ID):
            raise BdcApiException(f'{task_ID} is not a valid ObjectId!')
        response = self._send_get('{0}/{1}'.format(self.URL_DOMAINS, task_ID))
        return json.loads(response.content)

    def get_datacollections(self, task_numbers=[], domains=[], time_limits=[], limit=0):
        """Get all available datacollections, optionally filtered.

        Parameters (all optional):

            :task_numbers: A list of task number names.
            :domains: A list of domain names.
            :time_limits: A list of 2 timestamps.
            :limit: Maximum number of datacollections to return.

        Returns: 
            
            - List of datacollection names.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint) or when no datacollections are found.
        """
        if not isinstance(task_numbers, list):
            task_numbers = [task_numbers]
        if not isinstance(domains, list):
            domains = [domains]

        parameters = {}
        if task_numbers:
            parameters['tasks'] = ','.join(task_numbers)
        if domains:
            parameters['domains'] = ','.join(domains)
        if time_limits: 
            parameters['time_limits'] = ','.join([str(number) for number in time_limits])
        if limit:
            parameters['limit'] = limit

        if parameters != {}:
            response = self._send_get('{0}'.format(self.URL_DATACOLLECTIONS), parameters=parameters)
        else:
            response = self._send_get('{0}/'.format(self.URL_DATACOLLECTIONS))
        try:
            datacollection_names = [doc['name'] for doc in json.loads(response.content)]
        except ValueError as e:
            if len(response.content) == 0:
                raise BdcApiException('No datacollections found.')
            else:
                raise e
        return datacollection_names

    def get_files(self, datacollections=[], extensions='', limit=0):
        """Get file names from all or selected datacollections.
        
        Parameters (optional):

            :datacollections: list of datacollection names whose filenames are desired.
            :limit: limit on data collections returned (defaults to 0, meaning no limit).
            :extensions: comma-separated string of desired extensions (e.g: 'json,txt')

        Returns:

            - Dictionary of the form

                .. code-block:: json

                    {
                        "datacollection_1": ["filename1", "filename2"],
                        "datacollection_2": ["filename3", "filename4"]
                    }

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        if not isinstance(datacollections, list):
            datacollections = [datacollections]
        parameters = {'limit': limit}
        if datacollections:
            parameters['datacollections'] = ','.join(list(datacollections))
        if extensions:
            parameters['extensions'] = extensions
        response = self._send_get(self.URL_FILELIST, parameters=parameters)
        return json.loads(response.content)

    def start_files_query(self, files):
        """Initiate a query to download specified files.

        Parameters:
            
            :files: List of desired file names as selected from `get_files`.

        Returns:

            - Query ID with which to check query progress or request the download.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        if not isinstance(files, list):
            files = [files]
        post_data = {'filepaths': ','.join(files)}
        headers = {'Accept': ', '.join(self.QUERY_ACCEPT_TYPES)}
        response = self._send_post(self.URL_QUERY, post_data, headers=headers)
        response = json.loads(response.content)
        if 'query_id' in response:
            return response['query_id']
        elif 'error_message' in response:
            raise BdcApiException(response['error_message'])
        else:
            raise BdcApiException('Unknown response received when requesting files!')

    def start_datacollection_query(self, datacollection):
        """Initiate a query to download all files associated with a datacollection.

        Parameters:

            :datacollection: name of datacollection to get data for.

        Returns:

            - Query ID with which to check query progress or request the download.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        if not isinstance(datacollection, str):
            raise BdcApiException('Please use a datacollection name in string form.')
        post_data = {'datacollection': datacollection}
        headers = {'Accept': ', '.join(self.QUERY_ACCEPT_TYPES)}
        response = self._send_post(self.URL_QUERY, post_data, headers=headers)
        response = json.loads(response.content)
        if 'query_id' in response:
            return response['query_id']
        elif 'error_message' in response:
            raise BdcApiException(response['error_message'])
        else:
            raise BdcApiException('Unknown response received when requesting files!')

    def check_query_progress(self, query_id):
        """Check the progress query.

        Parameters:

            :query_id: Query ID from one of the `start_datacollection_query` 
                       or `start_files_query` functions.

        Returns:

            - Named tuple object QueryInfo where the `progress` field
              contains the percentage progress of the query and the `status`
              field includes the job status.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        if not self._valid_id(query_id):
            raise BdcApiException(f'{query_id} is not a valid ObjectId!')        
        response = self._send_get('{0}/{1}'.format(self.URL_PROGRESS, query_id))
        response = json.loads(response.content)
        if 'errormessage' in response:
            raise BdcApiException(response['errormessage'])
        else:
            return self.QueryInfo(progress=response['progress'], 
                            status=response['job_status'])

    def save_file(self, query_id, jupyterhub=False, local_path=""):
        """This function will save the result of a query to the given directory
        on local disk, or, if the jupyterhub flag is set to True, to the user's
        JupyterHub home directory. The latter requires that the JupyterHub home
        directory has been created and is accessible to the Apache user that
        runs the service.

        The response will contain either a success or error message returned by
        the service.

        Parameters:

            :query_id: Query ID from one of the `start_datacollection_query` or
                       `start_files_query` functions.
            :jupyterhub: Whether or not the API is being run from JupyterHub
                         (default False).
            :local_path: A path to a directory on local disk, which will only be
                         considered if the API is not being run from JupyterHub.

        Returns:
            
            - Upon success if the jupyterhub flag is True, will return a dict
              with the success message:

              .. code-block:: json
              
                    {"message": 
                        "Successfully moved query results to home directory. Filename is: download_5d9407266362395834cdbfbe.zip"}

              Upon success if the jupyterhub flag is False, will return a dict
              with the success message:

              .. code-block:: json

                    {"message":
                        "Successfully downloaded query results to /path/on/disk. Filename is: download_5d9407266362395834cdbfbe.zip"}

              Upon failure, will raise `BdcApiException`.
        
        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or
              issues reaching the API endpoint).
        """
        if not self._valid_id(query_id):
            raise BdcApiException(f'{query_id} is not a valid ObjectId!')
        jupyterhub = bool(jupyterhub)
        if jupyterhub:
            parameters = {'jupyterhub': int(jupyterhub)}
            response = self._send_get(
                '{0}/{1}'.format(self.URL_DOWNLOAD, str(query_id)), parameters=parameters)
            response = json.loads(response.content)
        else:
            response = self._save_file_local(str(query_id), local_path)
        return response

    def _save_file_local(self, query_id, path):
        """Helper function that attempts to save the results of the given query
        to the given directory on local disk.

        Parameters:

            :query_id: Query ID from one of the `start_datacollection_query` or
                       `start_files_query` functions.
            :path: A directory on local disk to save the results to.

        Returns:

            - A dict with the success message:

              .. code-block:: json

                    {"message":
                        "Successfully downloaded query results to /path/on/disk. Filename is: download_5d9407266362395834cdbfbe.zip"}

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or
              issues reaching the API endpoint) or OS errors during the save
        """
        if not self._valid_id(query_id):
            raise BdcApiException(f'{query_id} is not a valid ObjectId!')
        def get_file_name_from_http_response(http_response):
            """Retrieve the file name from the given HTTP response, or the empty string. If the
            parent method is run from tests, return a default name."""
            if isinstance(self.session, requests.Session):
                return http_response.headers['content-disposition'].split("=")[1]
            return "test.zip"
        if not os.path.exists(path) or not os.path.isdir(path):
            raise BdcApiException('Invalid directory "{0}".'.format(path))
        headers = {'Accept': ', '.join(self.QUERY_ACCEPT_TYPES)}
        response = self._send_get(
            '{0}/{1}'.format(self.URL_DOWNLOAD, str(query_id)), headers=headers)
        try:
            file_name = get_file_name_from_http_response(response).strip()
            if not file_name:
                raise ValueError('Empty file name {0}.'.format(file_name))
        except Exception as e:
            raise BdcApiException(
                'Error occurred while retrieving file name from response. Details: {0}'.format(e))
        try:
            full_path = os.path.join(path, file_name)
            open(full_path, 'wb').write(response.content)
        except OSError as e:
            raise BdcApiException(
                'Error occurred while saving file to {0}. Details: {1}'.format(full_path, e))
        return {'message': ('Successfully downloaded query results to "{0}". Filename is: '
                            '{1}.').format(path, file_name)}
    
    def _valid_id(self, to_validate):
        if not to_validate:
            return True
        try:
            validated = ObjectId(to_validate)
            return True
        except InvalidId:
            return False

    def _send_get(self, url, parameters=None, headers={}):
        """Helper function to send GET requests.

        Parameters:

            :url: URL of API endpoint suffix as a string.
            :parameters: GET parameters to include.
            :headers: Request headers to include.

        Returns:

            - Response from server.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        headers.update({'Authorization': self.auth_header})
        response = None
        try:
            # Production case: session.get has a particular signature which we use:
            if isinstance(self.session, requests.Session):
                if parameters:
                    response = self.session.get(
                        '{0}/{1}'.format(self._host, url), params=parameters, headers=headers)
                else:
                    response = self.session.get(
                        '{0}/{1}'.format(self._host, url), headers=headers)
            # Test case: session is really a Django Client object, different signature:
            else:
                if parameters:
                    params = urllib.parse.urlencode(parameters)
                    response = self.session.get(
                        '{0}/{1}/?{2}'.format(self._host, url, params), headers=headers)
                else:
                    response = self.session.get(
                        '{0}/{1}'.format(self._host, url), headers=headers)
            response.raise_for_status()
        except Exception as e:
            if not response:
                raise BdcApiException('Error sending request to host server: {0}', e)

        if b'error_message' in response.content: 
            raise BdcApiException('Error occurred while making request: {0}',
                    json.loads(response.content)['error_message'])
        elif b'errormessage' in response.content:
            raise BdcApiException('Error occurred while making request: {0}',
                    json.loads(response.content)['errormessage'])
        return response
    
    def _send_post(self, url, post_data, headers={}):
        """Helper function to send POST requests.

        Parameters:

            :url: of API endpoint suffix as a string.
            :post_data: Data to attach to POST request.
            :headers: Request headers to include.

        Returns:

            - Response from server.

        Raises:

            - BdcApiException on problematic requests (e.g. malformed inputs or 
                issues reaching the API endpoint).
        """
        headers.update({'Authorization': self.auth_header})
        response = None
        try:
            response = self.session.post(
                '{0}/{1}/'.format(self._host, url), data=post_data, headers=headers)
        except Exception as e:
            if not response:
                raise BdcApiException('Error sending request to host server: {0}', e)

        if b'error_message' in response.content: 
            raise BdcApiException('Error occurred while making request: {0}',
                    json.loads(response.content)['error_message'])
        elif b'errormessage' in response.content:
            raise BdcApiException('Error occurred while making request: {0}',
                    json.loads(response.content)['errormessage'])
        return response
