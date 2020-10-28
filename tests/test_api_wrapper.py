from __future__ import absolute_import

# import os
# import shutil
from bdc_api import *
from unittest.mock import patch

import unittest
import json

class TestBdcApiInterface(unittest.TestCase):
    
    def setUp(self):
        self.api = BdcApi('test_user', 
                        'somekey', 'localhost')
        # Creating some test datacollections and divvying them up
        # between data domains and tasks. Each has time limits
        # slightly different than the one before it.
        self.coll_docs = []
        self.files = {}
        for i in range(10):
            name = 'test_coll_{0}'.format(i)
            coll_doc = {'_id': {'$oid':"5c772de88b751502b44f22ba"},
                        'name': name}
            self.coll_docs.append(coll_doc)
            self.files[name] = [f'/file_{i}.txt',f'/file_{i+1}.txt']        

    @patch('bdc_api.BdcApi._send_get')
    def test_get_task_numbers(self, mock_get):
        mock_get.return_value.status_code = 200
        return_val = {'Task 1-ARES': 
                        {'POCS': [
                            {'username': 'admin', 
                             'laboratory': 'LBNL', 
                             'fullname': ' '}], 
                         'ID': '5d9e26ada81660b57e387f49'},
                        'Task 2-ARES_2': 
                        {'POCS': [
                            {'username': 'admin', 
                             'laboratory': 'LBNL', 
                             'fullname': ' '}], 
                         'ID': '5d9e26ada81660b57e387f50'}
                         }
        mock_get.return_value.content = json.dumps(return_val)
        tasks = self.api.get_task_numbers()
        assert len(tasks) == 2
        assert 'Task 1-ARES' in tasks
        assert 'Task 2-ARES_2' in tasks

    @patch('bdc_api.BdcApi._send_get')
    def test_get_domains_success(self, mock_get):
        """Test get_domains successful cases."""
        mock_get.return_value.status_code = 200
        return_val = {'ARES': ['domain_1', 'domain_2'],
                      'TEST_TASK_1' : ['domain_3'],
                      'TEST_TASK_2' : ['domain_4', 'domain_5']}
        mock_get.return_value.content = json.dumps(return_val)           
        domains = self.api.get_domains()
        assert len(domains) == 3
        assert 'ARES' in domains
        assert 'TEST_TASK_2' in domains
        assert 'TEST_TASK_1' in domains
        
        fake_id = '5d9e26ada81660b57e387f50'
        
        return_val = {'TEST_TASK_1' : ['domain_3']}
        mock_get.return_value.content = json.dumps(return_val)  
        domains = self.api.get_domains(fake_id)
        assert len(domains) == 1
        assert 'TEST_TASK_1' in domains

    @patch('bdc_api.BdcApi._send_get')
    def test_get_domain_fail(self, mock_get):
        """Test get_domains failing case"""   
        fake_id = '5d9e26ada81660b57e387f49'     
        return_val = {'ARES': ['domain_1', 'domain_2']}
        mock_get.return_value.content = json.dumps(return_val)  
        domains = self.api.get_domains(fake_id)
        assert len(domains) == 1
        assert 'ARES' in domains
        assert len(domains['ARES']) == 2
        self.assertRaises(BdcApiException, self.api.get_domains, 'not_an_ID')

    @patch('bdc_api.BdcApi._send_get')
    def test_get_datacollections_success(self, mock_get):
        """Test get_datacollections successful cases."""
        mock_get.return_value.status_code = 200
        return_val = self.coll_docs
        mock_get.return_value.content = json.dumps(return_val)
        dcols = self.api.get_datacollections()
        assert len(dcols) == 10

        return_val = self.coll_docs[0:5]
        mock_get.return_value.content = json.dumps(return_val)
        dcols = self.api.get_datacollections(limit=5)
        assert len(dcols) == 5

        task_1 = 'TEST_TASK_1'
        task_2 = 'TEST_TASK_2'

        return_val = self.coll_docs[0:5]
        mock_get.return_value.content = json.dumps(return_val)
        dcols = self.api.get_datacollections(task_numbers=task_1)
        assert dcols[0] == 'test_coll_0'
        assert dcols[-1] == 'test_coll_4'
        assert len(dcols) == 5

        return_val = self.coll_docs[0:3] + self.coll_docs[9:]
        mock_get.return_value.content = json.dumps(return_val)
        dcols = self.api.get_datacollections(task_numbers=[task_1, task_2],
                domains=['TEST_DOMAIN_2']) 
        assert dcols[0] == 'test_coll_0'
        assert dcols[-1] == 'test_coll_9'
        assert len(dcols) == 4

        return_val = [self.coll_docs[3]] + [self.coll_docs[5]]
        mock_get.return_value.content = json.dumps(return_val)
        dcols = self.api.get_datacollections(domains='TEST_DOMAIN_2',
                    time_limits=(1559774837, 1559774859))
        assert len(dcols) == 2
        assert 'test_coll_3' in dcols
        assert 'test_coll_5' in dcols

    @patch('bdc_api.BdcApi._send_get')
    def test_get_datacollections_fail(self, mock_get):
        """Test get_datacollections fail"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = ''
        self.assertRaises(BdcApiException, self.api.get_datacollections)
        
    @patch('bdc_api.BdcApi._send_get')
    def test_get_files(self, mock_get):
        """Ensure that listing files from datacollections works as expected."""
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = json.dumps(self.files)
        files = self.api.get_files()
        assert len(files) == 10
        mock_get.return_value.content = json.dumps({
                        'test_coll_0': self.files['test_coll_0']})
        files = self.api.get_files('test_coll_0')
        assert len(files['test_coll_0']) == 2

#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE='/tmp/{0}')
#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE_WITH_DATE='/tmp/{0}/{1}')
#     def test_save_file_local_success(self):
#         """Test normal operation of a save_file on local disk"""
#         jupyterhub_directory = settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user)
#         try:
#             os.mkdir(jupyterhub_directory)
#         except OSError:
#             pass
#         queryID = self.api.start_datacollection_query(self.uploaded_datacollection)
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one(
#                 {'_id': ObjectId(queryID)})
#             assert 'success' in query['job_status']

#         # Save the results to test.zip in a local directory.
#         local_directory = "/tmp"
#         file_name = "test.zip"
#         save_file_response = self.api.save_file(
#             queryID, jupyterhub=False, local_path=local_directory)

#         assert 'message' in save_file_response
#         assert save_file_response['message'] == (
#             'Successfully downloaded query results to "{0}". Filename is: '
#             '{1}.').format(local_directory, file_name)

#         # The results should have been saved to the specified local directory, not the user's
#         # JupyterHub home directory.
#         date_dir = date.today().strftime("%m-%d-%Y")
#         jupyterhub_path = os.path.join(jupyterhub_directory, date_dir, file_name)
#         assert not os.path.exists(jupyterhub_path)
#         local_path = os.path.join(local_directory, file_name)
#         assert os.path.exists(local_path)

#         os.remove(local_path)
#         shutil.rmtree(settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user))

#     def test_save_file_local_failure(self):
#         """Test that save_file fails locally given a bad path"""
#         queryID = self.api.start_datacollection_query(self.uploaded_datacollection)
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one(
#                 {'_id': ObjectId(queryID)})
#             assert 'success' in query['job_status']
#         # Attempt, and fail, to save the results to various bad directories.
#         for local_directory in ['', '/does/not/exist']:
#             try:
#                 self.api.save_file(queryID, jupyterhub=False, local_path=local_directory)
#                 self.fail('An exception should have been raised.')
#             except BdcApiException as e:
#                 assert str(e) == 'Invalid directory "{0}".'.format(local_directory)

#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE='/tmp/{0}')
#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE_WITH_DATE='/tmp/{0}/{1}')
#     def test_save_file_jupyterhub_success(self):
#         """Test that running save_file for a .zip on JupyterHub copies
#         the unzipped zip file to the user's home directory."""
#         try:
#             os.mkdir(settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user))
#         except OSError:
#             pass
#         queryID = self.api.start_datacollection_query(self.uploaded_datacollection)
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one({'_id': ObjectId(queryID)})
#             assert 'success' in query['job_status']   
#         save_file_response = self.api.save_file(queryID, jupyterhub=True)
#         assert('message' in save_file_response)
#         assert('Started moving query results to JupyterHub.'
#             in save_file_response['message'])
#         today_str = date.today().strftime('%m-%d-%Y')
#         destination_directory = f'/tmp/{self.test_user}/{today_str}'
#         directory_name = f'download_{queryID}'
#         unzipped_directory = os.path.join(destination_directory, directory_name)
#         self.assertIn(directory_name, os.listdir(destination_directory))
#         self.assertFalse(os.path.exists(f'{unzipped_directory}.zip'))
#         shutil.rmtree(settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user))

#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE='/tmp/{0}')
#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE_WITH_DATE='/tmp/{0}/{1}')
#     def test_save_file_jupyterhub_local_path_specified_success(self):
#         """Test that, even if a local path is specified, files saved to JupyterHub always write to
#         the user's home directory."""
#         jupyterhub_directory = settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user)
#         try:
#             os.mkdir(jupyterhub_directory)
#         except OSError:
#             pass
#         queryID = self.api.start_datacollection_query(self.uploaded_datacollection)
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one({'_id': ObjectId(queryID)})
#             assert 'success' in query['job_status']

#         # Attempt to specify a local path to save to, even though the jupyterhub flag is specified.
#         local_directory = "/tmp"
#         save_file_response = self.api.save_file(queryID, jupyterhub=True, local_path=local_directory)

#         assert('message' in save_file_response)
#         message = save_file_response['message']
#         assert ('Started moving query results to JupyterHub.' in message)
#         file_name = message[message.find("download_"):]
#         file_name = file_name.replace('.zip. The file will be unzipped.','')

#         # The results should have been saved to the user's JupyterHub home directory, not the
#         # specified local directory.
#         date_dir = date.today().strftime("%m-%d-%Y")
#         jupyterhub_path = os.path.join(jupyterhub_directory, date_dir, file_name)
#         assert os.path.exists(jupyterhub_path)
#         local_path = os.path.join(local_directory, file_name)
#         assert not os.path.exists(local_path)

#         shutil.rmtree(settings.JUPYTERHUB_HOME_DIR_TEMPLATE.format(self.test_user))

#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE='/tmp/{0}')
#     @override_settings(JUPYTERHUB_HOME_DIR_TEMPLATE_WITH_DATE='/tmp/{0}/{1}')    
#     def test_save_file_failure(self):
#         """Test failing save_file due to bad query ID"""
#         try:
#             self.api.save_file('not-a-valid-query-id')
#             assert False
#         except Exception as e:
#             assert isinstance(e, BdcApiException)

#     def test_bad_new_query_files(self):
#         """Ensure that bad file download queries cleanly."""
#         queryID = self.api.start_files_query('not_a_real_file')
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one({'_id': ObjectId(queryID)})
#             assert 'failed' in query['job_status']
#         assert self.api.check_query_progress(queryID).progress == '0%'
#         assert self.api.check_query_progress(queryID).status == JobStatus.failed

#     def test_bad_new_query_datacollection(self):
#         """Ensure that bad datacollection download queries cleanly."""
#         queryID = self.api.start_datacollection_query('not_a_real_datacollection')
#         with MongoConnection() as conn:
#             query = conn.set_collection(MongoCollections.queries).find_one({'_id': ObjectId(queryID)})
#             assert 'failed' in query['job_status']
#         assert self.api.check_query_progress(queryID).progress == '0%'
#         assert self.api.check_query_progress(queryID).status == JobStatus.failed

unittest.main()