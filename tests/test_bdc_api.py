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

    @patch('bdc_api.BdcApi._send_post')
    def test_start_files_query_success(self, mock_post):
        """Ensure that start_files_query works as expected."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.content = json.dumps(
                {'query_id': '5d9e26ada81660b57e387f49'})
        query_id = self.api.start_files_query(['file_1','file_2'])
        assert query_id == '5d9e26ada81660b57e387f49'

    @patch('bdc_api.BdcApi._send_post')
    def test_start_files_query_fail(self, mock_post):
        """Ensure that start_files_query fails as expected."""
        mock_post.return_value.status_code = 200
        error_message_1 = 'Only filepaths OR datacollection '
        error_message_1 += 'selection is allowed, not both.'
        mock_post.return_value.content = json.dumps(
                {'error_message': error_message_1})
        self.assertRaises(BdcApiException, self.api.start_files_query, 
                ['file_1','file_2'])        
    
    @patch('bdc_api.BdcApi._send_post')
    def test_start_datacollection_query_success(self, mock_post):
        """Ensure that start_datacollection_query works as expected."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.content = json.dumps(
                {'query_id': '5d9e26ada81660b57e387f49'})
        query_id = self.api.start_datacollection_query('coll_1')
        assert query_id == '5d9e26ada81660b57e387f49'        

    @patch('bdc_api.BdcApi._send_post')
    def test_start_datacollection_query_fail_1(self, mock_post):
        """Ensure that start_datacollection_query fails as expected (errormessage)"""
        mock_post.return_value.status_code = 200
        error_message_1 = 'Only filepaths OR datacollection '
        error_message_1 += 'selection is allowed, not both.'
        mock_post.return_value.content = json.dumps(
                {'error_message': error_message_1})
        self.assertRaises(BdcApiException, self.api.start_datacollection_query, 
                'coll_2')

    @patch('bdc_api.BdcApi._send_post')
    def test_start_datacollection_query_fail_2(self, mock_post):
        """Ensure that start_datacollection_query fails as expected (bad input)"""        
        self.assertRaises(BdcApiException, self.api.start_datacollection_query, 
                ['coll_1','coll_2'])

    @patch('bdc_api.BdcApi._send_get')
    def test_check_query_progress_success(self, mock_get):
        """Ensure that check_query_progress works as expected"""
        mock_get.return_value.content = json.dumps({'progress': 100,
                                            'job_status': 'success'})
        query_id = '5d9e26ada81660b57e387f49'
        query_status = self.api.check_query_progress(query_id)
        assert query_status.progress == 100
        assert query_status.status == 'success'

    @patch('bdc_api.BdcApi._send_get')
    def test_check_query_progress_fail_1(self, mock_get):
        """Ensure that check_query_progress fails as expected (errormessage)"""
        mock_get.return_value.status_code = 200
        error_message = 'Query ID was invalid or you are not '
        error_message += 'allowed access to this query.'
        mock_get.return_value.content = json.dumps({'errormessage': error_message})
        query_id = '5d9e26ada81660b57e387f49'
        self.assertRaises(BdcApiException, self.api.check_query_progress, 
                query_id)

    @patch('bdc_api.BdcApi._send_get')
    def test_check_query_progress_fail_2(self, mock_get):
        """Ensure that check_query_progress fails as expected (bad query_id)"""
        query_id = 'not_an_ID'
        self.assertRaises(BdcApiException, self.api.check_query_progress, 
                query_id)

if __name__ == '__main__':
    unittest.main()