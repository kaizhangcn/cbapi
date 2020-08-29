import sys
import json
import requests
import pandas as pd
import threading
from datetime import datetime, date, time, timedelta, timezone

class CrunchbaseAPI():
    '''
    The API to download and present organization and people data from Crunchbase.
    '''

    def __init__(self, RAPIDAPI_KEY):
        self.RAPIDAPI_KEY = RAPIDAPI_KEY
        self.headers = {
            'x-rapidapi-host': "crunchbase-crunchbase-v1.p.rapidapi.com",
            'x-rapidapi-key': RAPIDAPI_KEY
        }
        self.url_organization = 'https://crunchbase-crunchbase-v1.p.rapidapi.com/odm-organizations'
        self.url_people = 'https://crunchbase-crunchbase-v1.p.rapidapi.com/odm-people'

    def target_page_retrieve(self, page_data_list, start_page, end_page, querystring, querytype):
        '''
        This function retrieve data from start_page to end_page (not included) and put in page_data_list
        Input:
            page_data_list: list of dataframes
            start_page, end_page: integers
            querystring: dictionary of query info
            querytype: 'org' or 'ppl', indicating organization or people
        Output:
            None
        '''
        
        if querytype == 'org':
            url = self.url_organization
        elif querytype == 'ppl':
            url = self.url_people
        else:
            raise Exception(f'Query type does not exist: {querytype}')
        
        # create a new query string and specify the page
        temp_querystring = querystring.copy()
        for page in range(start_page, end_page):
            temp_querystring['page'] = page
            
            #request
            response = requests.request('GET', url, headers = self.headers, params = temp_querystring)
            
            # check if status code is correct
            # if not, raise an exception
            if (200 == response.status_code):
                api_response = json.loads(response.text)
            else:
                raise Exception(f'Error in request: {response.status_code}')
                
            # print the progress
            current_page = api_response['data']['paging']['current_page']
            total_page = api_response['data']['paging']['number_of_pages']
            print(f'Retrieving page {current_page}/{total_page}')
            
            # retrieve data
            page_data = pd.concat([pd.DataFrame([item['properties']]) for item in api_response['data']['items']])
            # update page_data_listv
            page_data_list[page] = page_data

            
    def trigger_api_organization(self, updated_since = None, query = None, name = None, domain_name = None, locations = None, 
                                 organization_types = None, sort_order = None, page = None, max_threads = 1):
        '''
        This function gets orgnization data from Crunchbase.
        Input: 
        
            updated_since: NUMBER OPTIONAL
            When provided, restricts the result set to Organizations where updated_at >= the passed value
            
            query: STRING OPTIONAL
            Full text search of an Organization's name, aliases (i.e. previous names or "also known as"), and short description
            
            name: STRING OPTIONAL
            Full text search limited to name and aliases
            
            domain_name: STRING OPTIONAL
            Text search of an Organization's domain_name (e.g. www.google.com)
            
            locations: STRING OPTIONAL
            Filter by location names (comma separated, AND'd together) e.g. locations=California,San Francisco
            
            organization_types: STRING OPTIONAL
            Filter by one or more types. Multiple types are separated by commas. Available types are "company", "investor", 
            "school", and "group". Multiple organization_types are logically AND'd.
            
            sort_order: STRING OPTIONAL
            The sort order of the collection. Options are "createdat ASC", "createdat DESC", "updatedat ASC", and
            "updatedat DESC"
            
            page: NUMBER OPTIONAL
            Page number of the results to retrieve.
            
            max_threads: NUMBER
            Maximum threads you would like to use.
        
        Output:
        
            Pandas dataframe of retrieved organization data.
        
        '''
        
        # construct query information dictionary
        querystring = {}
        if updated_since:
            querystring['updated_since'] = str(updated_since)
        if query:
            querystring['query'] = query
        if name:
            querystring['name'] = name
        if domain_name:
            querystring['domain_name'] = domain_name
        if locations:
            querystring['locations'] = locations
        if organization_types:
            querystring['organization_types'] = organization_types
        if sort_order:
            querystring['sort_order'] = sort_order
        if page:
            querystring['page'] = str(page)
        
        # request
        response = requests.request('GET', self.url_organization, headers = self.headers, params = querystring)
        
        # check if status code is correct
        # if not, raise an exception
        if (200 == response.status_code):
            api_response = json.loads(response.text)
        else:
            raise Exception(f'Error in request: {response.status_code}')
        
        # if page is given by user, simply return the retrieved data
        if page:
            return pd.concat([pd.DataFrame([item['properties']])
                              for item in api_response['data']['items']]).reset_index(drop=True)
        
        # if page is not given by user, we need to retrieve all pages using multithreading
        else:
            # total number of pages
            number_of_pages = api_response['data']['paging']['number_of_pages']
            # list of dataframes that contains contents of each page
            page_data_list = [None] * number_of_pages
            # list of threads
            threads = []
            
            if number_of_pages <= max_threads:
                # only need number_of_pages threads, each thread process one page
                for i in range(number_of_pages):
                    t = threading.Thread(
                        target = CrunchbaseAPI.target_page_retrieve, 
                        args = (self, page_data_list, i, i + 1, querystring, 'org'))
                    threads.append(t)
                    t.start()
            else:
                # need max_threads threads, each has a range of pages to process
                d = number_of_pages // max_threads
                for i in range(max_threads - 1):
                    t = threading.Thread(
                        target = CrunchbaseAPI.target_page_retrieve,
                        args = (self, page_data_list, i * d, (i + 1) * d, querystring, 'org'))
                    threads.append(t)
                    t.start()
                # notice that the last thread has a different end point
                t = threading.Thread(
                    target = CrunchbaseAPI.target_page_retrieve,
                    args = (self, page_data_list, (max_threads - 1) * d, number_of_pages, querystring, 'org'))
                threads.append(t)
                t.start()
                    
            for t in threads:
                t.join()
            
            # integrate to one dataframe
            return pd.concat(page_data_list).reset_index(drop=True)

        
        
    def trigger_api_people(self, name = None, query = None, updated_since = None, sort_order = None, page = None, 
                       locations = None, socials = None, types = None, max_threads = 1):
        '''
        This function gets people data from Crunchbase.
        Input: 
            
            name: STRING OPTIONAL
            A full-text query of name only
            
            query: STRING OPTIONAL
            A full-text query of name, title, and company
            
            updated_since: NUMBER OPTIONAL
            When provided, restricts the result set to People where updated_at >= the passed value
            
            sort_order: STRING OPTIONAL
            The sort order of the collection. Options are "createdat ASC", "createdat DESC", "updatedat ASC", and "updatedat 
            DESC"
            
            page: NUMBER OPTIONAL
            the 1-indexed page number to retrieve
            
            locations: STRING OPTIONAL
            Filter by location names (comma separated, AND'd together) e.g. locations=California,San Francisco
            
            socials: STRING OPTIONAL
            Filter by social media identity (comma separated, AND'd together) e.g. socials=ronconway
            
            types: STRING OPTIONAL
            Filter by type (currently, either this is empty, or is simply "investor")
            
            max_threads: NUMBER
            Maximum threads you would like to use.
        
        Output:
        
            Pandas dataframe of retrieved people data.
        
        '''
        
        # construct query information dictionary
        querystring = {}
        if name:
            querystring['name'] = name
        if query:
            querystring['query'] = query
        if updated_since:
            querystring['updated_since'] = str(updated_since)
        if sort_order:
            querystring['sort_order'] = sort_order
        if page:
            querystring['page'] = str(page)
        if locations:
            querystring['locations'] = locations
        if socials:
            querystring['socials'] = socials
        if types:
            querystring['types'] = types
        
        # request
        response = requests.request('GET', self.url_people, headers = self.headers, params = querystring)
        
        # check if status code is correct
        # if not, raise an exception
        if (200 == response.status_code):
            api_response = json.loads(response.text)
        else:
            raise Exception(f'Error in request: {response.status_code}')
        
        # if page is given by user, simply return the retrieved data
        if page:
            return pd.concat([pd.DataFrame([item['properties']])
                              for item in api_response['data']['items']]).reset_index(drop=True)
        
        # if page is not given by user, we need to retrieve all pages using multithreading
        else:
            # total number of pages
            number_of_pages = api_response['data']['paging']['number_of_pages']
            # list of dataframes that contains contents of each page
            page_data_list = [None] * number_of_pages
            # list of threads
            threads = []
            
            if number_of_pages <= max_threads:
                # only need number_of_pages threads, each thread process one page
                for i in range(number_of_pages):
                    t = threading.Thread(
                        target = CrunchbaseAPI.target_page_retrieve, 
                        args = (self, page_data_list, i, i + 1, querystring, 'ppl'))
                    threads.append(t)
                    t.start()
            else:
                # need max_threads threads, each has a range of pages to process
                d = number_of_pages // max_threads
                for i in range(max_threads - 1):
                    t = threading.Thread(
                        target = CrunchbaseAPI.target_page_retrieve,
                        args = (self, page_data_list, i * d, (i + 1) * d, querystring, 'ppl'))
                    threads.append(t)
                    t.start()
                # notice that the last thread has a different end point
                t = threading.Thread(
                    target = CrunchbaseAPI.target_page_retrieve,
                    args = (self, page_data_list, (max_threads - 1) * d, number_of_pages, querystring, 'ppl'))
                threads.append(t)
                t.start()
                    
            for t in threads:
                t.join()
            
            # integrate to one dataframe
            return pd.concat(page_data_list).reset_index(drop=True)