import requests
import json
import time
from pprint import pprint
import csv

import pymongo


class OctoScraper:
    def __init__(self, language, location, auth_string=None, silent=True, wait_throttle=60):
        self.headers = {
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "en-US,en;q=0.8",
            'accept': "application/json, text/javascript, */*; q=0.01",
            'connection': "keep-alive",
            'cache-control': "no-cache",
        }

        self._querystring_template = {"q": "type:user language:\"{language}\" location:\"{location}\"",
                                      "page": "{page}"}

        self.querystring = dict(self._querystring_template)
        self.querystring['q'] = self.querystring['q'].format(language=language, location=location)

        if auth_string is not None:
            self.set_auth(auth_string)

        self.silent = silent

        self.wait_throttle = wait_throttle

    def set_auth(self, auth_string):
        self.headers.update({'authorization': auth_string})

    def _get_request_data(self, url, headers=None, params=None, wait_retry=True):
        if url is None: return None

        while True:
            if params is None:
                response = requests.request("GET", url, headers=headers)
            else:
                response = requests.request("GET", url, headers=headers, params=params)

            r = response.text

            d = json.loads(r)

            d = self._get_status(d)

            if d.get('status') == 'wait' and wait_retry:
                time.sleep(self.wait_throttle)
                continue
            else:
                break

        return d

    def _get_status(self, d):

        try:
            d1 = dict(d)
        except ValueError:
            d1 = {'items': list(d)}

        records = d1.get('items')

        if not records:
            if "rate limit" in str(d1.get('message', '')):
                d1.update({'status': 'wait'})
            elif str(d1.get('message', '')) == "Only the first 1000 search results are available":
                d1.update({'status': 'done'})
            else:
                d1.update({'status': 'done?'})
        else:
            d1.update({'status': 'ok'})

        return d1

    def _add_data(self, records):
        l = list(records)

        headers = self.headers

        for record in l:
            user_url = record.get('url')
            repos_url = record.get('repos_url')

            user_data = self._get_request_data(user_url, headers)
            record.update({'user_data': user_data})

            repos_data = self._get_request_data(repos_url, headers)
            repo_list = repos_data.get('items', [])
            record.update({'repo_data': repo_list})

        return l

    def get_accounts(self, add_data=True):
        url = "https://api.github.com/search/users"

        headers = self.headers

        results = []
        page = 1
        record_count = 0

        while True:
            querystring = dict(self.querystring)
            querystring['page'] = querystring['page'].format(page=str(page))

            d = self._get_request_data(url, headers, querystring)

            if 'done' in d['status']: break

            records = d.get('items')

            print(records)

            input()

            if add_data:
                records = self._add_data(records)

            if not self.silent:
                pprint(records)

            record_count += len(records)

            results += records
            page += 1

        return results


def for_lianne(results):
    fieldnames = ['name', 'email', 'hireable', 'blog', 'html_url', 'location']

    language_count_dict = {}

    for result in results:

        repos = result.get('repo_data')

        for repo in repos:
            language = repo.get('language')
            if language in language_count_dict:
                language_count_dict[language] += 1
            else:
                language_count_dict.update({language: 1})

    language_count_tuple = sorted([(x, y) for x, y in language_count_dict.items()], key=lambda g: g[1], reverse=True)

    fieldnames += [x[0] for x in language_count_tuple]

    with open('output.csv', 'w', encoding='utf-8', errors='ignore') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames, lineterminator='\n')
        writer.writeheader()

        for result in results:

            repos = result.get('repo_data')
            language_count_dict = {}
            for repo in repos:
                language = repo.get('language')
                if language in language_count_dict:
                    language_count_dict[language] += 1
                else:
                    language_count_dict.update({language: 1})

            out_row = {
                'name': result['user_data']['name'],
                'email': result['user_data']['email'],
                'hireable': result['user_data']['hireable'],
                'blog': result['user_data']['blog'],
                'html_url': result['html_url'],
                'location': result['user_data']['location']
            }
            out_row.update(language_count_dict)

            writer.writerow(out_row)


if __name__ == '__main__':
    import pickle
    from secrets import *

    # import these from secrets.py
    auth_string = AUTH_STRING
    uri = MONGO_URI
    mongo_database = MONGO_DATABASE
    mongo_collection = MONGO_COLLECTION

    oscr = OctoScraper('python', 'toronto', auth_string=auth_string, silent=False)

    results = oscr.get_accounts()

    pickle.dump(results, open('temp.pkl', 'wb'))
    results = pickle.load(open('temp.pkl', 'rb'))

    conn = pymongo.MongoClient(uri)

    collection = conn[mongo_database][mongo_collection]

    for result in results:
        r = collection.update({'id': result['id']}, result, upsert=True)
        print(r)

    conn.close()

    for_lianne(results)
