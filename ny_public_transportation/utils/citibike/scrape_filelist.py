from xml.etree import ElementTree
import re
import requests
import pandas as pd
from ..constants import CITIBIKE_DATASET_URL

## Scrape citibike dataset urls from s3 bucket
def list_dataset_urls(url=CITIBIKE_DATASET_URL):
    response = requests.get(url)
    if response.status_code == 200:
        page_content = response.content
        # parse xml
        tree = ElementTree.fromstring(page_content)
        # for elem in tree.iter():
        #     print(elem.tag, elem.text)
        names = [elem.text for elem in tree.iter() if elem.tag.split('}')[-1] == 'Key' and elem.text.endswith('.zip')]
        #print(names)
        urls = [url + '/' + name.replace(' ', '%20') for name in names]
        return dict(zip(names, urls))
    else:
        print(f'Failed to retrieve page: status code {response.status_code}')
        return []
    
def retrieve_file_urls():
    url_dict = list_dataset_urls()
    df_files = pd.DataFrame(url_dict.items(), columns=['filename', 'url'])
    return df_files
