import requests

url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
response = requests.get(url)
response_string = response.text

# print(response_string)
for i in response.json():
    if "theme" in i and 'Hospitals' in i['theme']:
        print(i['distribution'][0]['downloadURL'])