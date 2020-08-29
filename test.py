import cbapi
RAPIDAPI_KEY = "0352d64021msh703feee1708caeap1771c1jsnc30a4333d75a"
cb = cbapi.CrunchbaseAPI(RAPIDAPI_KEY)
org = cb.trigger_api_organization(updated_since = 1577890000, organization_types = 'school', max_threads = 4)
ppl = cb.trigger_api_people(updated_since = 1577890000, types = 'investor', max_threads = 4)

## see results in test.ipynb
