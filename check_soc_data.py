import json

# Load the facet data
with open('data/raw/anthropic_index/anthropic_index_2025_08_by_facet.json') as f:
    data = json.load(f)

# Check SOC occupation facet
soc = data['facets'].get('soc_occupation', {})
print('SOC occupation facet info:')
print(f'  Row count: {soc.get("row_count", 0)}')
print(f'  Variables: {soc.get("variables", [])}')

soc_data = soc.get('data', [])
print(f'  Total entries: {len(soc_data)}')

if soc_data:
    # Show sample entry
    sample = soc_data[0]
    print(f'\nSample entry:')
    for k, v in sample.items():
        print(f'    {k}: {v}')

    # Check geography
    print('\nChecking geography field...')
    geos = set([d.get('geography', '') for d in soc_data[:100]])
    print(f'  Unique geography values: {geos}')

    # Check for geo_id or geo_name
    geo_ids = set([d.get('geo_id', '') for d in soc_data[:100]])
    print(f'  Unique geo_ids: {geo_ids}')

# Check state_us facet for US-specific data
state_us = data['facets'].get('state_us', {})
print('\n\nState US facet info:')
print(f'  Row count: {state_us.get("row_count", 0)}')
print(f'  Variables: {state_us.get("variables", [])}')

state_data = state_us.get('data', [])
if state_data:
    sample_state = state_data[0]
    print(f'\nSample state entry:')
    for k, v in sample_state.items():
        print(f'    {k}: {v}')