import json

# Load the facet data
with open('data/raw/anthropic_index/anthropic_index_2025_08_by_facet.json') as f:
    data = json.load(f)

# Find US-specific SOC occupation data
soc_data = data['facets']['soc_occupation']['data']
us_soc = [d for d in soc_data if d.get('geo_id') == 'USA' or d.get('geo_name') == 'United States']

print(f'Found {len(us_soc)} US SOC entries')

if us_soc:
    # Get unique clusters
    clusters = set([d.get('cluster_name') for d in us_soc])
    print(f'US SOC categories found: {len(clusters)}')
    print('\nUS SOC distribution:')

    # Group by cluster
    us_soc_by_cluster = {}
    for entry in us_soc:
        cluster = entry.get('cluster_name')
        value = entry.get('value', 0)
        us_soc_by_cluster[cluster] = value

    # Sort by value
    for cluster, pct in sorted(us_soc_by_cluster.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cluster}: {pct:.2f}%")

    print(f"\nTotal percentage: {sum(us_soc_by_cluster.values()):.2f}%")
else:
    print("No US-specific SOC data found. Checking all geo_ids...")
    all_geo_ids = sorted(set([d.get('geo_id') for d in soc_data]))
    print(f"All unique geo_ids: {all_geo_ids}")