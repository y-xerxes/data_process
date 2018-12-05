dags = {}
dags = create_member_referee_dag(dags)

for key in retailer_dags:  # type: str
    globals()[key] = retailer_dags[key]