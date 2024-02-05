if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())
    print("Rows with zero trip distance:", data['trip_distance'].isin([0]).sum())
    
    filtered_data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    filtered_data['lpep_pickup_date'] = filtered_data['lpep_pickup_datetime'].dt.date
    filtered_data.columns = (filtered_data.columns
                            .str.replace(' ', '_')
                            .str.replace(r'(?<=[a-z])([A-Z])', r'_\1')  # Convert camel case to snake case
                            .str.lower()  # Convert all characters to lowercase
    )
    
    vendor_id_counts = filtered_data['vendor_id'].value_counts()
    print(vendor_id_counts)
    
    return filtered_data


@test
def test_output(output, *args) -> None:
    
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert 'vendor_id' in output.columns, 'vendor_id column exist in the DataFrame'