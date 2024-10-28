import sys, h5py, json
import numpy as np

if __name__ == '__main__':

    with h5py.File(sys.argv[2],'w') as h:
        with open(sys.argv[1]) as j:
            d = json.load(j)

            # metadata
            metadata = d['metadata']

            for metadata_key in metadata.keys():
                if metadata_key == 'sources': continue

                if metadata_key == 'provenance':
                    h.attrs['provenance_statement'] = metadata[metadata_key]['statement']

                    # provenance history dataset
                    provenance_history_dtype = [
                        ('date',h5py.string_dtype()),
                        ('owner',h5py.string_dtype()),
                        ('published_to',h5py.string_dtype())
                    ]

                    provenance_history_date_all = []
                    provenance_history_owner_all = []
                    provenance_history_published_to_all = []
                    for history_item in metadata[metadata_key]['history']:
                        provenance_history_date_all.append(history_item['date'])
                        provenance_history_owner_all.append(history_item['owner'])
                        provenance_history_published_to_all.append(history_item['published_to'])
                    
                    provenance_history_arr = np.recarray(shape=(len(metadata[metadata_key]['history']),),dtype=provenance_history_dtype)
                    provenance_history_arr['date'] = np.asarray(provenance_history_date_all)
                    provenance_history_arr['owner'] = np.asarray(provenance_history_owner_all)
                    provenance_history_arr['published_to'] = np.asarray(provenance_history_published_to_all)

                    provenance_history_dataset = h.create_dataset('provenance_history',data=provenance_history_arr,dtype=provenance_history_dtype)
                elif metadata_key == 'format':
                    h.attrs[metadata_key] = 'HDF5'
                else:
                    h.attrs[metadata_key] = metadata[metadata_key]

            # create group for state data
            h.create_group('states_data')

            # data type for data for each state
            state_dtype = [
                ('visitor_spending_amount',float),
                ('visitor_spending_growth_from_previous_year',float),
                ('jobs_generated',float),
                ('taxes_generated',float),
                ('percent_gdp',float),
                ('tourism_total_economic_impact',float)
            ]

            for state_data in d['states_data']:
                state_dataset = h['states_data'].create_dataset(state_data['state_name'] + '_data',dtype=state_dtype,shape=1)

                # attributes
                if 'state_abbreviation' in state_data:
                    state_dataset.attrs['state_abbreviation'] = state_data['state_abbreviation']
                else: state_dataset.attrs['state_abbreviation'] = 'N/A'

                if 'timeframe' in state_data['state_data']:
                    if type(state_data['state_data']['timeframe']) == list:
                        state_dataset.attrs['data_timeframe'] = state_data['state_data']['timeframe'][0] + ' - ' + \
                            state_data['state_data']['timeframe'][1]
                    else:  state_dataset.attrs['data_timeframe'] = state_data['state_data']['timeframe']
                else:  state_dataset.attrs['data_timeframe'] = 'N/A'

                state_dataset.attrs['visitor_spending_amount_units'] = 'billion $'
                state_dataset.attrs['visitor_spending_growth_from_previous_year_units'] = '%'
                state_dataset.attrs['jobs_generated_units'] = 'thousand'
                state_dataset.attrs['taxes_generated_units'] = 'million $'
                state_dataset.attrs['tourism_total_economic_impact_units'] = 'billion $'

                state_metadata = d['metadata']['sources'][state_data['state_abbreviation']]

                state_dataset.attrs['source_url'] = state_metadata['url']
                state_dataset.attrs['source_title'] = state_metadata['title']
                state_dataset.attrs['source_subtitles'] = state_metadata['subtitles']
                state_dataset.attrs['source_last_updated'] = state_metadata['last_updated']
                state_dataset.attrs['source_authors'] = state_metadata['authors']
                state_dataset.attrs['source_publisher'] = state_metadata['publisher']['name'] + \
                    ' (' + state_metadata['publisher']['type'] + ')'

                # data
                if 'visitor_spending' in state_data['state_data'] and 'amount_billions' in state_data['state_data']['visitor_spending']:
                    state_dataset['visitor_spending_amount'] = state_data['state_data']['visitor_spending']['amount_billions']
                else: state_dataset['visitor_spending_amount'] = -1

                if 'visitor_spending' in state_data['state_data'] and 'growth_from_previous_year_percent' in\
                state_data['state_data']['visitor_spending']:
                    state_dataset['visitor_spending_growth_from_previous_year'] = state_data['state_data']['visitor_spending']\
                                                                ['growth_from_previous_year_percent']
                else: state_dataset['visitor_spending_growth_from_previous_year'] = -1

                if 'jobs_generated_thousands' in state_data['state_data']:
                    state_dataset['jobs_generated'] = state_data['state_data']['jobs_generated_thousands']
                else: state_dataset['jobs_generated'] = -1

                if 'taxes_generated_millions' in state_data['state_data']:
                    state_dataset['taxes_generated'] = state_data['state_data']['taxes_generated_millions']
                else: state_dataset['taxes_generated'] = -1

                if 'percent_gdp' in state_data['state_data']: state_dataset['percent_gdp'] = state_data['state_data']['percent_gdp']
                else: state_dataset['percent_gdp'] = -1

                if 'tourism_total_economic_impact_billions' in state_data['state_data']:
                    state_dataset['tourism_total_economic_impact'] = state_data['state_data']['tourism_total_economic_impact_billions']
                else:
                    state_dataset['tourism_total_economic_impact'] = -1