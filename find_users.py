import pandas as pd

data = pd.read_csv('C:/Users/User/Desktop/HW/HW1_BIG_DATA/source_hw/sample_500.csv')
print('Data imported successfuly')
print(data.columns)

extention_list = data['extention']

def check_dots(accesses):
    if accesses.startswith('.'):
        return True

    return False


dotted_extentions_iterator = filter(check_dots, extention_list)
dotted_extentions = list(dotted_extentions_iterator)
print('Number of incomplete extentions in 500 chunk: '+str(len(dotted_extentions)))

#gia ka8e xristi 8a prepei na checkarume ka8e arxeio an to perase panw apo mia fora
'''
def fix_extention():
    for i in range(len(data)):
        if(data.at[i,'extention'].startswith('.')):
            data.at[i,'extention'] = data.at[i,'accession'].map(str) + data.at[i,'extention']
    return data
'''
