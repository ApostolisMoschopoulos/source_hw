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

def fixed_extention(ext, acces):
    if(ext.startswith('.')):
        ext = acces + ext

    return ext


data['extention'] = data.apply(lambda row: fixed_extention
                    (row['extention'], row['accession']), axis =1)
extention_list = data['extention'] # now fixed

dotted_extentions_iterator = filter(check_dots, extention_list)
dotted_extentions = list(dotted_extentions_iterator)
print('Number of incomplete extentions in 500 chunk: '+str(len(dotted_extentions)))
