
import pandas as pd
import numpy as np
import time

full_time = time.process_time()
pathfile26 = 'C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170626.csv'
pathfile27 = 'C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170627.csv'
pathfile28 = 'C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170628.csv'
pathfile29 = 'C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170629.csv'
pathfile30 = 'C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170630.csv'
data26 = pd.read_csv(pathfile26, low_memory=False)
data26 = data26.filter(['ip','date','accession','extention'], axis=1)

data27 = pd.read_csv(pathfile27, low_memory=False)
data27 = data27.filter(['ip','date','accession','extention'], axis=1)

data28 = pd.read_csv(pathfile28, low_memory=False)
data28 = data28.filter(['ip','date','accession','extention'], axis=1)

data29 = pd.read_csv(pathfile29, low_memory=False)
data29 = data29.filter(['ip','date','accession','extention'], axis=1)

data30 = pd.read_csv(pathfile30, low_memory=False)
data30 = data30.filter(['ip','date','accession','extention'], axis=1)

frames = [data26,data27,data28,data29,data30]
full_data = pd.concat(frames)
del data26; del data27; del data28; del data29; del data30;


print('Concatenation completed.')

def fixed_extention(ext, acces):
    if(ext.startswith('.')):
        ext = acces + ext

    return ext

full_data['extention'] = full_data.apply(lambda row: fixed_extention
                    (row['extention'], row['accession']), axis =1)

print("Extention fixed! It took "+ str(time.process_time()- full_time)  +"seconds.")

clean_data = full_data.filter(['ip','date','extention'], axis =1)
del full_data
clean_data.sort_values(by=['ip'], inplace=True)
print("Data sorted.. It took "+ str(time.process_time()- full_time)  +"seconds.")
#No duplication removal will be performed since it is too costy.
#clean_data.drop_duplicates(subset=['ip','date','extention'], keep='first', inplace=True)
#print("Duplicates removed.. It took "+ str(time.process_time()- full_time)  +"seconds.")

matrix = clean_data.to_numpy()
prev_ip = matrix.item(0)
dates_set = set([])
ext_set = set([])
print("Initiating calculations")
start = time.process_time()
cnt = 0
pairs = pd.DataFrame(columns=['ip','extention'])
for i in range(len(matrix)):
    if(prev_ip==matrix.item(i,0)): #same ip
        if not(matrix.item(i,1) in dates_set): #new date
            if(matrix.item(i,2) in ext_set): #same file
                #pairs.iloc[ind] = [matrix.item(i,0),matrix.item(i,2)]
                temp_df = pd.DataFrame([matrix.item(i,0),matrix.item(i,2)])
                pairs.append(temp_df)
                cnt += 1
                print(str(cnt)+"th time it finds pair.")
            else:
                ext_set.add(matrix.item(i,2))
        else:
            dates_set.add(matrix.item(i,1))
            ext_set.add(matrix.item(i,2)) #if file already exists in set this line is just ignored.
    else:
        dates_set.clear()
        dates_set.add(matrix.item(i,1))
        ext_set.clear()
        ext_set.add(matrix.item(i,2))

    prev_ip = matrix.item(i,0)

end = time.process_time()
print("Calculations for all 5 files took "+ str(end - full_time)  +"seconds.")
print('Writing in file')
pairs.to_csv('pairs_full.csv')
print("File ready")
print("The whole process took "+ str(time.process_time()- full_time)  +"seconds.")
