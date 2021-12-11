import pandas
file_name = 'sample_500_with_fixed_extention.csv'
data= pandas.read_csv(file_name)

indiv_ip = data.ip.unique()
print('Unique ip adresses: '+str(len(indiv_ip)))
#the dataset is sorted by dates (technically by time but we aren't interested in that column)
#We can sort for ip
clean_data = data.filter(['ip','date','extention'], axis=1)
clean_data.sort_values(by=['ip'], inplace=True)
print("The shape of the dataset: "+str(clean_data.shape))
#We can also go a step futher; We can delete the rows where ip, date, extention are same
clean_data.drop_duplicates(subset=['ip','date','extention'], keep='first', inplace=True)
print("Instances without multiple accesses in 1 day: "+str(clean_data.shape))
clean_data.to_csv("clean_data_500.csv",index=False)
print("Clean data csv created")
