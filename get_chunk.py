
import pandas as pd
import os; import sys

if(len(sys.argv) != 2): # the name is considered an argument apparently
    sys.exit('Wrong number of arguments')
cwd = os.getcwd()  # Get the current working directory (cwd)
files = os.listdir(cwd)  # Get all the files in that directory
print("Files in %r: %s" % (cwd, files))
chunk = int(sys.argv[1])

log26 = pd.read_csv('C:/Users/User/Desktop/HW/HW1_BIG_DATA/data/log20170626.csv')
sample_df = log26[0:chunk]
print(sample_df)
print(len(sample_df))


sample_df.to_csv('sample_' +str(chunk)+'.csv')

print('The output csv file written succesfully and generated.')

print('The number of unique extensions is: 'len(sample_df['extention'].unique()))
