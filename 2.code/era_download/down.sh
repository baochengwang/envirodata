#!/bin/sh

# module load anaconda/5.0.0
# module load gnu 

#declare -a vars=('u10','v10','d2','t2')
declare -a vars=('v10')

# 
for var in ${vars[*]}
    do
        for year in {2018..2019}
    		do 
	       	   for month in {1..12}
	            # do echo $model+$var+$month
		        do python3 down_unit.py $var $year $(printf "%02d" $month) &
	       done
    done
done