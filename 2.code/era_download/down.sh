#!/bin/sh

# module load anaconda/5.0.0
# module load gnu 

# ----------------------
#       INPUT (!no space before and after =)
# ----------------------

# Folder to store the downloaded data
target_folder='../../0.raw/era5_land/'

# VARIABLES to be downloaded 
declare -a vars=('u10' 'v10' 'd2' 't2')

# start and end years 
year_start=2020
year_end=2022

# ----------------------
#   END INPUT 
# ----------------------

# check if the target folder exists
# if not, create it.
if [ ! -d $target_folder ]
    mkdir -p $target_folder
fi

# 
for var in ${vars[*]}
    do
        for year in `seq $year_start $year_end`
	            # do echo $model+$var+$month
		        # do python3 down_unit.py $var $year $(printf "%02d" $month) &
              do python3 down_unit.py $var $year $target_folder &
    done
done