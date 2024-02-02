#! /bin/bash

## This bash script is used to select essential address columns
## Remove "Bezirksverwaltung" and "Landkreis" prefix to make data concise.  

if [ $# -ne 2 ]
then
  echo "Call with arguments <input file path (.csv)> <output file path (.csv)>"
  echo ""
  return 1 2> /dev/null || exit 1
fi

inputFile=$1
outputFile=$2

#python3 -m pip install csvkit --> already done by poetry
csvcut -d ";" -c str,hnr,adz,postplz,postonm,kreis,regbez,land,ostwert,nordwert $inputFile |  sed 's/Bezirksverwaltung //g; s/Landkreis //g' > $outputFile
