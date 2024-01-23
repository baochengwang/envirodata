#! /bin/sh

## This bash script is used to select essential address columns
## Remove "Bezirksverwaltung" and "Landkreis" prefix to make data concise.  


# pip install csvkit`` 
csvcut -d ";" -c str,hnr,adz,postplz,postonm,kreis,regbez,land,ostwert,nordwert Hauskoordinaten_bayernweit_Stand_20230901_neu.txt | 
sed 's/Bezirksverwaltung //g; s/Landkreis //g' > Hauskoordinaten_bayernweit_Lite.csv








