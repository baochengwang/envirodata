git clone https://github.com/jdemaeyer/brightsky-infrastructure.git brightsky
cd brightsky

cat "BRIGHTSKY_MIN_DATE=2000-01-01" >> brightsky.env

./brightsky up -d
