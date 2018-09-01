create-table cubetest.ul1 --t in-memory --flexible true
populate-table cubetest.ul1 /Users/ashish/Downloads/ULData/2007/CivilCas.csv true , ''
create-data-cube cubetest.ul1 STATE AGE RACE ETHNICITY