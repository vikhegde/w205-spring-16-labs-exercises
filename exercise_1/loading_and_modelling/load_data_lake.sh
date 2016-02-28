#!/bin/bash

TMPDIR=""
DATASET_FILE="Hospital_Revised_Flatfiles.zip"
DATASET_URL="http://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip"

exit_script () {

  exit_code="$1"
  if [ -z "$exit_code" ]; then
      exit_code=10
  fi

  if [ -n "$TMPDIR" ]; then
      echo -n "Removing temp directory..."
      /bin/rm -rf "$TMPDIR"
     echo done
  fi

  echo "Exiting with exit code $exit_code..."
  exit $exit_code
}

trap exit_script SIGHUP SIGINT SIGTERM

usage () {
    echo
    echo "Usage: $0 [optional/path/to/local/zip/file/with/entire/dataset]"
    echo "       No argument implies download dataset from medicare website"
    echo
}

echo
echo "Class:   W205 Spring 2016"
echo "Student: Vikram Hegde"
echo

if [ "$#" -gt 1 ]; then
    usage
    exit_script 1
fi

TMPDIR=`/bin/mktemp -d`
echo "Using temp directory: $TMPDIR"

if [ -z $TMPDIR ]; then
    echo "Failed to create temporary directory."
    echo
    exit_script 1
fi 

entire_zip="${TMPDIR}/${DATASET_FILE}"
/usr/bin/wget -O "$entire_zip" "$DATASET_URL" 
# wget exit codes are unreliable
if [ ! -r $entire_zip -o ! -s $entire_zip ]; then
    echo
    /bin/rm -rf "$entire_zip"
    echo "FAILED to download dataset zip file. Falling back to local file"
    entire_zip="$1"
    if [ -z "$entire_zip" ]; then
        echo "No local dataset zip specified either. Cannot continue..."
        usage
        exit_script 1
    fi
fi

if /usr/bin/file $entire_zip 2>/dev/null | /bin/grep Zip > /dev/null 2>&1; then
    echo "$entire_zip is a zip file"
    echo
else
    echo "$entire_zip is not a zip file."
    echo
    exit_script 1
fi


ORIG_CSV1="Hospital General Information.csv"
ORIG_CSV2="Timely and Effective Care - Hospital.csv"
ORIG_CSV3="Readmissions and Deaths - Hospital.csv"
ORIG_CSV4="Measure Dates.csv"
ORIG_CSV5="hvbp_hcahps_05_28_2015.csv"

CSV1="hospitals.csv"
CSV2="effective_care.csv"
CSV3="readmissions.csv"
CSV4="Measures.csv"
CSV5="survey_responses.csv"

/usr/bin/unzip $entire_zip "$ORIG_CSV1" "$ORIG_CSV2" "$ORIG_CSV3" "$ORIG_CSV4"\
     "$ORIG_CSV5" -d "$TMPDIR"
if [ "$?" -ne 0 ]; then
    echo "Unzip of dataset failed."
    /bin/rm -rf "$TMPDIR"
    exit_script 1
fi

for name in CSV1 CSV2 CSV3 CSV4 CSV5
do
    eval old='$'ORIG_${name}
    eval new='$'${name}
    echo "$old ====> $new"
    /usr/bin/tail -n +2 "${TMPDIR}/${old}" > "${TMPDIR}/${new}"
done

RAW_TBL_1="raw_hospitals"
RAW_TBL_2="raw_effective_care"
RAW_TBL_3="raw_readmissions"
RAW_TBL_4="raw_measures"
RAW_TBL_5="raw_survey_responses"

# We are going to clean up the HDFS directory conatining the raw data
# files so clean up the corresponding tables as well (if they exist)
for tbl in "$RAW_TBL_1" "$RAW_TBL_2" "$RAW_TBL_3" "$RAW_TBL_4" "$RAW_TBL_5"
do 
    echo -n "Dropping table $tbl (if it exists)... "
    /usr/bin/hive -e "drop table if exists $tbl" > /dev/null 2>&1
    if [ "$?" -ne 0 ];then
        echo
        echo "FAILED to drop table $tbl."
        exit_script 1
    fi
    echo done

done

hdfs_project_dir=/user/w205/hospital_compare/

# Remove the HDFS directories we plan to use (if they exist)
echo -n "Removing existing ${hdfs_project_dir} (if any) ... "
/usr/bin/hdfs dfs -rm -r -f "$hdfs_project_dir" > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
    echo "FAILED to clean project dir: ${hdfs_project_dir}."
    exit_script 1
fi
echo done

RAW_DIR_1="raw_hospitals_dir"
RAW_DIR_2="raw_effective_care_dir"
RAW_DIR_3="raw_readmissions_dir"
RAW_DIR_4="raw_measures_dir"
RAW_DIR_5="raw_survey_responses_dir"

# Create the HDFS directories we plan to use 

echo -n "mkdir ${hdfs_project_dir}... "
/usr/bin/hdfs dfs -mkdir "${hdfs_project_dir}" > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
    echo "FAILED to mkdir: ${hdfs_project_dir}."
    exit_script 1
fi
echo done

for hdfs_dir in "$RAW_DIR_1" "$RAW_DIR_2" "$RAW_DIR_3" "$RAW_DIR_4" "$RAW_DIR_5"
do
    echo -n "mkdir ${hdfs_project_dir}${hdfs_dir}... "
    /usr/bin/hdfs dfs -mkdir "${hdfs_project_dir}${hdfs_dir}" > /dev/null 2>&1
    if [ "$?" -ne 0 ]; then
        echo "FAILED to mkdir: ${hdfs_project_dir}${hdfs_dir}."
        exit_script 1
    fi
    echo done
done

# Show the directories we created
echo "Listing contents of ${hdfs_project_dir} ... "
/usr/bin/hdfs dfs -ls "$hdfs_project_dir"
if [ "$?" -ne 0 ]; then
    echo "FAILED to list directory contents for: ${hdfs_project_dir}"
    exit_script 1
fi

# Copy raw csv files into HDFS
echo "Copying raw CSV files into HDFS... "
for i in 1 2 3 4 5
do
    eval src='${TMPDIR}/$CSV'$i
    eval dest='${hdfs_project_dir}${RAW_DIR_'$i'}'
    echo -n "Copying $src ====> $dest ..."
    /usr/bin/hdfs dfs -put $src $dest
    if [ "$?" -ne 0 ]; then
        echo "FAILED to copy: $src ===> $dest"
        exit_script 1
    fi
    echo done
    
    # Show the results of copy
    echo "Listing contents of ${dest} ... "
    /usr/bin/hdfs dfs -ls "$dest"
    if [ "$?" -ne 0 ]; then
        echo "FAILED to list directory contents for: ${dest}"
        exit_script 1
    fi
done

echo "script $0 completed successfully"
exit_script 0
